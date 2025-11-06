package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

const MAX_SUBSCRIPTIONS = 3

// eventReader represets a collection of Kafka consumer subscriptions for a plugin
type eventReader struct {
	FetchEventsParams *consumer.ConsumeParams
	pluginKey         string
	model             events.Model
	subscriptions     []subscription
	last              time.Time
	pollWait          time.Duration
	channels          []chan *sarama_internals.Message
}

// newEventReader creates a new event reader.
func newEventReader(prov provider.ProviderInterface, pluginKey string, p *consumer.ConsumeParams, lastPauseTime time.Time) (*eventReader, error) {
	var err error
	c := eventReader{pluginKey: pluginKey, model: p.Model, FetchEventsParams: p}
	c.subscriptions, err = getSubscriptions(pluginKey, p)
	if err != nil {
		return nil, err
	}
	// time to wait for events on each subscription Poll() call
	c.pollWait, err = time.ParseDuration(st.Settings.Events.Kafka.PollWaitEvents)
	if err != nil {
		return nil, err
	}

	subNames := []string{}
	for i := range c.subscriptions {
		sub := &c.subscriptions[i]
		subNames = append(subNames, sub.name)
		// initialise the kafka connection for each subscription
		opts := provider.NewConsumerOptions(c.pollWait)
		opts.LastPauseTime = lastPauseTime
		sub.consumer, err = prov.CreateConsumer(pluginKey, sub.group, sub.offset, sub.pattern, opts)
		if err != nil {
			return nil, err
		}
	}

	if len(c.subscriptions) > MAX_SUBSCRIPTIONS {
		return nil, fmt.Errorf("too many subscriptions (%d) generated for %s", len(c.subscriptions), pluginKey)
	}

	// prioritised list of channels
	c.channels = make([]chan *sarama_internals.Message, MAX_SUBSCRIPTIONS)
	for i, sub := range c.subscriptions {
		c.channels[i] = sub.consumer.Chan()
	}

	c.last = time.Now()
	st.Logger.Info().Str("plugin", pluginKey).Str("subscriptions", strings.Join(subNames, ",")).Msg("plugin event reader created")
	return &c, nil
}

func (c *eventReader) checkReady() ([]string, []string) {
	// find consumers with access to partitions
	// when not ready, no events can possibly be retrieved - can occur:
	// * during startup (waiting for consumer rebalance)
	// * if too many dispatchers are running (more dispatchers than partitions for topics)
	// * if dispatchers are failing and restarting quickly (no nice shutdown of consumers)
	// note - this only tracks if at least one consumer has partitions
	consumers_ready := []string{}
	consumers_not_ready := []string{}
	for _, nc := range c.subscriptions {
		if nc.consumer.Ready() {
			// has partitions assigned, so can retrieve events
			consumers_ready = append(consumers_ready, nc.name)
		} else {
			consumers_not_ready = append(consumers_not_ready, nc.name)
		}
	}
	return consumers_ready, consumers_not_ready
}

func (c *eventReader) getMessage() *sarama_internals.Message {
	// in order of priority, see if any channels have data for us to process
	for _, ch := range c.channels {
		select {
		case msg := <-ch:
			return msg
		default:
		}
	}

	// wait on all simultaneously or wait for ticker
	// can't subscribe to a list of channels so this extremely repetitive
	// must implement up to MAX_SUBSCRIPTIONS-1
	ticker := time.NewTicker(c.pollWait)
	defer ticker.Stop()
	select {
	case msg := <-c.channels[0]:
		return msg
	case msg := <-c.channels[1]:
		return msg
	case msg := <-c.channels[2]:
		return msg
	case <-ticker.C:
	}
	return nil
}

// Pull will retrieve latest events from Kafka topics for a specific consumer,
// by priority, applying any supplied message filters.
func (c *eventReader) pull(pipe *pipeline.ConsumePipeline, p *consumer.ConsumeParams) ([]*msginflight.MsgInFlight, *models.EventResponseInfo) {
	c.last = time.Now()
	evs := make([]*msginflight.MsgInFlight, 0, p.Count)
	originTopics := map[string]int{}
	info := models.EventResponseInfo{
		Fetched:  0,
		Filtered: 0,
		Ready:    false,
		Filters:  map[string]int{},
	}
	// Pull messages in priority order from underlying consumer groups.
	// Unfortunately, we can't use consumer channels, as deprecated, so just polling loop.
	// golang's 'select' doesn't allow priority ordering of cases anyway.
	deadline := time.Now().Add(time.Duration(p.Deadline) * time.Second)

	var promTotalTimeConsumers int64 = 0
	var promTotalTimeParse int64 = 0
	var promTotalTimePipelines int64 = 0

	// Iterate across all consumer groups in order. give them a name for use in stats
	for {
		promTimeStart := time.Now().UnixNano()

		candidates := [][]byte{}
		// Keep pulling candidate messages until hit requested limit.
		// Must track events + candidates as otherwise we have too many events to return.
		for (len(evs) + len(candidates)) < p.Count {
			// TODO - need a metric about nature of events being filtered and why.

			// If deadline is set and we've exceeded our deadline, bail.
			if p.Deadline > 0 && time.Now().After(deadline) {
				break
			}

			msg := c.getMessage()
			if msg == (*sarama_internals.Message)(nil) {
				// no messages ready, try again
				continue
			}
			st.Logger.Trace().Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Msg("kafka - received msg")
			if len(msg.Value) == 0 {
				// msg was a deletion tombstone or had no data, skip
				st.Logger.Trace().Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Msg("kafka - received tombstoned message")
				continue
			}
			candidates = append(candidates, msg.Value)
			// don't actually care about the count, only that the topics were used
			originTopics[msg.Topic]++
		}
		promTimeAfterSubscriptions := time.Now().UnixNano()
		promTotalTimeConsumers += promTimeAfterSubscriptions - promTimeStart

		// run consumer pipelines on the candidate events to filter and transform
		var inFlight *msginflight.MsgInFlight
		var err error
		msgInFlights := []*msginflight.MsgInFlight{}
		for _, parsed := range candidates {
			inFlight, err = msginflight.NewMsgInFlightFromAvro(parsed, p.Model)
			if err != nil {
				st.Logger.Warn().Err(err).Str("event", string(parsed)).Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Msg("could not decode event to inflight")
				continue
			}
			msgInFlights = append(msgInFlights, inFlight)
		}

		// track time taken to parse bytes and upgrade events
		promTimeAfterParse := time.Now().UnixNano()
		promTotalTimeParse += promTimeAfterParse - promTimeAfterSubscriptions

		states, newEvents := pipe.RunConsumeActions(msgInFlights, p)

		for state, count := range states {
			info.Filters[state] += count
			info.Filtered += count
			prom.EventsConsumeFiltered.WithLabelValues(p.Name, c.model.Str()).Add(float64(count))
		}
		evs = append(evs, newEvents...)

		// track time taken to run pipelines
		promTotalTimePipelines += time.Now().UnixNano() - promTimeAfterParse

		if len(evs) < p.Count && time.Now().Before(deadline) {
			// if we don't have enough events, consider going back for more
			continue
		}
		// Time or message count reached
		break
	}

	promTimeAfterPipelines := time.Now().UnixNano()
	// assemble info for request
	numEvents := len(evs)
	info.Fetched = numEvents
	if numEvents > 0 {
		// get all topics that we read from
		topics := []string{}
		for i := range originTopics {
			topics = append(topics, i)
		}
		st.Logger.Debug().Strs("topics", topics).Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Int("count", numEvents).Msg("events consumed")
	} else {
		st.Logger.Trace().Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Int("count", numEvents).Msg("no events consumed")
	}

	// if some consumers are consistently not ready, something is wrong
	_, consumers_not_ready := c.checkReady()
	ready := len(consumers_not_ready) <= 0
	if !ready {
		info.ConsumersNotReady = strings.Join(consumers_not_ready, ",")
		st.Logger.Info().Strs("consumers", consumers_not_ready).Str("pluginKey", c.pluginKey).Str("entityType", c.model.Str()).Msg("consumers not ready")
	}
	info.Ready = ready
	prom.EventsConsumeNotReady.WithLabelValues(p.Name, p.Version).Set(float64(len(consumers_not_ready)))

	promtTimeAfterMetadata := time.Now().UnixNano()

	// prometheus metrics
	// measuring time may impact performance!
	prom.EventsConsumeStagesDuration.WithLabelValues("read_from_kafka").Observe(float64(promTotalTimeConsumers) / 1e9)
	prom.EventsConsumeStagesDuration.WithLabelValues("upgrade_event").Observe(float64(promTotalTimeParse) / 1e9)
	prom.EventsConsumeStagesDuration.WithLabelValues("run_pipelines").Observe(float64(promTotalTimePipelines) / 1e9)
	prom.EventsConsumeStagesDuration.WithLabelValues("metadata").Observe(float64(promtTimeAfterMetadata-promTimeAfterPipelines) / 1e9)
	return evs, &info
}

// stop kafka consumers
func (ev *eventReader) Stop() {
	for i := range ev.subscriptions {
		ref := &ev.subscriptions[i]
		st.Logger.Debug().Str("name", ref.name).Str("group", ref.group).Msg("closing consumer")
		ref.consumer.Close()
	}
}
