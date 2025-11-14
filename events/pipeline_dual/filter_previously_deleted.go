package pipeline_dual

import (
	"fmt"
	"sync"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

const consumerGroupReaper = "azul-graveyard"

// FilterPreviouslyDeleted collects deleted messages to filter until kafka compaction.
type FilterPreviouslyDeleted struct {
	consumer               provider.ConsumerInterface
	producer               *producer.Producer
	run                    bool
	mux                    sync.RWMutex
	purgedSourceReferences map[string]events.DeleteEntitySubmission
	purgedLinks            map[string]events.DeleteEntityLink
	purgedAuthors          map[string]events.DeleteEntityAuthor
	purgedIDs              map[string]bool
}

// NewFilterDeleted creates a graveyard for tracking deleted messages.
func NewFilterDeleted(prov provider.ProviderInterface, p *producer.Producer) (*FilterPreviouslyDeleted, error) {
	pollWait, err := time.ParseDuration(st.Settings.Events.Kafka.PollWaitPipeline)
	if err != nil {
		return nil, err
	}
	c, err := prov.CreateConsumer(consumerGroupReaper, instance(), "earliest", topics.DeleteTopic, provider.NewConsumerOptions(pollWait))
	return &FilterPreviouslyDeleted{
		consumer:               c,
		producer:               p,
		run:                    false,
		purgedSourceReferences: map[string]events.DeleteEntitySubmission{},
		purgedLinks:            map[string]events.DeleteEntityLink{},
		purgedAuthors:          map[string]events.DeleteEntityAuthor{},
		purgedIDs:              map[string]bool{},
	}, err
}

// StartRoutine is an event loop for the graveyard consumer.
func (g *FilterPreviouslyDeleted) StartRoutine(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		startupComplete := false
		loaded := 0
		bedSet.Logger.Info().Msg("FilterPreviouslyDeleted starting up")
		g.run = true
		for g.run {
			ev := g.consumer.Poll()
			if ev == (*sarama_internals.Message)(nil) {
				if !startupComplete && g.consumer.AreAllPartitionsCaughtUp() {
					bedSet.Logger.Info().Int("events", loaded).Msg("FilterPreviouslyDeleted startup complete")
					startupComplete = true
					wg.Done()
				}
				// need to keep receiving any newly published events
				continue
			}
			msginflight, err := msginflight.NewMsgInFlightFromAvro(ev.Value, events.ModelDelete)
			if err != nil {
				bedSet.Logger.Err(err).Msg("FilterPreviouslyDeleted bad message")
				continue
			}
			delete, ok := msginflight.GetDelete()
			if !ok {
				bedSet.Logger.Err(err).Msg("FilterPreviouslyDeleted not a delete")
				continue
			}
			g.registerDelete(delete)
			loaded += 1
		}
		g.consumer.Close()
		bedSet.Logger.Debug().Msg("FilterPreviouslyDeleted closed")

	}()
}

// Stop causes the graveyard to stop consuming and exit.
func (g *FilterPreviouslyDeleted) Stop() {
	g.run = false
}

// judgePurged returns if the message needs to be purged.
// maps are used for initial detection for performance reasons.
func (p *FilterPreviouslyDeleted) judgePurged(ev *events.BinaryEvent) (bool, error) {
	id := ev.KafkaKey
	sourceReferences := ev.TrackSourceReferences
	links := ev.TrackLinks
	authors := ev.TrackAuthors

	p.mux.RLock()
	defer p.mux.RUnlock()

	// purge ids
	_, ok := p.purgedIDs[id]
	if ok {
		return true, nil
	}

	// purge link
	for _, link := range links {
		_, ok := p.purgedLinks[link]
		if ok {
			return true, nil
		}
	}

	// purge plugin - if events are older than the author deletion time.
	for _, plugin := range authors {
		purgedAuthor, ok := p.purgedAuthors[plugin]
		if ok && ev.Timestamp.Before(purgedAuthor.Timestamp) {
			return true, nil
		}
	}

	// purge submission
	// more complex due to optional parameters
	f, ok := p.purgedSourceReferences[sourceReferences]
	if ok {
		// check optional timestamp
		if f.Timestamp.IsZero() || f.Timestamp.Equal(ev.Source.Timestamp) {
			return true, nil
		}
	}
	return false, nil
}

func (p *FilterPreviouslyDeleted) GetName() string { return "FilterPreviouslyDeleted" }

// Filter drops messages that have already been deleted.
func (p *FilterPreviouslyDeleted) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	if !ok {
		// ignore non binary
		return "", message
	}
	purged, err := p.judgePurged(binary)
	if err != nil {
		pipeline.HandleConsumerError(meta, p.GetName(), message, err, "bad judge purged")
		return "error", nil
	}
	if purged {
		// trigger tombstoning
		// FUTURE only tombstone when its an ingestor consuming the event
		err = p.producer.Tombstone(false, message)
		if err != nil {
			pipeline.HandleConsumerError(meta, p.GetName(), message, err, "bad tmobstone")
			return "error", nil
		}
		// filter the event
		return "deleted", nil
	}
	return "", message
}

// Filter drops messages that have already been deleted.
func (p *FilterPreviouslyDeleted) ProduceMod(message *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	if !ok {
		// ignore non binary
		return message, nil
	}
	purged, err := p.judgePurged(binary)
	if err != nil {
		pipeline.HandleProducerError(meta.UserAgent, p.GetName(), message, err, "bad judge purged")
		return nil, nil
	}
	if purged {
		// no tombstoning as this event has not reached kafka
		// filter the event
		return nil, nil
	}
	return message, nil
}

// Instance returns a uniqe consumer name to re-read all availble deletions.
func instance() string {
	return fmt.Sprintf("%s-%d", consumerGroupReaper, time.Now().Unix())
}

// Handle processes an incoming delete event.
func (g *FilterPreviouslyDeleted) registerDelete(m *events.DeleteEvent) {
	g.mux.Lock()
	defer g.mux.Unlock()
	switch m.Action {
	case events.DeleteActionSubmission:
		g.purgedSourceReferences[m.Entity.Submission.TrackSourceReferences] = m.Entity.Submission
	case events.DeleteActionLink:
		g.purgedLinks[m.Entity.Link.TrackLink] = m.Entity.Link
	case events.DeleteActionAuthor:
		g.purgedAuthors[m.Entity.Author.TrackAuthor] = m.Entity.Author
	case events.DeleteActionIDs:
		for _, id := range m.Entity.IDs.IDs {
			g.purgedIDs[id] = true
		}
	default:
		bedSet.Logger.Error().Msgf("error - unknown delete action '%s'", m.Action)
	}
}
