package pipeline_produce

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	fstore "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/store"
	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

const consumerGroupInject = "azul-injector"

// InjectChildEvents collects manual insert messages to inject child events as needed.
type InjectChildEvents struct {
	consumer provider.ConsumerInterface
	run      bool
	mux      sync.RWMutex
	store    fstore.FileStorage
	inserts  map[string][]*events.InsertEvent
}

// NewInjectChildEvents creates a consumer for tracking and inserting messages.
func NewInjectChildEvents(prov provider.ProviderInterface, s fstore.FileStorage) (*InjectChildEvents, error) {
	pollWait, err := time.ParseDuration(st.Settings.Events.Kafka.PollWaitPipeline)
	if err != nil {
		return nil, err
	}
	c, err := prov.CreateConsumer(consumerGroupInject, group(), "earliest", topics.InsertTopic, provider.NewConsumerOptions(pollWait))
	return &InjectChildEvents{
		consumer: c,
		run:      false,
		store:    s,
		inserts:  map[string][]*events.InsertEvent{},
	}, err
}

// StartRoutine is an event loop for the injector consumer.
func (p *InjectChildEvents) StartRoutine(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		startupComplete := false
		loaded := 0
		st.Logger.Info().Msg("InjectChildEvents starting up")
		p.run = true
		for p.run {
			raw := p.consumer.Poll()
			if raw == (*sarama_internals.Message)(nil) {
				if !startupComplete && p.consumer.AreAllPartitionsCaughtUp() {
					st.Logger.Info().Int("events", loaded).Msg("InjectChildEvents startup complete")
					startupComplete = true
					wg.Done()
				}
				// need to keep receiving any newly published events
				continue
			}

			// unmarshal the insert event using upgrade pipeline
			inFlight, err := msginflight.NewMsgInFlightFromAvro(raw.Value, events.ModelInsert)
			if err != nil {
				st.Logger.Err(err).Str("consumer", "InjectChildEvents").Msg("failed to load manual insert")
				continue
			}
			insert, ok := inFlight.GetInsert()
			if !ok {
				st.Logger.Err(err).Str("consumer", "InjectChildEvents").Msg("failed to load manual insert (not insert)")
				continue
			}
			err = p.registerInsert(insert.KafkaKey, insert)
			if err != nil {
				st.Logger.Err(err).Str("consumer", "InjectChildEvents").Msg("failed to handle manual insert")
			}
			loaded += 1
		}
		p.consumer.Close()
		st.Logger.Debug().Msg("InjectChildEvents closed")
	}()
}

// Stop causes the injector to stop consuming and exit.
func (j *InjectChildEvents) Stop() {
	j.run = false
}

func (p *InjectChildEvents) GetName() string { return "InjectChildEvents" }

// ProduceMod optionally returns additional child events if matching an existing manual insert request.
func (p *InjectChildEvents) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	// only binary events
	parent, ok := inFlight.GetBinary()
	if !ok {
		return inFlight, nil
	}

	// first decide whether to ignore the event completely
	if !injectable(parent) {
		return inFlight, nil
	}

	// test whether this id is in the insert list
	p.mux.RLock()
	defer p.mux.RUnlock()
	insert, ok := p.inserts[parent.Entity.Sha256]
	if !ok {
		return inFlight, nil
	}
	newFlights := []*msginflight.MsgInFlight{}
	for _, ins := range insert {
		child := ins.Entity.ChildHistory.Sha256
		// extract information for file copy operation
		childSource := ins.Entity.OriginalSource
		parentSource := parent.Source.Name
		for _, data := range ins.Entity.Child.Datastreams {
			childLabel := data.Label
			err := p.store.Copy(childSource, childLabel.Str(), child, parentSource, childLabel.Str(), child)
			if err != nil {
				pipeline.HandleProducerError(meta.UserAgent, p.GetName(), inFlight, err, "error copying child binary")
				return nil, nil
			}
		}
		st.Logger.Debug().Str("parent", parent.Entity.Sha256).Str("child", child).Msg("Attaching manual-insert child to parent")
		msg, err := merge(parent, ins)
		if err != nil {
			pipeline.HandleProducerError(meta.UserAgent, p.GetName(), inFlight, err, "error merging insert with parent")
			return nil, nil
		}
		newFlights = append(newFlights, msg)
	}
	return inFlight, newFlights
}

// group returns a unique consumer name to re-read all available insertions.
func group() string {
	return fmt.Sprintf("%s-%d", consumerGroupInject, time.Now().Unix())
}

// Handle processes an incoming insert event.
// Takes a format of insert and validates the message
// Then puts the event into a cache.
func (p *InjectChildEvents) registerInsert(k string, ev *events.InsertEvent) error {
	if ev == nil {
		p.remove(k)
		return nil
	}

	key := ev.Entity.ParentSha256
	p.mux.Lock()
	defer p.mux.Unlock()
	// Insert the new insert event into the cache either as a new entry or replace an existing entry if it's a duplicate
	// remember may be more than one insert event for a parent
	existing, ok := p.inserts[key]
	if !ok {
		p.inserts[key] = []*events.InsertEvent{}
	}
	// may be a duplicate
	for i, old := range existing {
		if old.KafkaKey == ev.KafkaKey {
			log.Printf("Replacing existing insert event with kafka_key '%s'", ev.KafkaKey)
			existing[i] = ev
			return nil
		}
	}
	// new insert for this parent
	p.inserts[key] = append(existing, ev)
	return nil
}

// remove will delete the insert event by id if present in cache.
func (j *InjectChildEvents) remove(k string) {
	j.mux.Lock()
	defer j.mux.Unlock()
	// walk all current inserts
	for i, existing := range j.inserts {
		// remember may be more than one insert event for a parent
		n := 0
		for _, rec := range existing {
			// rec-copy the events, filtering any that match the id
			if k != rec.KafkaKey {
				existing[n] = rec
				n++
			}
			j.inserts[i] = existing[:n]
		}
	}
}

// Injectable informs whether this event should be considered for injection.
// This is primarily if the event is for a new/extracted entity vs an existing
// entity being enriched (which we would not want to create children on).
func injectable(ev *events.BinaryEvent) bool {
	steps := len(ev.Source.Path)
	// an event that produces new binary data
	if !slices.Contains(events.ActionsBinaryDataOk, ev.Action) {
		return false
	}
	// no earlier history/steps
	if steps < 2 {
		return true
	}

	// entity changed in this event (was an extraction or similar)
	curr := ev.Source.Path[steps-1].Sha256
	prev := ev.Source.Path[steps-2].Sha256
	// if check fails, last step was some additional event on the same entity, so ignore
	return curr != prev
}

// Merge parent event with insert and return child event.
func merge(parent *events.BinaryEvent, insert *events.InsertEvent) (*msginflight.MsgInFlight, error) {
	now := time.Now()
	out := events.BinaryEvent{
		Author:       insert.Entity.ChildHistory.Author,
		Timestamp:    now,
		ModelVersion: events.CurrentModelVersion,
		Source:       parent.Source,
		Action:       insert.Entity.ChildHistory.Action,
		Entity:       insert.Entity.Child,
	}
	out.Source.Path = append(out.Source.Path, insert.Entity.ChildHistory)

	cp, err := json.Marshal(&out)
	if err != nil {
		return nil, err
	}

	outFlight, err := pipeline.NewMsgInFlightFromJson(cp, events.ModelBinary)
	if err != nil {
		return nil, err
	}
	return outFlight, nil
}
