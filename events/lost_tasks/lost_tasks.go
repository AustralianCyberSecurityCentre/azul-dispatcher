package lost_tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_produce"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

var ctx = context.Background()

type LostTaskProcessor struct {
	produceParams pipeline.ProduceParams
	qprov         provider.ProviderInterface
	kvprov        kvprovider.KVInterface
	producer      *producer.Producer
	tracker       *tracking.TaskTracker
}

func NewLostTaskProcessor(qprov provider.ProviderInterface, kvprov *kvprovider.KVMulti, ctx context.Context) (*LostTaskProcessor, error) {
	produceParams := pipeline.ProduceParams{UserAgent: "lost-tasks"}

	p, err := producer.NewProducer(qprov)
	if err != nil {
		return nil, err
	}

	producePipe := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		// set correct ids on events
		pipeline_produce.NewInjectId(),
	})
	p.SetPipeline(producePipe)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, ctx)
	if err != nil {
		return nil, err
	}
	return &LostTaskProcessor{qprov: qprov, kvprov: kvprov.TrackPluginExecution, producer: p, tracker: tracker, produceParams: produceParams}, nil
}

// collectStats will continually collect redis statistics for prometheus
func (ltp *LostTaskProcessor) collectStats() {
	for {
		prom.EventsRedisTracking.WithLabelValues("lost-tasks").Set(float64(ltp.kvprov.GetDBSize(ctx)))
		time.Sleep(5 * time.Second)
	}
}

func (ltp *LostTaskProcessor) genFailureEvent(item *tracking.TaskStarted) (*msginflight.MsgInFlight, error) {
	ent := events.StatusEntity{
		// Id used to correlate statuses and failures for a plugin.
		Status: events.StatusTypeErrorNetwork,
		Error:  fmt.Sprintf("lost task: %s", item.Dequeued),
		Input:  *item.Event,
	}

	// create the status update message (to notify dequeued)
	now := time.Now().UTC()
	s := events.StatusEvent{
		Author: events.EventAuthor{
			Category: item.Event.Author.Category,
			Name:     item.Event.Author.Name,
			Version:  item.Event.Author.Version,
		},
		ModelVersion: events.CurrentModelVersion,
		Timestamp:    now,
		Entity:       ent,
	}
	return msginflight.NewMsgInFlightFromEvent(&s)
}

// handleLost will create a failure status for lost tasks
// We don't resubmit, to avoid all plugins rerunning on sample
func (ltp *LostTaskProcessor) handleLost(k *tracking.TaskStarted) error {
	event, err := ltp.genFailureEvent(k)
	if err != nil {
		return fmt.Errorf("failed to generate message: %v", err)
	}
	// publish event to kafka
	to_publish, _ := ltp.producer.TransformMsgInFlights([]*msginflight.MsgInFlight{event}, &ltp.produceParams)
	err = ltp.producer.ProduceAnyEvents(true, to_publish)
	if err != nil {
		return fmt.Errorf("ProduceAnyEvents failed: %v", err)
	}
	// delete from tracking
	_, err = ltp.tracker.Prov.Del(ctx, k.Dequeued)
	if err != nil {
		return fmt.Errorf("key gone: %v", err)
	}
	return nil
}

// Find old tasks and generate failure messages
func (ltp *LostTaskProcessor) RunOnce() error {
	var items []*tracking.TaskStarted
	var cursor uint64
	var err error
	for {
		// find lost tasks
		items, cursor, err = ltp.tracker.FindLostTasks()
		if err != nil {
			log.Fatal().Err(err).Msg("ScanLost error")
		}

		// handle lost tasks
		now := time.Now().UTC().Unix()
		for _, item := range items {
			elapsed := now - item.Time
			log.Info().Str("id", item.Dequeued).Int("age_seconds", int(elapsed)).Msg("job was lost")
			err := ltp.handleLost(item)
			if err != nil {
				log.Warn().Err(err).Msg("handleLost error")
			}
			prom.EventsRedisTrackingControl.WithLabelValues("lost").Add(1)
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

// Find old tasks and generate failure messages
func (ltp *LostTaskProcessor) Start() error {
	log.Info().Int64("expire_minutes", st.Events.LostTasksAfterMinutes).Msg("Starting lost tasks processor")
	go ltp.collectStats()
	for {
		err := ltp.RunOnce()
		if err != nil {
			return err
		}
		// wait a minimum time between checks for lost tasks
		log.Info().Int64("wait_minutes", st.Events.LostTasksIntervalMinutes).Msg("Finishing run and waiting")
		time.Sleep(5 * time.Minute)
	}
}

func (ltp *LostTaskProcessor) Close() error {
	ltp.producer.Stop()
	return nil
}
