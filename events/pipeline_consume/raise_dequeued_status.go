package pipeline_consume

import (
	"sync"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

type RaiseDequeuedStatus struct {
	Producer pipeline.ProducerInterface
	wg       sync.WaitGroup
	Tracker  *tracking.TaskTracker
}

func NewRaiseDequeuedStatus(producer pipeline.ProducerInterface, tracker *tracking.TaskTracker) (*RaiseDequeuedStatus, error) {
	rds := &RaiseDequeuedStatus{Producer: producer,
		Tracker: tracker}
	return rds, nil
}

func (p *RaiseDequeuedStatus) publishDequeuedStatus(msg *events.BinaryEvent, meta *consumer.ConsumeParams) error {
	// start tracking the processing of the event
	err := p.Tracker.DoProcessingStarted(msg, meta.Name, meta.Version)
	if err != nil {
		st.Logger.Err(err).Msg("update redis")
		return err
	}
	return nil
}

func (p *RaiseDequeuedStatus) GetName() string { return "RaiseDequeuedStatus" }

// Notifies a message has been dequeued for processing.
// Adds a dequeued_id to the message for correlation with a completion event.
// The dequeued Id is subsequently used for all status messages returned by a plugin, this way all the status messages
// can be associated with a single task and only the latest status kept.
func (p *RaiseDequeuedStatus) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	if !meta.IsTask {
		// client won't publish a status result so don't dequeue
		return "", message
	}
	binary, ok := message.GetBinary()
	if !ok {
		// only create dequeued status for binary events
		return "", message
	}

	if len(binary.KafkaKey) == 0 {
		pipeline.HandleConsumerError(meta, p.GetName(), message, nil, "no id")
		return "error_no_id", nil
	}

	// Provide dequeued_id to plugin so azul_runner can provide it back for event correlation.
	binary.Dequeued = p.Tracker.GenerateDequeuedID(binary.KafkaKey, meta.Name, meta.Version)

	// remove large optional message contents in partial copy
	copied := *binary
	copied.Entity.Features = []events.BinaryEntityFeature{}
	copied.Entity.Datastreams = []events.BinaryEntityDatastream{}
	copied.Entity.Info = nil

	// publish status
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		_ = p.publishDequeuedStatus(&copied, meta)
	}()

	return "", message
}
