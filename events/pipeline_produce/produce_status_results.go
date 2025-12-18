package pipeline_produce

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
)

// statuses that represent in progress actions
var StatusTypesInProgress = map[string]string{
	events.StatusTypeHeartbeat: "",
	events.StatusTypeDequeued:  "",
}

// ProduceStatusResults collects manual insert messages to inject child events as needed.
type ProduceStatusResults struct {
	Tracker *tracking.TaskTracker
}

// NewProduceStatusResults creates a consumer for tracking and inserting messages.
func NewProduceStatusResults(tracker *tracking.TaskTracker) *ProduceStatusResults {
	return &ProduceStatusResults{
		Tracker: tracker,
	}
}

func (p *ProduceStatusResults) GetName() string { return "ProduceStatusResults" }
func (p *ProduceStatusResults) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	// only status events
	status, ok := inFlight.GetStatus()
	if !ok {
		return inFlight, nil
	}
	statusLabel := status.Entity.Status
	//
	// remove the dequeued entry from redis
	//
	_, ok = StatusTypesInProgress[statusLabel]
	if !ok {
		// update tracking to mark job as complete
		dequeuedId := status.Entity.Input.Dequeued
		if len(dequeuedId) > 0 {
			p.Tracker.DoProcessingFinished(dequeuedId)
		}
	}
	if !events.IsStatusTypeCompleted(statusLabel) {
		// it is not valid to attach results to this status
		return inFlight, nil
	}

	// record time taken for plugin to generate the status message
	prom.ProcessingTimes.WithLabelValues(status.Author.Name).Observe(status.Entity.RunTime)

	// add the message bytes to our outgoing messages
	newFlights := []*msginflight.MsgInFlight{}
	for i := range status.Entity.Results {
		result := status.Entity.Results[i]
		// perform flag inheritance
		result.Flags = events.BinaryFlags{
			Expedite:    status.Entity.Input.Flags.Expedite,
			BypassCache: status.Entity.Input.Flags.BypassCache,
		}
		binaryMsg, err := msginflight.NewMsgInFlightFromEvent(&result)
		if err != nil {
			pipeline.HandleProducerError(meta.UserAgent, p.GetName(), inFlight, err, "error binary MsgInFlight conversion")
			return nil, nil
		}
		newFlights = append(newFlights, binaryMsg)
	}
	// remove unnecessary results from status events
	status.Entity.Results = nil
	return inFlight, newFlights
}
