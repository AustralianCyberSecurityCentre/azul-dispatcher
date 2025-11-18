package pipeline

import (
	"fmt"
	"reflect"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

type ProduceParams struct {
	UserAgent         string       // user agent of client
	Model             events.Model // model of events that the client has published
	AvroFormat        bool         // data in kafka is in avro format
	AvroNonBulkFormat bool         // used for reprocessing
}

// ProducerInterface is a publish interface to allow testing without running kafka.
type ProducerInterface interface {
	TransformMsgInFlights(msgInFlights []*msginflight.MsgInFlight, meta *ProduceParams) ([]*msginflight.MsgInFlight, *ProduceActionInfo)
	TransformEvents(events []byte, meta *ProduceParams) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, *ProduceActionInfo, error)
	ProduceAnyEvents(confirm bool, to_publish []*msginflight.MsgInFlight) error
}

type InvalidJsonWrap struct {
	Raw    string `json:"raw"`
	Reason string `json:"reason"`
}

type ProducerError struct {
	Time        string `json:"time"`
	UserAgent   string `json:"user_agent"`
	Pipeline    string `json:"pipeline"`
	Description string `json:"description"`
	Error       string `json:"error"`
	EventBytes  int    `json:"event_bytes"`
	Event       string `json:"event"`
}

type ProduceActionInfo struct {
	// FUTURE - track number of new events added by a producer pipeline.
	ProducersThatDroppedEvents map[string]int
}

// Capture important information about pipeline errors
// Increment prometheus error counter, log to stdout, log to pipeline error file
func HandleProducerError(userAgent string, pipeline string, inFlight *msginflight.MsgInFlight, inErr error, desc string, args ...any) {
	msg, err := json.Marshal(inFlight)
	if err != nil {
		bedSet.Logger.Err(err).Str("event", string(msg)).Msg("could not marshal json message during pipeline error handling")
		return
	}

	description := fmt.Sprintf(desc, args...)
	prom.EventsProducePipelineError.WithLabelValues(pipeline).Inc()
	bedSet.Logger.Err(inErr).Str("client", userAgent).Str("pipeline", pipeline).Int("bytes", len(msg)).Msg(description)

	logLineStruct := ProducerError{
		Time:        time.Now().Format(time.RFC3339),
		UserAgent:   userAgent,
		Pipeline:    pipeline,
		Description: description,
		Error:       inErr.Error(),
		EventBytes:  len(msg),
		Event:       string(msg),
	}
	logline, err := json.Marshal(logLineStruct)
	if err != nil {
		bedSet.Logger.Err(inErr).Str("event", logLineStruct.Event).Msg("could not marshal restapi log line")
		return
	}
	st.ChLogProducerErr <- logline
}

type ProducePipeline struct {
	actions []ProduceAction
	names   []string
}

type ProduceAction interface {
	// ProduceMod allows for flexible side effects and transformation of a message.
	ProduceMod(message *msginflight.MsgInFlight, meta *ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight)
	GetName() string
}

func NewProducePipeline(actions []ProduceAction) *ProducePipeline {
	filteredActions := []ProduceAction{}
	names := []string{}
	for _, act := range actions {
		if act == nil || reflect.ValueOf(act).IsNil() {
			// this pipeline was not initialised, so should be dropped
			continue
		}
		filteredActions = append(filteredActions, act)
		names = append(names, act.GetName())
	}
	return &ProducePipeline{filteredActions, names}
}

// RunProduceActions executes each pipeline action in order. Pipelines may have any side effects required.
// Messages produced by action sources are processed by following action sources
func (p *ProducePipeline) RunProduceActions(messages []*msginflight.MsgInFlight, meta *ProduceParams) ([]*msginflight.MsgInFlight, *ProduceActionInfo) {
	bedSet.Logger.Trace().Int("len", len(messages)).Msg("new messages before modifiers")
	produceActionInfo := &ProduceActionInfo{
		ProducersThatDroppedEvents: map[string]int{},
	}
	for i, f := range p.actions {
		pipeName := p.names[i]
		startTime := time.Now().UnixNano()
		countRemoved := 0
		countAdded := 0
		// make a slice of the messages so we can modify in place
		for index := range messages[:] {
			if messages[index] == nil {
				continue
			}
			// If this is nil, nil it means we've dropped an event.
			replacement, additional := f.ProduceMod(messages[index], meta)
			// always replace message, even if nil
			// no way to tell if the message was modified
			messages[index] = replacement
			if replacement == nil {
				// Record that a producer dropped an event for later info.
				_, ok := produceActionInfo.ProducersThatDroppedEvents[f.GetName()]
				if !ok {
					produceActionInfo.ProducersThatDroppedEvents[f.GetName()] = 1
				} else {
					produceActionInfo.ProducersThatDroppedEvents[f.GetName()] += 1
				}
				countRemoved += 1
			}
			if additional != nil {
				countAdded += len(additional)
				messages = append(messages, additional...)
			}
		}
		durationSeconds := float64(time.Now().UnixNano()-startTime) / 1e9
		prom.EventsProducePipelineDuration.WithLabelValues(pipeName).Observe(durationSeconds)
		if countRemoved > 0 {
			bedSet.Logger.Trace().Str("modifier", pipeName).Int("count", countRemoved).Msg("removed messages")
			prom.EventsProducePipelineRemoved.WithLabelValues(pipeName).Add(float64(countRemoved))
		}
		if countAdded > 0 {
			prom.EventsProducePipelineAdded.WithLabelValues(pipeName).Add(float64(countAdded))
			bedSet.Logger.Trace().Str("modifier", pipeName).Int("count", countAdded).Msg("additional messages")
		}
	}
	// filter nil
	ret := []*msginflight.MsgInFlight{}
	for i := range messages {
		if messages[i] == nil {
			continue
		}
		ret = append(ret, messages[i])
	}
	bedSet.Logger.Trace().Int("len", len(ret)).Msg("new messages after modifiers")
	return ret, produceActionInfo
}
