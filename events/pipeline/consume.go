package pipeline

import (
	"fmt"
	"reflect"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	fstore "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

type ConsumeError struct {
	Time        string `json:"time"`
	UserAgent   string `json:"user_agent"`
	Pipeline    string `json:"pipeline"`
	Plugin      string `json:"plugin"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Error       string `json:"error"`
	EventBytes  int    `json:"event_bytes"`
	Event       string `json:"event"`
}

// Capture important information about pipeline errors
// Increment prometheus error counter, log to stdout, log to pipeline error file
func HandleConsumerError(consumer *consumer.ConsumeParams, pipeline string, inFlight *msginflight.MsgInFlight, inErr error, desc string, args ...any) {
	if inErr == nil {
		inErr = fmt.Errorf("no error provided")
	}
	msg, err := json.Marshal(inFlight)
	if err != nil {
		bedSet.Logger.Err(err).Str("event", string(msg)).Msg("could not marshal json message during pipeline error handling")
		return
	}

	description := fmt.Sprintf(desc, args...)
	prom.EventsConsumePipelineError.WithLabelValues(pipeline).Inc()
	bedSet.Logger.Err(inErr).Str("user-agent", consumer.UserAgent).Str("plugin", consumer.Name).Str("pipeline", pipeline).Int("bytes", len(msg)).Msg(description)

	logLineStruct := ConsumeError{
		Time:        time.Now().Format(time.RFC3339),
		UserAgent:   consumer.UserAgent,
		Pipeline:    pipeline,
		Plugin:      consumer.Name,
		Version:     consumer.Version,
		Description: description,
		Error:       inErr.Error(),
		EventBytes:  len(msg),
		Event:       string(msg),
	}
	logline, err := json.Marshal(logLineStruct)
	if err != nil {
		bedSet.Logger.Err(err).Str("event", logLineStruct.Event).Msg("HandleConsumerError possibly corrupt json event")
		// clear event and retry
		logLineStruct.Event = ""
		logline, err = json.Marshal(logLineStruct)
		if err != nil {
			bedSet.Logger.Err(err).Msg("HandleConsumerError could not encode even with dropped event")
			return
		}
	}
	st.ChLogConsumerErr <- logline
}

// ConsumePipeline encapsulates a series of filters that are applied to events.
type ConsumePipeline struct {
	actions []ConsumeAction
	names   []string
	Store   fstore.FileStorage
}

// ConsumeAction represents a stage of pipeline filtering.
type ConsumeAction interface {
	// ConsumeMod allows for flexible side effects and transformation of a message.
	ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight)
	GetName() string
}

func NewConsumePipeline(actions []ConsumeAction, store fstore.FileStorage) *ConsumePipeline {
	filteredActions := []ConsumeAction{}
	names := []string{}
	for _, act := range actions {
		if reflect.ValueOf(act).IsNil() {
			// this pipeline was not initialised, so should be dropped
			continue
		}
		filteredActions = append(filteredActions, act)
		names = append(names, act.GetName())
	}
	return &ConsumePipeline{filteredActions, names, store}
}

// RunConsumeActions executes each pipeline action in order. Pipelines may have any side effects required.
// Return string describes why a filtering event took place or an empty string if the event was not filtered.
// Return transformed or original bytes.
func (p *ConsumePipeline) RunConsumeActions(messages []*msginflight.MsgInFlight, meta *consumer.ConsumeParams) (map[string]int, []*msginflight.MsgInFlight) {
	states := map[string]int{}
	for i, f := range p.actions {
		pipeName := p.names[i]
		startTime := time.Now().UnixNano()
		countRemoved := 0
		// make a slice of the messages so we can modify in place
		for index := range messages[:] {
			if messages[index] == nil {
				continue
			}
			state, replacement := f.ConsumeMod(messages[index], meta)
			messages[index] = replacement
			if replacement == nil {
				if len(state) > 0 {
					// use pipeline name in state to prevent confusion about source of filtering
					states[pipeName+"-"+state]++
				} else {
					// if replacement nil and state len 0, invalid - non-fatal error
					bedSet.Logger.Error().Str("modifier", pipeName).Msg("invalid to not raise state when filtering")
				}
				countRemoved++
			} else if len(state) > 0 {
				// if replacement not nil and state not nil, invalid - non-fatal error
				bedSet.Logger.Error().Str("modifier", pipeName).Msg("invalid to raise state when not filtering")
			}
		}
		durationSeconds := float64(time.Now().UnixNano()-startTime) / 1e9
		prom.EventsConsumePipelineDuration.WithLabelValues(pipeName).Observe(durationSeconds)
		if countRemoved > 0 {
			prom.EventsConsumePipelineRemoved.WithLabelValues(pipeName).Add(float64(countRemoved))
			bedSet.Logger.Debug().Str("modifier", pipeName).Int("count", countRemoved).Msg("consumer modifier deleted message")
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
	return states, ret
}
