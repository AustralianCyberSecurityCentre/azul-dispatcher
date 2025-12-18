package pipeline_consume

import (
	"encoding/json"
	"fmt"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/dedupe"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
)

// filterDuplicatesKey ensures only one copy of an event is provided to each consumer.
func filterDuplicatesKey(binary *events.BinaryEvent, plugin, version string) ([]byte, error) {
	id := binary.KafkaKey
	if len(id) <= 0 {
		msg, err := json.Marshal(binary)
		if err != nil {
			return nil, fmt.Errorf("message not json serialisable, %s:\n%v", err.Error(), binary)
		}
		return nil, fmt.Errorf("message has no id: %s", msg)
	}
	key := append([]byte{}, plugin...)
	key = append(key, version...)
	key = append(key, id...)
	return key, nil
}

type FilterDuplicates struct {
	Seen *dedupe.Lookup
}

func (p *FilterDuplicates) GetName() string { return "FilterDuplicates" }

// Filter if this entity/src has already been consumed by the requesting plugin.
// (useful if the kafka consumer for historic events has caught up to the consumer for live events)
func (p *FilterDuplicates) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	if !ok {
		// only use cache for binary events
		return "", message
	}
	// if event should not be cached, send through immediately
	// Expedited events ignore the cache as otherwise they block regular events getting to their topic of origin.
	// e.g if an event is published to expedite and then testing, the testing event will be deduplicated and never get
	// into the testing topic and the data that got to the expedited topic will eventually age-off.
	if binary.Flags.BypassCache || binary.Flags.Expedite {
		return "", message
	}
	key, err := filterDuplicatesKey(binary, meta.Name, meta.Version)
	if err != nil {
		pipeline.HandleConsumerError(meta, p.GetName(), message, nil, "message has no id")
		return "error", nil
	}
	if p.Seen.CheckAndSet(key) {
		return "skipped", nil
	}
	return "", message
}
