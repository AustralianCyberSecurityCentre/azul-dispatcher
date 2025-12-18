package pipeline_consume

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

type FilterFlagged struct{}

func (p *FilterFlagged) GetName() string { return "FilterFlagged" }

// Filter if this message has a certain flag.
// This pipeline is used during unit tests only.
func (f *FilterFlagged) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	if binary, ok := message.GetBinary(); ok && binary.Flags.BypassCache {
		return "filter_flagged", nil
	}
	return "", message
}
