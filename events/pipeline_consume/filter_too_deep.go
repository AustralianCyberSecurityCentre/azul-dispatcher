package pipeline_consume

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

type FilterTooDeep struct {
	MaxDepth int
}

func (p *FilterTooDeep) GetName() string { return "FilterTooDeep" }

// Filter if this message has exceeded the configured processing depth.
func (p *FilterTooDeep) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	if !ok {
		return "", message
	}
	if len(binary.Source.Path) > p.MaxDepth {
		return "too_deep", nil
	}
	return "", message
}
