package pipeline_consume

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

type SelfFilter struct{}

func (p *SelfFilter) GetName() string { return "SelfFilter" }

// Filter if binary event and requesting plugin was the last author (regardless of version)
func (f *SelfFilter) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	if bin, ok := message.GetBinary(); ok {
		if bin.Author.Name == meta.Name {
			return "ignored", nil
		}
	}
	return "", message
}
