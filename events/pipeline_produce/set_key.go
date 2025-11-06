package pipeline_produce

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
)

// InjectId injects a corrected ID into the message
type InjectId struct {
}

// InjectId injects a corrected ID into the message
func NewInjectId() *InjectId {
	return &InjectId{}
}

func (p *InjectId) GetName() string { return "InjectId" }
func (p *InjectId) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	// generate id
	key, err := key(inFlight)
	if err != nil {
		pipeline.HandleProducerError(meta.UserAgent, p.GetName(), inFlight, err, "failed to generate id for message")
		return nil, nil
	}
	*inFlight.Base.KafkaKey = string(key)
	return inFlight, nil
}
