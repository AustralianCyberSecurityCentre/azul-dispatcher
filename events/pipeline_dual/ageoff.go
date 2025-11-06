package pipeline_dual

import (
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// this pipeline will filter any events that are too old to be added to a source
type PipelineAgeoff struct {
	sourcesAgeoff map[string]time.Duration
}

const PipelineAgeOffName = "PipelineAgeoff"
const FilterAgeoff = "ageoff"

func NewPipelineAgeoff() (*PipelineAgeoff, error) {
	// build duration from ageoff settings for each source
	sourcesConf, err := models.ParseSourcesYaml(st.Events.Sources)
	if err != nil {
		return nil, err
	}
	p := &PipelineAgeoff{sourcesAgeoff: map[string]time.Duration{}}
	for name, src := range sourcesConf.Sources {
		if src.ExpireEventsAfterMs <= 0 {
			continue
		}
		p.sourcesAgeoff[name] = time.Duration(src.ExpireEventsAfterMs * int64(time.Millisecond))
	}
	return p, nil
}

func (p *PipelineAgeoff) GetName() string { return PipelineAgeOffName }

func (p *PipelineAgeoff) ProduceMod(msg *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) { // only binary or status messages can be aged off like this
	// only binary or status messages can be aged off like this
	var ev *events.BinaryEvent
	if binary, ok := msg.GetBinary(); ok {
		ev = binary
	} else if status, ok := msg.GetStatus(); ok {
		ev = &status.Entity.Input
	} else {
		return msg, nil
	}

	duration, ok := p.sourcesAgeoff[ev.Source.Name]
	if !ok {
		// keep as the source is unknown
		return msg, nil
	}
	ageoffTime := ev.Source.Timestamp.Add(duration)
	// if ageoffTime has already passed
	if ageoffTime.Compare(time.Now()) < 0 {
		return nil, nil
	}
	return msg, nil
}

func (p *PipelineAgeoff) ConsumeMod(msg *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	// only binary or status messages can be aged off like this
	var ev *events.BinaryEvent
	if binary, ok := msg.GetBinary(); ok {
		ev = binary
	} else if status, ok := msg.GetStatus(); ok {
		ev = &status.Entity.Input
	} else {
		// only binary or status messages are compatible
		return "", msg
	}

	duration, ok := p.sourcesAgeoff[ev.Source.Name]
	if !ok {
		// kepp as the source is unknown
		return "", msg
	}
	ageoffTime := ev.Source.Timestamp.Add(duration)
	// if ageoffTime has already passed
	if ageoffTime.Compare(time.Now()) < 0 {
		return FilterAgeoff, nil
	}
	return "", msg
}
