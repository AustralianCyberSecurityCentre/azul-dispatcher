package pipeline_dual

import (
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

var ageoff_basic = testdata.GetEventBytes("events/pipelines/ageoff/" + "basic.json")

const AGEOFF_SOURCE_YAML = `
sources:
  virustotal:
    description: "test"
    expire_events_after: "2 days"
  tasking:
    description: "test"
`

func TestAgeoffProduce(t *testing.T) {
	st.Events.Sources = AGEOFF_SOURCE_YAML
	defer st.ResetSettings()

	// source info
	var meta = pipeline.ProduceParams{UserAgent: "test"}
	p, err := NewPipelineAgeoff()
	require.Nil(t, err)

	inflight, err := pipeline.NewMsgInFlightFromJson(ageoff_basic, events.ModelBinary)
	require.Nil(t, err)

	binary, _ := inflight.GetBinary()

	// 10 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 10 * -1)
	orig, additional := p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.Nil(t, orig)

	// 1 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 1 * -1)
	orig, additional = p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.NotNil(t, orig)

	// 1 days in the future
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 1)
	orig, additional = p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.NotNil(t, orig)

	// 3 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 3 * -1)
	orig, additional = p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.Nil(t, orig)

	// source with no ageoff
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 60 * -1)
	binary.Source.Name = "tasking"
	orig, additional = p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.NotNil(t, orig)

	// unknown source
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 3 * -1)
	binary.Source.Name = "testing"
	orig, additional = p.ProduceMod(inflight, &meta)
	require.Equal(t, len(additional), 0)
	require.NotNil(t, orig)
}

func TestAgeoffConsume(t *testing.T) {
	st.Events.Sources = AGEOFF_SOURCE_YAML
	defer st.ResetSettings()

	// source info
	var meta = consumer.ConsumeParams{UserAgent: "test"}
	p, err := NewPipelineAgeoff()
	require.Nil(t, err)

	inflight, err := pipeline.NewMsgInFlightFromJson(ageoff_basic, events.ModelBinary)
	require.Nil(t, err)

	binary, _ := inflight.GetBinary()

	// 10 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 10 * -1)
	status, orig := p.ConsumeMod(inflight, &meta)
	require.Nil(t, orig)
	require.Equal(t, status, FilterAgeoff)

	// 1 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 1 * -1)
	status, orig = p.ConsumeMod(inflight, &meta)
	require.NotNil(t, orig)
	require.Equal(t, status, "")

	// 1 days in the future
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 1)
	status, orig = p.ConsumeMod(inflight, &meta)
	require.NotNil(t, orig)
	require.Equal(t, status, "")

	// 3 days in the past
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 3 * -1)
	status, orig = p.ConsumeMod(inflight, &meta)
	require.Nil(t, orig)
	require.Equal(t, status, FilterAgeoff)

	// source with no ageoff
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 60 * -1)
	binary.Source.Name = "tasking"
	status, orig = p.ConsumeMod(inflight, &meta)
	require.NotNil(t, orig)
	require.Equal(t, status, "")

	// unknown source
	binary.Source.Timestamp = time.Now().Add(time.Hour * 24 * 3 * -1)
	binary.Source.Name = "testing"
	status, orig = p.ConsumeMod(inflight, &meta)
	require.NotNil(t, orig)
	require.Equal(t, status, "")
}
