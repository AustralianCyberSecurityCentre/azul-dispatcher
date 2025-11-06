package pipeline_consume

import (
	"fmt"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

const td_dir = "events/pipelines/consume/too_deep/"

var td_empty_path = testdata.GetEventBytes(td_dir + "empty_path.json")

func appendToPath(data []byte, extraDepth int) []byte {
	for i := 0; i < extraDepth; i++ {
		data, _ = sjson.SetRawBytes(data, fmt.Sprintf("source.path.%d", i), []byte(`{"relationship": {"action": "extracted","type": "overlay","offset": "0xca00"}}`))
	}
	return data
}

func TestDepthNormal(t *testing.T) {
	inFlight, err := pipeline.NewMsgInFlightFromJson(td_empty_path, events.ModelBinary)
	require.Nil(t, err)

	maxDepth := 4
	d := FilterTooDeep{MaxDepth: maxDepth}
	// Check depth filter when path has a length of zero.
	warning, _ := d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true})
	if warning != "" {
		t.Error("Depth filter errored when depth was 0")
	}

	data := td_empty_path
	data = appendToPath(data, maxDepth)

	inFlight, err = pipeline.NewMsgInFlightFromJson(data, events.ModelBinary)
	require.Nil(t, err)
	// Check depth filter when path is equal to depth
	warning, _ = d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true})
	if warning != "" {
		t.Errorf("Depth filter errored when depth was equal to max depth '%d'", maxDepth)
	}

	data = appendToPath(data, 1)
	inFlight, err = pipeline.NewMsgInFlightFromJson(data, events.ModelBinary)
	require.Nil(t, err)
	// Check depth filter when path is equal to depth + 1
	warning, _ = d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true})
	if warning != "" {
		t.Errorf("Depth filter failed to detect too deep when depth was one greater than max depth %d", maxDepth+1)
	}

	// Check depth filter when depth is much greater than max depth
	data = appendToPath(data, maxDepth*5)
	inFlight, err = pipeline.NewMsgInFlightFromJson(data, events.ModelBinary)
	require.Nil(t, err)
	warning, message := d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true})
	if warning != "too_deep" {
		t.Error("Depth filter failed to detect a very deep filter")
	}

	if message != nil {
		t.Error("Depth filter was past max depth but didn't return a nil message.")
	}
}
