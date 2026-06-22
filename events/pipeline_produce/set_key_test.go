package pipeline_produce

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

func TestSetKey(t *testing.T) {
	tables := []struct {
		test       string
		inputFile  string
		expectedID string
		err        bool
	}{
		{"simple", "simple.json", "e299c2cfa5345f98ea34ff0455de22b7", false},
		{"existing_key", "existing_key.json", "e299c2cfa5345f98ea34ff0455de22b7", false},
	}
	for _, table := range tables {
		raw := testdata.GetEventBytes("events/pipelines/key_generator/" + table.inputFile)
		// id not set already
		injector := NewInjectId()
		orig, err := pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
		require.Nil(t, err, table.test)
		mod, more := injector.ProduceMod(orig, &produceParams)
		require.Nil(t, more, table.test)
		if table.err {
			// doesn't raise an error but instead returns all nils
			require.Nil(t, mod, table.test)
			continue
		}
		require.Equal(t, table.expectedID, *mod.Base.KafkaKey, table.test)
	}
}
