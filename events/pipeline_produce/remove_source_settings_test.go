package pipeline_produce

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

func TestRemoveSourceSettings(t *testing.T) {
	sourceRemover := NewSourceSettingRemoval()

	// No Settings present
	raw := testdata.GetEventBytes("events/pipelines/remove_source_settings/simple.json")
	orig, err := pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
	require.Nil(t, err)
	mod, _ := sourceRemover.ProduceMod(orig, &produceParams)

	binary, ok := mod.GetBinary()
	require.True(t, ok)
	require.Equal(t, map[string]string(nil), binary.Source.Settings)

	// Setting is present and should remain.
	raw = testdata.GetEventBytes("events/pipelines/remove_source_settings/simple_with_settings.json")
	orig, err = pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
	require.Nil(t, err)
	mod, _ = sourceRemover.ProduceMod(orig, &produceParams)

	binary, ok = mod.GetBinary()
	require.True(t, ok)
	require.Equal(t, map[string]string{"passwords": "a;b;c", "remove_at_depth": "5"}, binary.Source.Settings)

	// Setting is present and should be removed.
	raw = testdata.GetEventBytes("events/pipelines/remove_source_settings/simple_with_settings_remove.json")
	orig, err = pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
	require.Nil(t, err)
	mod, _ = sourceRemover.ProduceMod(orig, &produceParams)

	binary, ok = mod.GetBinary()
	require.True(t, ok)
	require.Equal(t, map[string]string{}, binary.Source.Settings)

}
