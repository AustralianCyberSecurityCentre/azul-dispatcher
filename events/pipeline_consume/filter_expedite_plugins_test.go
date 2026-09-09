package pipeline_consume

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

const td_filterExpediteDir = "events/pipelines/consume/filter_expedite_plugins/"

var td_Basic = testdata.GetEventBytes(td_filterExpediteDir + "basic.json")
var td_expeditePluginBasic = testdata.GetEventBytes(td_filterExpediteDir + "basic_with_expedite.json")

func TestFilterExpeditePlugin(t *testing.T) {

	inFlight, err := pipeline.NewMsgInFlightFromJson(td_Basic, events.ModelBinary)
	require.Nil(t, err)

	d := FilterExpeditePlugins{}
	// Have plugin expedite setting with no expedite flag set, has no affect.
	warning, _ := d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "TestPlugin", Version: "", IsTask: true})
	if warning != "" {
		t.Error("Failed to pass through expected plugin.")
	}

	warning, _ = d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "NotMatchingPlugin", Version: "", IsTask: true})
	if warning != "" {
		t.Error("Failed to pass through non-matching plugin when expedite wasn't set.")
	}

	// Same event but with expedite enabled.
	inFlight, err = pipeline.NewMsgInFlightFromJson(td_expeditePluginBasic, events.ModelBinary)
	require.Nil(t, err)
	warning, _ = d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "TestPlugin", Version: "", IsTask: true})
	if warning != "" {
		t.Error("Failed to pass through expected plugin.")
	}

	warning, msg := d.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "NotMatchingPlugin", Version: "", IsTask: true})
	require.Equal(t, warning, "filter_expedite_plugins")
	require.Nil(t, msg)
}
