package pipeline_consume

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

type FilterExpeditePlugins struct{}

func (p *FilterExpeditePlugins) GetName() string { return "FilterExpeditePlugins" }

// Filter if the message contains an expedite filter and the requestor is not the appropriate plugin.
func (p *FilterExpeditePlugins) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	if !ok {
		return "", message
	}
	// Only apply filter on expedite events.
	if binary.Flags.Expedite {
		// If the plugin name setting is present only the target plugin should run.
		targetPluginName, ok := binary.Source.Settings[events.SETTINGS_EXPEDITE_PLUGIN_KEY]
		if ok {
			if meta.Name != targetPluginName {
				return "filter_expedite_plugins", nil
			}
		}
	}
	return "", message
}
