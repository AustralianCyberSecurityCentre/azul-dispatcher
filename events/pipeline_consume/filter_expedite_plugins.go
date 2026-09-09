package pipeline_consume

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
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
	bedSet.Logger.Warn().Msgf("Expedite plugin consuming for plugin, %s", meta.Name)
	// Only apply filter on expedite events.
	if binary.Flags.Expedite {
		bedSet.Logger.Error().Msgf("EXPEDITE FLAG SEEN FOR PLUGIN WITH NAME, %s", meta.Name)
		bedSet.Logger.Error().Msgf("SETTINTGS ARE %+v", binary.Source.Settings)
		// If the plugin name setting is present only the target plugin should run.
		targetPluginName, ok := binary.Source.Settings[events.SETTINGS_EXPEDITE_PLUGIN_KEY]
		if ok {
			if meta.Name != targetPluginName {
				bedSet.Logger.Warn().Msgf("Denying message with expedite and setting pass to plugin, %s == %s", meta.Name, targetPluginName)
				return "filter_expedite_plugins", nil
			}
			bedSet.Logger.Warn().Msgf("Allowing message with expedite and setting pass to plugin, %s == %s", meta.Name, targetPluginName)
		}
	}
	return "", message
}
