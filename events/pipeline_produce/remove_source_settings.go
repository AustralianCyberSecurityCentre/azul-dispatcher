package pipeline_produce

import (
	"strconv"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// Must match the key provided in metastore
const SUBMIT_SETTINGS_DEPTH_REMOVAL_KEY = "remove_at_depth"

// SourceSettingRemoval removes a sources setting at a specific depth depending on the model type.
type SourceSettingRemoval struct {
}

func NewSourceSettingRemoval() *SourceSettingRemoval {
	return &SourceSettingRemoval{}
}

func (p *SourceSettingRemoval) GetName() string { return "SourceSettingRemoval" }
func (p *SourceSettingRemoval) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	binaryEvent, ok := inFlight.GetBinary()
	if !ok {
		return inFlight, nil
	}

	removalDepthValue, keyPresent := binaryEvent.Source.Settings[SUBMIT_SETTINGS_DEPTH_REMOVAL_KEY]
	if !keyPresent {
		return inFlight, nil
	}
	removalDepth, err := strconv.Atoi(removalDepthValue)
	// If depth can't be found just skip removing depth as it is less bad than dropping the event completely.
	if err != nil {
		st.Logger.Error().Err(err).Msg("failed to convert removal depth value into an integer, it should always be set!")
		return inFlight, nil
	}

	if len(binaryEvent.Source.Path) >= removalDepth {
		binaryEvent.Source.Settings = map[string]string{}
	}
	return inFlight, nil
}
