package pipeline_produce

import (
	"context"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/goccy/go-json"
)

// Alerter Producer that observers Alert
type Alerter struct {
	ctx     context.Context
	kvStore *kvprovider.KVMulti
	rules   *models.LoadedRules
}

// Load the alerter configuration from the key value store.
func loadAlerterConfigFromKvStore(ctx context.Context, kvStore *kvprovider.KVMulti) (*models.LoadedRules, error) {
	configBytes, err := kvStore.Alerter.GetBytes(ctx, models.ALERTER_CONFIG_KEY)
	if err != nil {
		return nil, err
	}
	var loadedRules models.LoadedRules
	err = json.Unmarshal(configBytes, &loadedRules)
	if err != nil {
		return nil, err
	}
	return &loadedRules, nil
}

func (alert *Alerter) RecheckRules(ctx context.Context) error {
	loadedRules, err := loadAlerterConfigFromKvStore(ctx, alert.kvStore)
	if err != nil {
		return err
	}
	if loadedRules.RulesCompileTime != alert.rules.RulesCompileTime {
		alert.rules = loadedRules
	}
	return nil
}

func NewAlerter(ctx context.Context, kvStore *kvprovider.KVMulti) (*Alerter, error) {
	loadedRules, err := loadAlerterConfigFromKvStore(ctx, kvStore)
	if err != nil {
		return nil, err
	}

	// TODO - periodically reload config from redis.
	return &Alerter{
		ctx:     ctx,
		kvStore: kvStore,
		rules:   loadedRules,
	}, nil
}

func (alert *Alerter) GetName() string { return "Alerter" }

// Check if the current binary event being produced matches any alert rules and if it does raise an alert.
func (alert *Alerter) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	binaryEvent, ok := inFlight.GetBinary()
	if !ok {
		return inFlight, nil
	}
	for _, curRule := range alert.rules.Rules {
		// Basic matching conditions.
		if curRule.EventType != "" && binaryEvent.Action != curRule.EventType {
			continue
		}
		if curRule.PluginName != "" && binaryEvent.Author.Name != curRule.PluginName {
			continue
		}
		if curRule.PluginVersion != "" && binaryEvent.Author.Version != curRule.PluginVersion {
			continue
		}
		if curRule.SourceName != "" && binaryEvent.Source.Name != curRule.SourceName {
			continue
		}
		// Complex map conditions.
		if len(curRule.SourceReferenceKeyValues) > 0 {
			// Ensure all expected key value pairs match or the rule isn't a match
			isNotValidSourceRefs := false
			for key, value := range curRule.SourceReferenceKeyValues {
				refValue, ok := binaryEvent.Source.References[key]
				// Reference key not in event.
				if !ok {
					isNotValidSourceRefs = true
					break
				}
				// Key exists but it's not equal to the reference key.
				if refValue != value {
					isNotValidSourceRefs = true
					break
				}
			}
			if isNotValidSourceRefs {
				// Rule doesn't match jump to next rule
				continue
			}
		}
		if len(curRule.FeatureNameValues) > 0 {
			// Ensure all expected Feature names and corresponding values are in the event.
			isValidFeatureValues := true
			for featName, expectedFeatVal := range curRule.FeatureNameValues {
				isValueFound := false
				for _, curFeat := range binaryEvent.Entity.Features {
					if curFeat.Name == featName && curFeat.Value == expectedFeatVal {
						isValueFound = true
						break
					}
				}
				// Feature and corresponding not find exit with an invalid feature/value state.
				if !isValueFound {
					isValidFeatureValues = false
					break
				}

			}
			if !isValidFeatureValues {
				// Rule doesn't match jump to next rule
				continue
			}
		}

		// TODO - consider security.

		hit := models.AlertHit{
			Rule:   curRule,
			Sha256: binaryEvent.Entity.Sha256,
		}
		encodedHit, err := json.Marshal(hit)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("could not marshal Alert hit")
			continue
		}
		err = alert.kvStore.Alerter.PushToQueue(alert.ctx, models.ALERTER_ALERT_KEY, encodedHit)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("could not store alert hit in redis")
		}
	}
	return inFlight, nil
}
