package pipeline_produce

import (
	"context"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_consume"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/goccy/go-json"
)

// Allow ticker time scale to be adjusted for testing.
var TICKER_UNIT = time.Minute

// Alerter Producer that observers Alert
type Alerter struct {
	ctx                   context.Context
	kvStore               *kvprovider.KVMulti
	rules                 *models.LoadedRules
	cachedMaxSecurityHits map[string]bool
}

// Load the alerter configuration from the key value store.
func loadAlerterConfigFromKvStore(ctx context.Context, kvStore *kvprovider.KVMulti) (*models.LoadedRules, error) {
	configBytes, err := kvStore.Alerter.GetBytes(ctx, settings.Settings.Alerter.ConfigKey)
	if err != nil {
		// Nothing to load so just supply an empty rule set.
		return &models.LoadedRules{Rules: []models.AlertRule{}}, nil
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

// Start a periodic reload of the alerters configuration.
func (alert *Alerter) startPeriodicReload(ctx context.Context) {
	configReloadFrequency := settings.Settings.Alerter.ConfigReloadFrequencyMin
	if configReloadFrequency > 0 {
		go func(ctx context.Context) {
			recheckTicker := time.NewTicker(TICKER_UNIT * time.Duration(configReloadFrequency))
			defer recheckTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-recheckTicker.C:
					err := alert.RecheckRules(ctx)
					if err != nil {
						bedSet.Logger.Error().Err(err).Msg("failed to reload alerter config from redis")
					}
				}
			}
		}(ctx)
	}
}

func NewAlerter(ctx context.Context, kvStore *kvprovider.KVMulti) (*Alerter, error) {
	loadedRules, err := loadAlerterConfigFromKvStore(ctx, kvStore)
	if err != nil {
		return nil, err
	}
	alert := &Alerter{
		ctx:                   ctx,
		kvStore:               kvStore,
		rules:                 loadedRules,
		cachedMaxSecurityHits: map[string]bool{},
	}
	if settings.Settings.Alerter.ConfigReloadFrequencyMin > 0 {
		alert.startPeriodicReload(ctx)
	}
	return alert, nil
}

func (alert *Alerter) GetName() string { return "Alerter" }

// Check if the current binary event being produced matches any alert rules and if it does raise an alert.
func (alert *Alerter) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	statusEvent, ok := inFlight.GetStatus()
	if !ok {
		return inFlight, nil
	}
	// Ensuring the event passes the security filtering.
	var err error
	if len(settings.Settings.Alerter.MaxSecurity) > 0 {
		isSecurityAllowedToContinue, ok := alert.cachedMaxSecurityHits[statusEvent.Entity.Input.Source.Security]
		// No cached result so calculate the new result.
		if !ok {
			isSecurityAllowedToContinue, err = pipeline_consume.CalculateSecurityResult(settings.Settings.Alerter.MaxSecurity, statusEvent.Entity.Input.Source.Security)
			if err != nil {
				bedSet.Logger.Error().Err(err).Msg("Unable to provide security filtering for alerter.")
				return inFlight, nil
			}
			alert.cachedMaxSecurityHits[statusEvent.Entity.Input.Source.Security] = isSecurityAllowedToContinue
		}
		if !isSecurityAllowedToContinue {
			return inFlight, nil
		}
	}

	// Checking if event hits rules.
	for _, curRule := range alert.rules.Rules {
		// Basic matching conditions.
		if curRule.Status != "" && curRule.Status != statusEvent.Entity.Status {
			continue
		}
		if curRule.PluginName != "" && statusEvent.Author.Name != curRule.PluginName {
			continue
		}
		if curRule.PluginVersion != "" && statusEvent.Author.Version != curRule.PluginVersion {
			continue
		}
		if curRule.SourceName != "" && statusEvent.Entity.Input.Source.Name != curRule.SourceName {
			continue
		}
		if curRule.EventType != "" {
			noResultMatches := true
			for _, curResult := range statusEvent.Entity.Results {
				if curResult.Action == curRule.EventType {
					noResultMatches = false
					break
				}
			}
			if noResultMatches {
				continue
			}
		}

		// Complex map conditions.
		if len(curRule.SourceReferenceKeyValues) > 0 {
			// Ensure all expected key value pairs match or the rule isn't a match
			isNotValidSourceRefs := false
			for key, value := range curRule.SourceReferenceKeyValues {
				refValue, ok := statusEvent.Entity.Input.Source.References[key]
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
			for _, curResult := range statusEvent.Entity.Results {
				for featName, expectedFeatVal := range curRule.FeatureNameValues {
					isValueFound := false
					for _, curFeat := range curResult.Entity.Features {
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

					break
				}
			}
			if !isValidFeatureValues {
				// One of th feature rules don't match, jump to next rule
				continue
			}
		}

		hit := models.AlertHit{
			RuleId:       curRule.Id,
			WebhookId:    curRule.WebhookId,
			AlertMessage: curRule.AlertMessage,
			Sha256:       statusEvent.Entity.Input.Entity.Sha256,
		}
		encodedHit, err := json.Marshal(hit)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("could not marshal Alert hit")
			continue
		}
		err = alert.kvStore.Alerter.PushToQueue(alert.ctx, settings.Settings.Alerter.AlertKey, encodedHit)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("could not store alert hit in redis")
		}
	}
	return inFlight, nil
}
