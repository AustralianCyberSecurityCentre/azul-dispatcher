package pipeline_produce

import (
	"context"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func setupAlerter(t *testing.T, rules models.LoadedRules) *Alerter {
	ctx := context.Background()
	multiProvider, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	ruleBytes, err := json.Marshal(rules)
	require.Nil(t, err)
	err = multiProvider.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	require.Nil(t, err)
	alerter, err := NewAlerter(ctx, multiProvider)
	require.Nil(t, err)
	return alerter
}

func getInFlightMessage(t *testing.T, path string) *msginflight.MsgInFlight {
	raw := testdata.GetEventBytes("events/pipelines/alerter/" + path)
	orig, err := pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
	require.Nil(t, err)
	return orig
}

func TestAlerterAddsToRedis(t *testing.T) {
	loadedRules := models.LoadedRules{
		Rules: []models.AlertRule{
			{
				AlertEndpoint: "endpointA1",
				EventType:     events.ActionEnriched,
				PluginName:    "CustomPlugin",
				PluginVersion: "2025.03.18",
				SourceName:    "testing",
				SourceReferenceKeyValues: map[string]string{
					"user": "test-user",
				},
				FeatureNameValues: map[string]string{
					"index_of_coincidence": "1",
				},
			},
			{
				AlertEndpoint: "endpointB1",
				EventType:     events.ActionExtracted,
				PluginName:    "MimeDecoder",
			},
			{
				AlertEndpoint: "endpointC1",
				FeatureNameValues: map[string]string{
					"index_of_coincidence": "0.5",
				},
			},
			{
				AlertEndpoint: "endpointD1",
				PluginName:    "MimeDecoder",
			},
		},
		RulesCompileTime: time.Now(),
	}
	//// ------------------------------------------------------------ First hits (confirm a double hit)
	alerter := setupAlerter(t, loadedRules)
	msg := getInFlightMessage(t, "simple.json")
	original, additional := alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)

	result, err := alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Nil(t, err)
	var alertHit models.AlertHit
	err = json.Unmarshal(result, &alertHit)
	require.Nil(t, err)
	require.Equal(t, alertHit.Sha256, "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69")
	require.Equal(t, alertHit.Rule.AlertEndpoint, "endpointB1")

	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Nil(t, err)
	err = json.Unmarshal(result, &alertHit)
	require.Nil(t, err)
	require.Equal(t, alertHit.Sha256, "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69")
	require.Equal(t, alertHit.Rule.AlertEndpoint, "endpointD1")

	// Confirm no more events in queue
	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Equal(t, err, redis.Nil)

	//// ------------------------------------------------------------ Second message (confirm a single hit)
	msg = getInFlightMessage(t, "index-enriched-event.json")
	original, additional = alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)

	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Nil(t, err)
	err = json.Unmarshal(result, &alertHit)
	require.Nil(t, err)
	require.Equal(t, alertHit.Sha256, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	require.Equal(t, alertHit.Rule.AlertEndpoint, "endpointC1")

	// Confirm Queue is now empty
	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Equal(t, err, redis.Nil)

	//// ------------------------------------------------------------ Third message (confirm a single hit with multiple tight criteria)
	msg = getInFlightMessage(t, "custom-enriched-event.json")
	original, additional = alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)

	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Nil(t, err)
	err = json.Unmarshal(result, &alertHit)
	require.Nil(t, err)
	require.Equal(t, alertHit.Sha256, "ca4233acbcf3217ad8910afdf3ecf0c23650497a24e6cb953f91557d7daaaaaa")
	require.Equal(t, alertHit.Rule.AlertEndpoint, "endpointA1")

	// Confirm queue is now empty
	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Equal(t, err, redis.Nil)

}

func TestAlerterNoRaises(t *testing.T) {
	loadedRules := models.LoadedRules{
		Rules: []models.AlertRule{
			{
				AlertEndpoint: "endpointA1",
				EventType:     events.ActionEnriched,
				PluginName:    "Custom2",
				PluginVersion: "2025.03.18",
				SourceName:    "testing",
				SourceReferenceKeyValues: map[string]string{
					"user": "test-user",
				},
				FeatureNameValues: map[string]string{
					"index_of_coincidence": "1",
				},
			},
			{
				AlertEndpoint: "endpointB1",
				EventType:     events.ActionExtracted,
				PluginName:    "Custom2",
			},
		},
		RulesCompileTime: time.Now(),
	}

	alerter := setupAlerter(t, loadedRules)
	msg := getInFlightMessage(t, "simple.json")
	original, additional := alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)
	// Should be no hits as none of the rules match the event.
	_, err := alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Equal(t, err, redis.Nil)
}

func TestAlerterReloadingRules(t *testing.T) {
	loadedRules := models.LoadedRules{
		Rules: []models.AlertRule{
			{
				AlertEndpoint: "endpointA1",
				EventType:     events.ActionEnriched,
				PluginName:    "CustomPlugin",
				PluginVersion: "2025.03.18",
				SourceName:    "testing",
				SourceReferenceKeyValues: map[string]string{
					"user": "test-user",
				},
				FeatureNameValues: map[string]string{
					"index_of_coincidence": "1",
				},
			},
			{
				AlertEndpoint: "endpointB1",
				EventType:     events.ActionExtracted,
				PluginName:    "MimeDecoder",
			},
			{
				FeatureNameValues: map[string]string{
					"index_of_coincidence": "0.5",
				},
			},
		},
		RulesCompileTime: time.Now(),
	}

	alerter := setupAlerter(t, loadedRules)
	require.Equal(t, 3, len(alerter.rules.Rules))

	// Load in new rules but don't change compile time
	ctx := context.Background()
	ruleBytes, err := json.Marshal(models.LoadedRules{Rules: []models.AlertRule{loadedRules.Rules[1]}, RulesCompileTime: loadedRules.RulesCompileTime})
	require.Nil(t, err)
	err = alerter.kvStore.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	alerter.RecheckRules(ctx)
	require.Equal(t, 3, len(alerter.rules.Rules))

	// Load in new rules again but this time change compile time
	ruleBytes, err = json.Marshal(models.LoadedRules{Rules: []models.AlertRule{loadedRules.Rules[1]}, RulesCompileTime: time.Now().Add(1 * time.Minute)})
	require.Nil(t, err)
	err = alerter.kvStore.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	alerter.RecheckRules(ctx)
	require.Equal(t, 1, len(alerter.rules.Rules))
}
