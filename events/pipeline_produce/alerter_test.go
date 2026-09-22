package pipeline_produce

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func setupAlerter(t *testing.T, rules models.LoadedRules) (*Alerter, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	multiProvider, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	ruleBytes, err := json.Marshal(rules)
	require.Nil(t, err)
	err = multiProvider.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	require.Nil(t, err)
	alerter, err := NewAlerter(ctx, multiProvider)
	require.Nil(t, err)
	return alerter, cancelFunc
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
	alerter, cancelFunc := setupAlerter(t, loadedRules)
	defer cancelFunc()
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

	alerter, cancelFunc := setupAlerter(t, loadedRules)
	defer cancelFunc()
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

	alerter, cancelFunc := setupAlerter(t, loadedRules)
	defer cancelFunc()
	require.Equal(t, 3, len(alerter.rules.Rules))

	// Load in new rules but don't change compile time
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
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

func TestAlerterReloadingRulesAutomatically(t *testing.T) {
	settings.Settings.Alerter.ConfigReloadFrequencyMin = 1
	TICKER_UNIT = time.Microsecond
	defer func() {
		TICKER_UNIT = time.Minute
	}()
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

	alerter, cancelFunc := setupAlerter(t, loadedRules)
	defer cancelFunc()
	require.Equal(t, 3, len(alerter.rules.Rules))

	// Load in new rules but don't change compile time
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	ruleBytes, err := json.Marshal(models.LoadedRules{Rules: []models.AlertRule{loadedRules.Rules[1]}, RulesCompileTime: loadedRules.RulesCompileTime})
	require.Nil(t, err)
	err = alerter.kvStore.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	time.Sleep(time.Millisecond * time.Duration(settings.Settings.Alerter.ConfigReloadFrequencyMin))
	require.Equal(t, 3, len(alerter.rules.Rules))

	// Load in new rules again but this time change compile time
	ruleBytes, err = json.Marshal(models.LoadedRules{Rules: []models.AlertRule{loadedRules.Rules[1]}, RulesCompileTime: time.Now().Add(1 * time.Minute)})
	require.Nil(t, err)
	err = alerter.kvStore.Alerter.Set(ctx, models.ALERTER_CONFIG_KEY, ruleBytes, 0)
	time.Sleep(time.Millisecond * time.Duration(settings.Settings.Alerter.ConfigReloadFrequencyMin))
	require.Equal(t, 1, len(alerter.rules.Rules))
}

func easySetJsonEnv(t *testing.T, field string, v any) {
	bytes, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to configure security field %s with value %v", field, v)
	}
	err = os.Setenv(field, string(bytes))
	if err != nil {
		t.Fatalf("Failed when setting security environment variable %s with value %v", field, v)
	}
}

func easySetEnv(t *testing.T, field string, v string) {
	err := os.Setenv(field, v)
	if err != nil {
		t.Fatalf("Failed when setting security environment variable %s with value %v", field, v)
	}
}

func setupSecurityEnv(t *testing.T) {
	// Marshal converts data to []byte
	easySetJsonEnv(t, "security_minimum_required_access", []string{})
	easySetEnv(t, "security_default", "LOW")
	easySetJsonEnv(t, "security_presets", []string{
		"low TLP:CLEAR",
		"high",
		"REL:APPLE REL:BEE medium",
		"REL:APPLE REL:BEE REL:CAR medium",
		"TOP HIGH REL:APPLE REL:BEE REL:CAR",
	})
	easySetEnv(t, "security_allow_releasability_priority_gte", "30")
	easySetJsonEnv(t, "security_labels", map[string]map[string]any{
		"classification": {
			"title": "Classifications",
			"options": []map[string]string{
				{"name": "LOW", "priority": "10"},
				{"name": "LOW: LY", "priority": "20"},
				{"name": "MEDIUM", "priority": "30"},
				{"name": "HIGH", "priority": "40"},
				{"name": "TOP HIGH", "priority": "50"},
			},
		},
		"caveat": {
			"title": "Required",
			"options": []map[string]string{
				{"name": "MOD1"},
				{"name": "MOD2"},
				{"name": "MOD3"},
				{"name": "HANOVERLAP"},
				{"name": "OVER"},
				{"name": "RESTRICTED1", "min_priority": "10", "max_priority": "10"},
				{"name": "RESTRICTED2", "min_priority": "30", "max_priority": "50"},
			},
		},
		"releasability": {
			"title":           "Groups",
			"origin":          "REL:APPLE",
			"origin_alt_name": "APPLEO",
			"prefix":          "REL:",
			"options": []map[string]string{
				{"name": "REL:APPLE"},
				{"name": "REL:BEE"},
				{"name": "REL:CAR"},
			},
		},
		"tlp": {
			"title": "TLP",
			"options": []map[string]string{
				{"name": "TLP:CLEAR"},
				{"name": "TLP:GREEN"},
				{"name": "TLP:AMBER"},
				{"name": "TLP:AMBER+STRICT", "enforce_security": "true"},
			},
		},
	})
}

func TestAlerterSecurity(t *testing.T) {
	setupSecurityEnv(t)
	settings.Settings.Alerter.MaxSecurity = "MEDIUM"
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
	var alertHit models.AlertHit

	//// ------------------------------------------------------------ First hits (confirm a double hit)
	alerter, cancelFunc := setupAlerter(t, loadedRules)
	defer cancelFunc()
	msg := getInFlightMessage(t, "simple.json")
	original, additional := alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)

	// No hits due to security restrictions
	result, err := alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
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

	//// ------------------------------------------------------------ Third message again with tighter security can't be found.
	settings.Settings.Alerter.MaxSecurity = "LOW"
	alerter, cancelFunc = setupAlerter(t, loadedRules)
	defer cancelFunc()
	msg = getInFlightMessage(t, "custom-enriched-event.json")
	original, additional = alerter.ProduceMod(msg, &pipeline.ProduceParams{})
	require.Equal(t, msg, original)
	require.Equal(t, len(additional), 0)

	result, err = alerter.kvStore.Alerter.PopFromQueue(context.Background(), models.ALERTER_ALERT_KEY)
	require.Equal(t, redis.Nil, err)
}
