package pipeline_consume

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

const security_dir = "events/pipelines/consume/too_deep/"

var security_empty_path = testdata.GetEventBytes(security_dir + "empty_path.json")

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

func TestSecurityNormal(t *testing.T) {
	setupSecurityEnv(t)
	inFlight, err := pipeline.NewMsgInFlightFromJson(security_empty_path, events.ModelBinary)
	require.Nil(t, err)

	fs := FilterSecurity{CachedSecurityResults: map[string]CacheHit{}}
	be, ok := inFlight.GetBinary()
	require.True(t, ok)

	// pass security filtering
	for _, cls := range []string{"LOW", "LOW MOD1", "MEDIUM MOD1", "MEDIUM", "MEDIUM REL:APPLE", "MEDIUM REL:APPLE,BEE"} {
		be.Source.Security = cls
		// Check security string when cache value not hit.
		warning, msg := fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "MEDIUM MOD1 REL:APPLE"})
		if warning != "" {
			t.Errorf("Security filter failed to allow through classification %v", cls)
		}
		require.NotNilf(t, msg, "Security failed to allow a message through with classification %v and message %v", cls, msg)
	}

	// Fail to pass security filtering
	for _, cls := range []string{"HIGH", "TOP HIGH", "HIGH MOD1", "HIGH REL:APPLE", "LOW MOD1", "LOW MOD2", "MEDIUM MOD3"} {
		be.Source.Security = cls
		// Check security string when cache value not hit.
		warning, msg := fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "MEDIUM"})
		if warning != "max_security" {
			t.Errorf("Security filter failed to block classification %v", cls)
		}
		require.Nilf(t, msg, "Security failed to make messages nil with classification %v", cls)
	}
}

func TestSecurityBadSecurity(t *testing.T) {
	setupSecurityEnv(t)
	inFlight, err := pipeline.NewMsgInFlightFromJson(security_empty_path, events.ModelBinary)
	require.Nil(t, err)

	fs := FilterSecurity{CachedSecurityResults: map[string]CacheHit{}}
	be, ok := inFlight.GetBinary()
	require.True(t, ok)

	// Fail when event has bad security
	be.Source.Security = "Random"
	// should fail as security of binary event isn't valid.
	warning, _ := fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "MEDIUM"})
	if warning != "max_security" {
		t.Error("Security failed to discard when event has invalid security.")
	}

	// Fail when author has provided bad security
	be.Source.Security = "MEDIUM"
	// should fail as security of author event isn't valid.
	warning, _ = fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "Random"})
	if warning != "max_security" {
		t.Error("Security failed to discard when author security filter was bad (fail closed 2)")
	}

	// Fail when author and event have bad security and it's the same
	be.Source.Security = "Random"
	// should fail as security of author event isn't valid.
	warning, _ = fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "Random"})
	if warning != "max_security" {
		t.Error("Security failed to discard when author security filter was bad (fail closed 2)")
	}
}

func TestSecurityEmptySecurity(t *testing.T) {
	setupSecurityEnv(t)
	inFlight, err := pipeline.NewMsgInFlightFromJson(security_empty_path, events.ModelBinary)
	require.Nil(t, err)

	fs := FilterSecurity{CachedSecurityResults: map[string]CacheHit{}}
	be, ok := inFlight.GetBinary()
	require.True(t, ok)

	// Fail when event has no security
	be.Source.Security = ""
	// should fail as security of binary event isn't valid.
	warning, _ := fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: "MEDIUM"})
	if warning != "" {
		t.Error("Security failed to allow when event security was empty")
	}

	// Pass when author has provided no security
	be.Source.Security = "MEDIUM"
	// should fail as security of author event isn't valid.
	warning, _ = fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: ""})
	if warning != "" {
		t.Error("Security failed to allow when author security was empty.")
	}

	// Pass when author and event have no security and it's the same
	be.Source.Security = ""
	// should fail as security of author event isn't valid.
	warning, _ = fs.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "", Version: "", IsTask: true, MaxSecurity: ""})
	if warning != "" {
		t.Error("Security failed to allow when all security was empty.")
	}
}
