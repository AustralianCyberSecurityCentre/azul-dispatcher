package settings

import (
	"os"
	"strings"
	"testing"

	bedsettings "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/stretchr/testify/require"
)

func TestReadAllConfig(t *testing.T) {
	// backup env
	envs := os.Environ()
	os.Clearenv()

	os.Setenv("DP__EVENTS__REPLAY_PLUGIN_CACHE__SIZE_BYTES", "10Ki")
	ResetSettings()
	require.Equal(t, Settings.Events.ReplayPluginCache.SizeBytes, bedsettings.HumanReadableBytes(0x2800))
	require.Equal(t, Settings.Events.ReplayPluginCache.MinRuntimeSeconds, 10)

	os.Setenv("DP__EVENTS__REPLAY_PLUGIN_CACHE__SIZE_BYTES", "20Ki")
	ResetSettings()
	require.Equal(t, Settings.Events.ReplayPluginCache.SizeBytes, bedsettings.HumanReadableBytes(0x5000))
	require.Equal(t, Settings.Events.ReplayPluginCache.MinRuntimeSeconds, 10)

	os.Unsetenv("DP__EVENTS__REPLAY_PLUGIN_CACHE__SIZE_BYTES")
	os.Setenv("DP.EVENTS.REPLAY_PLUGIN_CACHE.SIZE_BYTES", "30Ki")
	os.Setenv("DP.EVENTS.CHECK_VALID_EVENTS", "TRUE")
	os.Setenv("DP.EVENTS.IGNORE_TOPIC_MISMATCH", "true")
	os.Setenv("DP.STREAMS.S3.SECURE", "false")
	os.Setenv("DP.STREAMS.S3.ACCESS_KEY", "myaccess")
	os.Setenv("DP.STREAMS.S3.SECRET_KEY", "mysecret")
	ResetSettings()
	require.Equal(t, Settings.Events.ReplayPluginCache.SizeBytes, bedsettings.HumanReadableBytes(0x7800))
	require.Equal(t, Settings.Events.IgnoreTopicMismatch, true)
	require.Equal(t, Settings.Streams.S3.Secure, false)
	require.Equal(t, Settings.Streams.S3.AccessKey, "myaccess")
	require.Equal(t, Settings.Streams.S3.SecretKey, "mysecret")
	require.Equal(t, Settings.Events.ReplayPluginCache.MinRuntimeSeconds, 10)

	// restore variables
	os.Clearenv()
	for _, e := range envs {
		pair := strings.SplitN(e, "=", 2)
		os.Setenv(pair[0], pair[1])
	}
}
