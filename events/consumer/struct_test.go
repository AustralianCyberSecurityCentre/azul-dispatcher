package consumer

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/stretchr/testify/require"
)

func TestGenerateKafkaPluginKey(t *testing.T) {
	var p ConsumeParams
	st.Events.Kafka.TopicPrefix = "test01"

	p = ConsumeParams{
		Model:           "status",
		Name:            "myplugin",
		Version:         "0.1.0",
		RequireLive:     true,
		RequireHistoric: true,
		RequireSources:  []string{},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-0.1.0-status-LH")

	p = ConsumeParams{
		Model:          "status",
		Name:           "myplugin",
		Version:        "0.1.0",
		RequireLive:    true,
		RequireSources: []string{},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-0.1.0-status-L")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "0.1.0",
		RequireExpedite: true,
		RequireLive:     true,
		RequireHistoric: true,
		RequireSources:  []string{},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-0.1.0-binary-allsrc-alltype-ELH")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "0.1.0",
		RequireExpedite: true,
		RequireLive:     true,
		RequireSources:  []string{},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-0.1.0-binary-allsrc-alltype-EL")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "1.0.0",
		RequireExpedite: true,
		RequireLive:     true,
		RequireSources:  []string{},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-1.0.0-binary-allsrc-alltype-EL")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "1.0.0",
		RequireExpedite: true,
		RequireLive:     true,
		RequireSources:  []string{"tasking"},
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-1.0.0-binary-tasking-alltype-EL")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "1.0.0",
		RequireSources:  []string{"tasking"},
		RequireHistoric: true,
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-1.0.0-binary-tasking-alltype-H")

	p = ConsumeParams{
		Model:           "binary",
		Name:            "myplugin",
		Version:         "1.0.0",
		RequireSources:  []string{"tasking"},
		RequireExpedite: true,
		RequireLive:     true,
		RequireHistoric: true,
		RequireEvents:   events.ActionsBinaryDataOk,
	}
	require.Equal(t, p.GenerateKafkaPluginKey(), "myplugin-1.0.0-binary-tasking-sourced.extracted.augmented-ELH")

}
