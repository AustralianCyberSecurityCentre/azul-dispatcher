package events

import (
	"testing"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

func TestSimulateConsumer(t *testing.T) {
	st.Events.Kafka.TopicPrefix = "unit01"
	st.Streams.Backend = "local"

	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	cancelFunc := testdata.InitGlobalContext()
	defer cancelFunc()
	var stream = streams.NewStreams()
	defer stream.Close()
	ev := NewEvents(prov, kvprov, stream.Store, testdata.GetGlobalTestContext())
	err = ev.storeConsumerInRedis(&consumer.ConsumeParams{
		Model:                  "binary",
		Name:                   "allow",
		Version:                "5",
		DeploymentKey:          "deny-filetype-key",
		RequireExpedite:        true,
		RequireLive:            true,
		Count:                  1,
		Deadline:               1,
		IsTask:                 false,
		RequireStreams:         map[events.DatastreamLabel]map[string]bool{"content": {}},
		RequireOverContentSize: 2,
	})
	require.Nil(t, err)

	consumers, err := ev.getAllConsumersFromRedis()
	require.Nil(t, err)
	require.Equal(t, len(consumers), 1)
	require.Equal(t, consumers[0], consumer.ConsumeParams{
		Model:                  "binary",
		Name:                   "allow",
		Version:                "5",
		DeploymentKey:          "deny-filetype-key",
		RequireExpedite:        true,
		RequireLive:            true,
		Count:                  1,
		Deadline:               1,
		IsTask:                 false,
		RequireStreams:         map[events.DatastreamLabel]map[string]bool{"content": {}},
		RequireOverContentSize: 2,
	})

	err = ev.storeConsumerInRedis(&consumer.ConsumeParams{
		Model:                   "binary",
		Name:                    "deny-filesize",
		Version:                 "55",
		DeploymentKey:           "deny-filetype-key",
		RequireExpedite:         true,
		RequireLive:             true,
		Count:                   1,
		Deadline:                1,
		IsTask:                  false,
		RequireStreams:          map[events.DatastreamLabel]map[string]bool{"content": {}},
		RequireUnderContentSize: 1000,
	})
	require.Nil(t, err)
	err = ev.storeConsumerInRedis(&consumer.ConsumeParams{
		Model:           "binary",
		Name:            "deny-filetype",
		Version:         "55",
		DeploymentKey:   "deny-filetype-key",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        1,
		IsTask:          false,
		RequireStreams:  map[events.DatastreamLabel]map[string]bool{"content": {"executable/windows/pe32": true, "executable/windows/dll32": true}},
	})
	require.Nil(t, err)

	consumers, err = ev.getAllConsumersFromRedis()
	require.Nil(t, err)
	require.Equal(t, len(consumers), 3)

	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	marshalled, err := json.Marshal(bse1)
	require.Nil(t, err)

	simulation, err := ev.eventSimulate(marshalled)
	require.Nil(t, err)
	require.ElementsMatch(t, simulation.Consumers, []models.EventSimulateConsumer{
		{
			Name:             "allow",
			Version:          "5",
			FilterOut:        false,
			FilterOutTrigger: "",
		},
		{
			Name:             "deny-filesize",
			Version:          "55",
			FilterOut:        true,
			FilterOutTrigger: "FilterConsumerRules-reject_content_too_large",
		},
		{
			Name:             "deny-filetype",
			Version:          "55",
			FilterOut:        true,
			FilterOutTrigger: "FilterConsumerRules-reject_file_format",
		},
	})

	// Validate that deployment key references are stored
	params := consumer.ConsumeParams{
		Model:           "binary",
		Name:            "deny-filetype",
		Version:         "55",
		DeploymentKey:   "deny-filetype-key",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        1,
		IsTask:          false,
		RequireStreams:  map[events.DatastreamLabel]map[string]bool{"content": {"executable/windows/pe32": true, "executable/windows/dll32": true}},
	}

	err = ev.storeConsumerInRedis(&params)
	require.Nil(t, err)

	raw, err := ev.kvstore.DeployedPlugins.GetBytes(ctx, "deny-filetype-key")
	require.Nil(t, err)

	deployedParam := models.DeployedPlugin{}
	err = json.Unmarshal(raw, &deployedParam)
	require.Nil(t, err)

	require.Equal(t, deployedParam.KafkaPrefix, params.GenerateKafkaPluginPrefix())
}
