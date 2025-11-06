package integration_tests

import (
	"context"
	"testing"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/stretchr/testify/require"
)

func GetAdminClient(ctx context.Context) *topics.TopicControl {
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
	if err != nil {
		st.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}
	kc, err := topics.NewTopicControl(prov)
	if err != nil {
		st.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}
	return kc
}

func CreateTopic(ctx context.Context, topicName string) {
	// Create an admin client, so we can create the testing topic
	client := GetAdminClient(ctx)
	topics := []st.GenericKafkaTopicSpecification{{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1}}

	err := client.Admin.CreateTopics(topics)
	if err != nil {
		st.Logger.Fatal().Err(err).Msgf("Failed to create topic %v", topicName)
	}
}

func DeleteTopic(ctx context.Context, topicName string) {
	client := GetAdminClient(ctx)
	err := client.Admin.DeleteTopics([]string{topicName})
	if err != nil {
		st.Logger.Error().Msgf("Failed to delete topic %v", topicName)
	}
}

// compare two structures by first dumping to json
// this removes problems with time, and other non-normalised data
func MarshalEqual(t *testing.T, in1, in2 any) {
	raw1, err := json.Marshal(in1)
	require.Nil(t, err)
	raw2, err := json.Marshal(in2)
	require.Nil(t, err)
	require.JSONEq(t, string(raw1), string(raw2))
}

// Skip through all events on the connection
// unfortunately can be quite slow depending on how much data is in the topics
func SkipAllEvents(t *testing.T, c *client.Client, processing bool, expedite, live, historic bool) {
	maxSecondsToWait := 5
	st.Logger.Debug().Msg("Skipping binary events")
	ok := false
	nothingFoundInARow := 0
	for retryCount := 0; retryCount < 10; retryCount++ { // retry until ok.
		for i := 1; i <= maxSecondsToWait; i++ {
			_, info, err := c.GetBinaryEvents(&client.FetchEventsStruct{
				Count: 1000, Deadline: 1, IsTask: processing,
				RequireExpedite: expedite, RequireLive: live, RequireHistoric: historic,
			})
			require.Nil(t, err)
			if int(info.Fetched) > 0 {
				nothingFoundInARow = 0
			}
			st.Logger.Printf("topics ready? %v - info %v", info.Ready, info)
			if info.Ready && int(info.Fetched) == 0 {
				st.Logger.Printf("consumed all")
				ok = true
				break
			}
		}
		if !ok && retryCount == 5 {
			t.Fatalf("skipAllEvents binary events were not readable")
		}

		ok = false
		st.Logger.Printf("Skipping status events")
		for i := 1; i <= maxSecondsToWait; i++ {
			_, info, err := c.GetStatusEvents(&client.FetchEventsStruct{
				Count: 1000, Deadline: 1, IsTask: processing,
				RequireLive: live, RequireHistoric: historic,
			})
			require.Nil(t, err)
			if int(info.Fetched) > 0 {
				nothingFoundInARow = 0
			}
			st.Logger.Printf("topics ready? %v - info %v", info.Ready, info)
			if info.Ready && int(info.Fetched) == 0 {
				st.Logger.Printf("consumed all")
				ok = true
				break
			}
		}
		if !ok && retryCount == 5 {
			t.Fatalf("skipAllEvents status events were not readable")
		}
		if ok && nothingFoundInARow > 2 {
			break
		}
		nothingFoundInARow += 1
	}
	if !ok {
		t.Fatalf("Final fail skipAllEvents status events were not readable")
	}
}
