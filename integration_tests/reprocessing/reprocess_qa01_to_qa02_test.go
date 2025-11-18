//go:build integration

package integration_reprocessing

import (
	"context"
	"fmt"
	"log"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/reprocessor"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	common_int "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/integration_tests"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/IBM/sarama"

	"github.com/stretchr/testify/require"
)

// must use same server for all tests here as otherwise the partitions will not get deallocated from consumers
// and new consumers with then not see any events
var dp *restapi.Dispatcher
var server *httptest.Server
var conn *client.Client
var prov provider.ProviderInterface
var kvprov *kvprovider.KVMulti
var tc *topics.TopicControl

func testCreateProviders(provCtx context.Context) {
	var err error
	prov, err = provider.NewSaramaProvider(st.Events.Kafka.Endpoint, provCtx)
	if err != nil {
		panic(err)
	}
	kvprov, err = kvprovider.NewRedisProviders()
	if err != nil {
		panic(err)
	}

	tc, err = topics.NewTopicControl(prov)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}
}

// Configures environmental variables for qa01 topics (src of topics to migrate)
func configureTopicsForQA01() {
	st.Events.Kafka.TopicPrefix = "qa01"
	// st.Events.Reprocess.PreviousPrefix = "qa01"
	st.Events.Sources = `
sources:
  testing: {}
`

	topics.RegenTopics()
	tc.EnsureAllTopics()
}

// Configures environmental variables for qa02 topics (dst of topics to migrate)
func configureTopicsForQA02() {
	// st.Events.Reprocess.UpgradeSystem = true
	st.Events.Reprocess.UpgradeSources = true
	st.Events.Kafka.TopicPrefix = "qa02"
	st.Events.Reprocess.PreviousPrefix = "qa01"
	st.Events.Sources = `
sources:
  testing: {}
`

	topics.RegenTopics()
	tc.EnsureAllTopics()
}

var ctx context.Context
var cancelFunc context.CancelFunc

// Opens a regular dispatcher server (/client to Kafka)
func testOpenClient() {
	ctx, cancelFunc = context.WithCancel(context.Background())
	dp = restapi.NewDispatcher(prov, kvprov, ctx)
	server = httptest.NewServer(dp.Router)
	conn = testdata.GetConnection(server.URL, "events-reprocessor")
}

// Closes a previously running dispatcher instance
func testCloseClient() {
	cancelFunc()
	server.Close()
	dp.Stop()
}

func testDeleteTopics() {
	// Need to do this twice for both prefixes to
	// simulate migrating from an old instance of Azul to a newer one
	configureTopicsForQA01()
	err := tc.DeleteAllTopics()
	if err != nil {
		panic("could not delete topics")
	}

	configureTopicsForQA02()
	err = tc.DeleteAllTopics()
	if err != nil {
		panic("could not delete topics")
	}
	// Handle edge case on new kafka cluster.
	time.Sleep(5 * time.Second)
}

// testCreateTopics creates topics needed for tests to run properly
// This is needed as otherwise consumers fail to register properly
func testCreateTopics() {
	// Need to do this twice for both prefixes to
	// simulate migrating from an old instance of Azul to a newer one
	configureTopicsForQA01()

	err := tc.CreateAllTopics()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msgf("Failed to create topics")
	}
	time.Sleep(5 * time.Second)

	configureTopicsForQA02()
	err = tc.CreateAllTopics()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msgf("Failed to create topics")
	}
	time.Sleep(5 * time.Second)
}

// Inserts a binary event
func insertBinarySourcedEvent(provCtx context.Context, t *testing.T, topicName string) *events.BinaryEvent {
	// must create without dispatcher, as otherwise we aren't testing any upgrade logic during reprocessing
	testCreateProviders(provCtx)
	dstTopic := topicName
	tc.Admin.CreateTopics([]st.GenericKafkaTopicSpecification{{Topic: dstTopic, NumPartitions: 1, ReplicationFactor: 1}})

	event := testdata.GenEventBinary(&testdata.BC{})
	event.Source.Security = "RESTRICTED"
	encoded, err := event.ToAvro()
	require.Nil(t, err)

	prod, err := prov.CreateProducer()
	require.Nil(t, err)
	msg := sarama.ProducerMessage{
		Topic:     dstTopic,
		Partition: 0,
		Value:     sarama.ByteEncoder(encoded),
		Key:       sarama.StringEncoder("id"),
	}
	err = prod.Produce(&msg, true)
	require.Nil(t, err)

	// ensure that returned data has event tracking set
	event.UpdateTrackingFields()
	return event
}

func verifySourceTopicHasData(provCtx context.Context, t *testing.T, topicName string) bool {
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, provCtx)
	group := "test-consumer-reprocessor-" + topicName
	// Wait 10 seconds to try and find one message
	c, err := prov.CreateConsumer(group, group, "earliest", topicName, provider.NewConsumerOptions(time.Second*time.Duration(10)))
	if err != nil {
		t.Fatalf("failed to create kafka consumer %v", err)
		return false
	}
	msg := c.Poll()
	return msg != (*sarama_internals.Message)(nil)
}

// test that an event published to qa01 is correctly copied to qa02
func TestCopyEventBinary(t *testing.T) {
	defer st.ResetSettings()
	st.Events.Reprocess.UpgradeSources = true
	st.Settings.Events.IgnoreTopicMismatch = true
	bedSet.Settings.LogLevel = "debug"
	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	testCreateProviders(testdata.GetGlobalTestContext())

	// clear pause time to ensure it doesn't break other tests.
	defer pauser.ClearLastPauseTime(testdata.GlobalTestCtx, kvprov)
	pauser.ClearLastPauseTime(testdata.GlobalTestCtx, kvprov)

	// publish a new binary sourced event
	testDeleteTopics() // remove any previous data in the system
	testCreateTopics()
	sourceTopic := "azul.qa01.testing.binary.sourced"
	event := insertBinarySourcedEvent(testdata.GetGlobalTestContext(), t, sourceTopic)
	log.Printf("inserted binary sourced event")

	dataSeedSuccess := verifySourceTopicHasData(testdata.GetGlobalTestContext(), t, sourceTopic)
	require.True(t, dataSeedSuccess, "Failed to verify data was actually inserted into topic")

	// perform a reprocess
	configureTopicsForQA02()
	testOpenClient()
	defer testCloseClient()

	log.Printf("start reprocessing")
	err := reprocessor.Start(prov, kvprov, true, true)
	require.Nil(t, err)

	// fetch the published event from dispatcher
	maxSecondsToWait := 45
	var data []*events.BinaryEvent
	success := false
	for i := 1; i <= maxSecondsToWait; i++ {
		var err error

		newdata, info, err := conn.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 2, IsTask: false,
			RequireExpedite: true, RequireLive: true, RequireHistoric: true,
		})
		require.Nil(t, err)
		bedSet.Logger.Printf("topics ready? %v - info %v", info.Ready, info)
		data = append(data, newdata.Events...)
		if info.Ready && int(info.Fetched) == 0 {
			bedSet.Logger.Printf("consumed all")
			success = true
			// Keep going for full 45 seconds if we don't have all the data yet.
			if len(data) == 0 {
				continue
			}
			break
		}
	}
	require.True(t, success, "failed to read all events from topics, wait time was not enough")
	require.Equal(t, 1, len(data))

	// check published event against the original
	event.Dequeued = ""
	event.KafkaKey = data[0].KafkaKey
	common_int.MarshalEqual(t, event, &data[0])
	require.Equal(t, data[0].Source.Security, "RESTRICTED")

	// Verify that plugins are now paused.
	pauseTime, err := pauser.WhenWasLastPause(ctx, kvprov)
	require.Nil(t, err)
	durationSincePause := time.Since(pauseTime)
	require.Less(t, durationSincePause, pauser.PAUSE_TIME_BEFORE_RESUME)

	isPaused, err := pauser.IsPluginProcessingPaused(ctx, kvprov)
	require.Nil(t, err)
	require.True(t, isPaused)

	// Re-run the reprocessor with no change to verify it immediately exits now that all the topics are empty

	ch := make(chan error)
	// Launch as a background task with a timeout to prevent it running 6minutes.
	go func() {
		// err = reprocessor.Start(prov, kvprov, true, true)
		err = nil

		select {
		default:
			ch <- err
		case <-testdata.GlobalTestCtx.Done():
			fmt.Println("Canceled by timeout")
			return
		}
	}()

	select {
	case channelErr := <-ch:
		require.Nil(t, channelErr)
		return
	case <-time.After(20 * time.Second):
		testdata.GlobalTestCancelCtx()
		t.Fatal("Failed to trigger and close off reprocessor with empty topics")
	}
}
