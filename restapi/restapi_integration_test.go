//go:build integration

package restapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	common_int "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/integration_tests"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RestapiIntTestSuite struct {
	suite.Suite

	server  *httptest.Server
	dp      *Dispatcher
	conn1   *client.Client
	conn2   *client.Client
	tracker *tracking.TaskTracker
	kvstore *kvprovider.KVMulti

	cancel context.CancelFunc
}

func TestRestapiIntegration(t *testing.T) {
	suite.Run(t, new(RestapiIntTestSuite))
}

func (s *RestapiIntTestSuite) SetupSuite() {
	// must use same server for all tests here as otherwise the partitions will not get deallocated from consumers
	// and new consumers with then not see any events
	st.Events.Sources = `
sources:
  testing: {}
`
	st.Events.Kafka.TopicPrefix = "qa01"
	topics.RegenTopics()
	// Global Context setup.
	s.cancel = testdata.InitGlobalContext()
	common_int.CreateTopic(testdata.GetGlobalTestContext(), "Testing")
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewRedisProviders()
	if err != nil {
		panic(err)
	}
	s.kvstore = kvprov
	s.dp = NewDispatcher(prov, kvprov, testdata.GetGlobalTestContext())
	s.server = httptest.NewServer(s.dp.Router)
	s.conn1 = testdata.GetConnection(s.server.URL, "events-qa01")
	s.conn2 = testdata.GetConnection(s.server.URL, "ingest-events-qa01")
}

func (s *RestapiIntTestSuite) TearDownSuite() {
	defer st.ResetSettings()
	s.server.Close()
	s.dp.Stop()
	s.cancel()
}

// post and get source binary event
func (s *RestapiIntTestSuite) TestPublishFetchEvent() {
	t := s.T()
	common_int.SkipAllEvents(t, s.conn1, false, false, true, true)

	// publish a new binary sourced event
	bse := testdata.GenEventBinary(&testdata.BC{})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse}}
	_, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	err = bse.UpdateTrackingFields()
	require.Nil(t, err)

	// fetch the published event from dispatcher
	data, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched) // expect event + historical event

	// check published event against what we sent
	bse.Dequeued = ""
	bse.KafkaKey = data.Events[0].KafkaKey
	common_int.MarshalEqual(t, bse, &data.Events[0])
	require.Equal(t, data.Events[0].Source.Security, "RESTRICTED")
}

// post and get status event with results
func (s *RestapiIntTestSuite) TestStatusEventWithBinaryEvent() {
	t := s.T()
	// Skip events for ignoring and not ignoring historic events (different consumers).
	common_int.SkipAllEvents(t, s.conn1, false, false, false, true)
	common_int.SkipAllEvents(t, s.conn1, false, false, true, true)
	common_int.SkipAllEvents(t, s.conn1, false, false, true, false)

	// publish a binary produced status with sub events
	sendEventStatus := testdata.GenEventStatus("1")
	sendEventEntities := sendEventStatus.Entity

	bulk := events.BulkStatusEvent{Events: []*events.StatusEvent{sendEventStatus}}
	_, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	// check published status message
	recvEventStatuses, info, err := s.conn1.GetStatusEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched) // expect event

	// compare status messages, ignoring entity component
	sendEventStatus.Entity = events.StatusEntity{}
	sendEventStatus.KafkaKey = recvEventStatuses.Events[0].KafkaKey
	recvEventStatuses.Events[0].Entity = events.StatusEntity{}
	common_int.MarshalEqual(t, sendEventStatus, &recvEventStatuses.Events[0])

	// check published binary messages 1
	recv_events_binary, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 4, info.Fetched) // expect event + historical event x2
	// check message 1
	for i := 0; i < 4; i++ {
		event_binary := recv_events_binary.Events[i]
		if event_binary.Action == events.ActionEnriched {
			expected := sendEventEntities.Results[0]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			common_int.MarshalEqual(t, &expected, &event_binary)
		} else if event_binary.Action == events.ActionExtracted {
			expected := sendEventEntities.Results[1]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			common_int.MarshalEqual(t, &expected, &event_binary)
		} else {
			fmt.Printf("bad thing '%s'", event_binary.KafkaKey)
			panic(5)
		}
	}

	// Refetch events with historical disabled - makes a new consumer group.
	recvEventBinaries, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched)

	// check messages
	for i := 0; i < 2; i++ {
		event_binary := recvEventBinaries.Events[i]
		if event_binary.Action == events.ActionEnriched {
			expected := sendEventEntities.Results[0]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			common_int.MarshalEqual(t, &expected, &event_binary)
		} else if event_binary.Action == events.ActionExtracted {
			expected := sendEventEntities.Results[1]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			common_int.MarshalEqual(t, &expected, &event_binary)
		} else {
			fmt.Printf("bad thing '%s'", event_binary.KafkaKey)
			panic(5)
		}
	}
}

const MAX_ATTEMPT_TIME = 6

// Repetitively query consumers until the expected number of events are found.
func hasEventsEventually(t *testing.T, fetchFunc func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error), eventCount int) {
	totalFetch := 0
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, info, err := fetchFunc()
		assert.Nil(t, err)
		totalFetch += info.Fetched
		t.Logf("Fetched so far %v, need to get %v", totalFetch, int(eventCount))
		assert.Equal(collect, int(eventCount), int(totalFetch))
	}, time.Duration(MAX_ATTEMPT_TIME)*time.Second, time.Duration(500)*time.Millisecond, "Failed to fetch %d events, only found %d", int(eventCount), int(totalFetch))
}

// Repetitively query consumers until the expected number of events or more are found.
func hasAtLeastEventsEventually(t *testing.T, fetchFunc func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error), eventCount int) {
	totalFetch := 0
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, info, err := fetchFunc()
		assert.Nil(t, err)
		totalFetch += info.Fetched
		t.Logf("Fetched so far %v, need to get %v", totalFetch, int(eventCount))
		assert.GreaterOrEqual(collect, int(totalFetch), int(eventCount))
	}, time.Duration(MAX_ATTEMPT_TIME)*time.Second, time.Duration(500)*time.Millisecond, "Failed to fetch %d events, only found %d", int(eventCount), int(totalFetch))
}

/*
Effectively the same as the function in restapi_test.go but using Sarama and redis instead of in memory versions.
*/
func (s *RestapiIntTestSuite) TestPluginPausesEvents() {
	// Make sure pause is removed.
	pauser.ClearLastPauseTime(testdata.GetGlobalTestContext(), s.kvstore)
	defer pauser.ClearLastPauseTime(testdata.GetGlobalTestContext(), s.kvstore)
	var err error
	t := s.T()

	// Give various deadlines to extend response times when looking for 0 vs many events.
	fetchFunc1Base := func(deadline int) func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error) {
		return func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error) {
			return s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
				Count: 1000, Deadline: deadline, RequireLive: true, RequireHistoric: true, IsTask: true,
			})
		}
	}
	// IMPORTANT! - deadline cannot be longer than the constant MAX_ATTEMPT_TIME, otherwise your fetch will never
	// finish before the deadline is not reached
	fetchFunc1Deadline5 := fetchFunc1Base(5)
	fetchFunc1Deadline1 := fetchFunc1Base(1)

	fetchFunc2 := func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error) {
		return s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 2, RequireExpedite: true,
			RequireLive: true, IsTask: false,
		})
	}

	// set offset in kafka to live (multiple calls to ensure it clears backlog)
	fetchFunc1Deadline5()
	require.Nil(t, err)
	// set offset to live for connection 2 as well. (multiple calls to ensure it clears backlog)
	fetchFunc2()
	fetchFunc2()
	fetchFunc2()
	require.Nil(t, err)

	// Generate first binary to setup plugin consumer
	ev1 := testdata.GenEventBinary(&testdata.BC{})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev1}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	hasEventsEventually(t, fetchFunc1Deadline1, 2)

	// Generate an event to pause plugins, and another event a plugin can in read.
	ev2 := testdata.GenEventBinary(&testdata.BC{ID: "2"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev2}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true, PausePlugins: true})
	require.Nil(t, err)

	ev3 := testdata.GenEventBinary(&testdata.BC{ID: "3"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev3}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	ev4 := testdata.GenEventBinary(&testdata.BC{ID: "4"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev4}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	// Ensure the pause has been applied takes affect.
	require.Eventually(t, func() bool {
		isPaused, _ := pauser.IsPluginProcessingPaused(testdata.GetGlobalTestContext(), s.kvstore)
		return isPaused
	}, time.Duration(3)*time.Second, time.Duration(10)*time.Millisecond, "Plugin was never processing was never paused and it should have been.")

	// Plugin fails to read any events because plugins are paused.
	hasEventsEventually(t, fetchFunc1Deadline5, 0)

	// Ingestor fetching should still find 3 events (note no historic which is why 4 instead of 8)
	hasEventsEventually(t, fetchFunc2, 4)

	// Remove pause and fetch plugins should get 0 events because plugin (live and historic) are at latest
	// Set pause time to be just after pause ends.
	newTime := time.Now().Add(-pauser.PAUSE_TIME_BEFORE_RESUME - time.Duration(1)*time.Minute)
	s.kvstore.PausePluginProcessingStartTime.Set(testdata.GetGlobalTestContext(), pauser.PAUSE_PLUGIN_FLAG_KEY, newTime, 0)
	isPaused, err := pauser.IsPluginProcessingPaused(testdata.GetGlobalTestContext(), s.kvstore)
	require.Nil(t, err)
	require.False(t, isPaused)
	// Need to do this twice due to the way pausing works. (deletes the whole consumer group.)
	hasEventsEventually(t, fetchFunc1Deadline5, 0)
	hasEventsEventually(t, fetchFunc1Deadline5, 0)

	// Publish one more event and verify plugin can now read events that occurred after latest.
	ev5 := testdata.GenEventBinary(&testdata.BC{ID: "5"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev5}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	hasEventsEventually(t, fetchFunc1Deadline1, 2)

	// Ensure a historic ingestor doesn't skip to live.
	conn3 := GetConnection(s.server.URL, "p3")
	hasAtLeastEventsEventually(t, func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error) {
		return conn3.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: false,
		})
	}, 5)

	// Ensure a historic Plugin skips to live. (as skip is still enabled)
	conn4 := GetConnection(s.server.URL, "p4")
	hasEventsEventually(t, func() (*events.BulkBinaryEvent, *models.EventResponseInfo, error) {
		return conn4.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 2, RequireHistoric: true, IsTask: true,
		})
	}, 0)
}
