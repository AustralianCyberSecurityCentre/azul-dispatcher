//go:build integration

package eventdebugger

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	common_int "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/integration_tests"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type EventDebuggerTestSuite struct {
	suite.Suite

	server  *httptest.Server
	dp      *restapi.Dispatcher
	conn   *client.Client
	tracker *tracking.TaskTracker
	kvstore *kvprovider.KVMulti

	cancel context.CancelFunc
}

func TestEventDebuggerIntegration(t *testing.T) {
	suite.Run(t, new(EventDebuggerTestSuite))
}

func (s *EventDebuggerTestSuite) SetupSuite() {
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
	s.dp = restapi.NewDispatcher(prov, kvprov, testdata.GetGlobalTestContext())
	s.server = httptest.NewServer(s.dp.Router)
	s.conn = testdata.GetConnection(s.server.URL, "events-qa01")
}

func (s *EventDebuggerTestSuite) TearDownSuite() {
	defer st.ResetSettings()
	s.server.Close()
	s.dp.Stop()
	s.cancel()
}

// post and get source binary event
func (s *EventDebuggerTestSuite) TestFetchEventsWithDebugFlag() {
	t := s.T()
	common_int.SkipAllEvents(t, s.conn, false, false, true, true, true)

	// publish a new binary sourced event
	bse := testdata.GenEventBinary(&testdata.BC{})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse}}
	_, err := s.conn.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	err = bse.UpdateTrackingFields()
	require.Nil(t, err)

	// fetch the published event from dispatcher
	data, info, err := s.conn.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false, Debug: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched) // expect event + historical event

	// check published event against what we sent
	bse.Dequeued = ""
	bse.KafkaKey = data.Events[0].KafkaKey
	common_int.MarshalEqual(t, bse, &data.Events[0])
	require.Equal(t, data.Events[0].Source.Security, "RESTRICTED")
}

// test that the reset flag returns the same events as the previous fetch
func (s *EventDebuggerTestSuite) TestFetchEventsWithResetFlag() {
	t := s.T()

	// publish a new binary sourced event
	bse := testdata.GenEventBinary(&testdata.BC{})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse}}
	_, err := s.conn.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	err = bse.UpdateTrackingFields()
	require.Nil(t, err)

	// fetch the published event from dispatcher
	data, info, err := s.conn.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false, Debug: true, Reset: true,
	})
	require.Nil(t, err)

	reset_data, reset_info, err := s.conn.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false, Debug: true, Reset: true,
	})
	require.Nil(t, err)
	require.Equal(t, info.Fetched, reset_info.Fetched)

	// ensure that all kafka keys are the same for the reset events (events may not return in same order)
	for _, e := range data.Events {
		found := false
		for _, re := range reset_data.Events {
			if e.KafkaKey == re.KafkaKey {
				found = true
				break
			}
		}
		require.True(t, found, "kafka key not found in reset events: %s", e.KafkaKey)
	}	
}
