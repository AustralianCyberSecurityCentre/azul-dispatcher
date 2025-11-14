package restapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func GetConnection(url string, nameExtra string) *client.Client {
	author := events.PluginEntity{Name: "test" + nameExtra, Version: "1", Category: "plugin"}
	c := client.NewClient(url, url, author, "test-key")
	err := c.PublishPlugin()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("Could not obtain client connection to dispatcher")
	}
	return c
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

type RestapiTestSuite struct {
	suite.Suite

	server  *httptest.Server
	dp      *Dispatcher
	conn1   *client.Client
	conn2   *client.Client
	tracker *tracking.TaskTracker
	kvstore *kvprovider.KVMulti

	cancel context.CancelFunc
}

func TestRestapi(t *testing.T) {
	suite.Run(t, new(RestapiTestSuite))
}

func (s *RestapiTestSuite) SetupTest() {
	st.Events.Sources = `
sources:
  testing:
    description: FOR TESTING
    elastic:
      number_of_replicas: 2
      number_of_shards: 3
    icon_class: fa-ambulance
    partition_unit: year
    references:
      - description: Task tracking identifier for incident or request
        name: task_id
        required: true
  samples: {}
`
	st.Events.Kafka.TopicPrefix = "unit01"
	st.Streams.Backend = "local"
	topics.RegenTopics()

	s.cancel = testdata.InitGlobalContext()
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	s.kvstore = kvprov
	require.Nil(s.T(), err)
	s.dp = NewDispatcher(prov, kvprov, testdata.GetGlobalTestContext())
	s.server = httptest.NewServer(s.dp.Router)
	s.conn1 = GetConnection(s.server.URL, "p1")
	s.conn2 = GetConnection(s.server.URL, "p2")

	s.tracker, err = tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(s.T(), err)
}

func (s *RestapiTestSuite) TearDownTest() {
	defer st.ResetSettings()
	s.server.Close()
	s.dp.Stop()
	s.cancel()
}

func (s *RestapiTestSuite) TestSimulatePlugin() {
	t := s.T()

	// publish invalid avro data (not bulk)
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	raw, err := bse1.ToAvro()
	require.Nil(t, err)
	_, err = s.conn1.PostEventsBytes(raw, &client.PublishEventsOptions{Sync: true, Model: events.ModelBinary, AvroFormat: true})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "avro event did not match expected bulk schema")

	// publish json with avro flag
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	require.Nil(t, bse1.CheckValid())
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	rawdata, err := json.Marshal(&bulk)
	require.Nil(t, err)
	_, err = s.conn1.PostEventsBytes(rawdata, &client.PublishEventsOptions{Sync: true, Model: events.ModelBinary, IncludeOk: true, AvroFormat: true})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "supplied events were in json format, not avro")

	// publish avro without flag
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	require.Nil(t, bse1.CheckValid())
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	rawdata, err = bulk.ToAvro()
	require.Nil(t, err)
	_, err = s.conn1.PostEventsBytes(rawdata, &client.PublishEventsOptions{Sync: true, Model: events.ModelBinary, IncludeOk: true, AvroFormat: false})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "ensure events were in json format")

	//
	// start the plugin2 queues
	//
	_, info, err := s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true,
		RequireExpedite: true,
		RequireLive:     true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched) // expect event

	//
	// publish a binary sourced event with plugin 1
	//
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	require.Nil(t, bse1.CheckValid())
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	resp, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true, IncludeOk: true})
	require.Nil(t, err)
	require.Equal(t, resp.TotalOk, 1)
	require.Equal(t, resp.TotalFailures, 0)
	require.Equal(t, len(resp.Ok), 1)
	// check that 'ok' has valid json data
	require.Equal(t, resp.Ok[0].(map[string]any)["action"], "sourced")
	require.Equal(t, resp.Ok[0].(map[string]any)["model_version"], float64(events.CurrentModelVersion))

	err = bse1.UpdateTrackingFields()
	require.Nil(t, err)

	//
	// read binary events with plugin 2
	//
	// only 1 event as live will be placed after the event we just published
	data, info, err := s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true,
		RequireExpedite: true,
		RequireLive:     true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched) // expect event

	time.Sleep(1 * time.Second)

	//
	// check tracking dequeued
	//
	track, err := s.tracker.GetRedisTaskStarted(data.Events[0].Dequeued)
	require.Nil(t, err)
	require.NotNil(t, string(track))

	// check published event against what we sent
	bse1.Dequeued = data.Events[0].Dequeued
	bse1.KafkaKey = data.Events[0].KafkaKey
	testdata.MarshalEqual(t, bse1, &data.Events[0])
	require.Equal(t, data.Events[0].Source.Security, "RESTRICTED")

	//
	// post status result with plugin 2
	//
	// upload a completion to check that dequeued item is resolved
	ev := testdata.GenEventStatus("myid")
	require.Nil(t, err)
	ev.Entity.Input = *data.Events[0]

	bulk2 := events.BulkStatusEvent{Events: []*events.StatusEvent{ev}}
	resp, err = s.conn2.PostEvents(&bulk2, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)
	log.Printf("%v", resp)
	require.Equal(t, resp.TotalOk, 1)
	require.Equal(t, len(resp.Ok), 0)

	// // publish a mix of valid and invalid events
	// bad := BadEvent{BadProperty: 8}
	// resp, err = s.conn2.PostEvents([]any{ev, ev, ev, bad}, &client.PublishEventsOptions{Sync: true})
	// require.Nil(t, err)
	// log.Printf("%v", resp)
	// require.Equal(t, resp.TotalOk, 3)
	// require.Equal(t, resp.TotalFailures, 1)
	// require.Equal(t, len(resp.Failures), 1)
	// require.Equal(t, resp.Failures[0].Error, "unmarshalAnyEntity failed to parse json event with len(46): invalid character ',' looking for beginning of value")
	// require.Equal(t, resp.Failures[0].Event, `{"bad_property":8}`)
}

func (s *RestapiTestSuite) TestBinaryEventOnlyHistoric() {
	var err error
	t := s.T()
	//
	// publish a binary sourced event with plugin 1
	//
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	//
	// read binary events with plugin 2
	//

	// should fail to read events
	_, info, err := s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true,
		RequireSources:  []string{"notexist"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)

	// should succeed on reading event
	_, info, err = s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true,
		RequireSources:  []string{"testing"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched)
}

// post json events to dispatcher
func (s *RestapiTestSuite) TestPostBadBinaryEvents() {
	var err error
	t := s.T()
	// publish an empty binary event
	_, err = s.conn1.PostEventsBytes([]byte(""), &client.PublishEventsOptions{Sync: true, Model: "binary"})
	var clientError *client.HttpError
	success := errors.As(err, &clientError)
	require.True(t, success)
	require.Equal(t, clientError.StatusCode, 400)
}

// post json events to dispatcher
func (s *RestapiTestSuite) TestBinaryEventPostJson() {
	var err error
	t := s.T()
	//
	// publish an old binary augmented event with plugin 1, should fail due to being old
	//
	encoded := testdata.GetEventBytes("events/restapi/3_binary_js_deob.json")
	tosend := fmt.Sprintf(`[%s]`, encoded)
	_, err = s.conn1.PostEventsBytes([]byte(tosend), &client.PublishEventsOptions{Sync: true, Model: "binary"})
	require.ErrorContains(t, err, "event message version was too old")

	// publish the upgraded version should work
	encoded = testdata.GetEventBytes("events/restapi/3_binary_js_deob_done.json")
	tosend = fmt.Sprintf(`[%s]`, encoded)
	resp, err := s.conn1.PostEventsBytes([]byte(tosend), &client.PublishEventsOptions{Sync: true, Model: "binary"})
	require.Nil(t, err)
	require.Equal(t, resp.TotalOk, 1)

	//
	// read binary events with plugin 2
	//

	// should fail to read events
	bulk2 := events.BulkBinaryEvent{}
	raw, info, err := s.conn2.GetEventsBytes(&client.FetchEventsStruct{
		Model: events.ModelBinary,
		Count: 1000, Deadline: 1, IsTask: true, AvroFormat: false,
		RequireSources:  []string{"notexist"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)
	err = json.Unmarshal(raw, &bulk2)
	require.Nil(t, err)
	require.Equal(t, len(bulk2.Events), 0)

	// should succeed on reading event
	raw, info, err = s.conn2.GetEventsBytes(&client.FetchEventsStruct{
		Model: events.ModelBinary,
		Count: 1000, Deadline: 1, IsTask: true, AvroFormat: false,
		RequireSources:  []string{"samples"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched)
	err = json.Unmarshal(raw, &bulk2)
	require.Nil(t, err)
	require.Equal(t, len(bulk2.Events), 1)
}

// ensure that json response is also a valid bulk message
func (s *RestapiTestSuite) TestBinaryEventOnlyHistoricJson() {
	var err error
	t := s.T()
	//
	// publish a binary sourced event with plugin 1
	//
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	//
	// read binary events with plugin 2
	//

	// should fail to read events
	bulk2 := events.BulkBinaryEvent{}
	raw, info, err := s.conn2.GetEventsBytes(&client.FetchEventsStruct{
		Model: events.ModelBinary,
		Count: 1000, Deadline: 1, IsTask: true, AvroFormat: false,
		RequireSources:  []string{"notexist"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)
	err = json.Unmarshal(raw, &bulk2)
	require.Nil(t, err)
	require.Equal(t, len(bulk2.Events), 0)

	// should succeed on reading event
	raw, info, err = s.conn2.GetEventsBytes(&client.FetchEventsStruct{
		Model: events.ModelBinary,
		Count: 1000, Deadline: 1, IsTask: true, AvroFormat: false,
		RequireSources:  []string{"testing"},
		RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched)
	err = json.Unmarshal(raw, &bulk2)
	require.Nil(t, err)
	require.Equal(t, len(bulk2.Events), 1)
}

// post and get source binary event
// unusual for plugins, but this is how azul recovery works
func (s *RestapiTestSuite) TestBinaryEventDirect() {
	var err error
	t := s.T()

	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse1}}
	rpe, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)
	log.Printf("%v", rpe.Failures)
	require.Equal(t, rpe.TotalOk, 1)

	err = bse1.UpdateTrackingFields()
	require.Nil(t, err)

	// only 1 event as live will be placed after the event we just published
	data, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched) // expect event

	// check published event against what we sent
	bse1.Dequeued = ""
	bse1.KafkaKey = data.Events[0].KafkaKey
	testdata.MarshalEqual(t, bse1, &data.Events[0])
	require.Equal(t, data.Events[0].Source.Security, "RESTRICTED")

	// test another event
	bse2 := testdata.GenEventBinary(&testdata.BC{ID: "b2"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{bse2}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	err = bse2.UpdateTrackingFields()
	require.Nil(t, err)

	// 2 events as both subscriptions point at doc we just published
	data, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched) // expect event

	// check published event against what we sent
	bse2.Dequeued = ""
	bse2.KafkaKey = data.Events[0].KafkaKey
	testdata.MarshalEqual(t, bse2, &data.Events[0])
	require.Equal(t, data.Events[0].Source.Security, "RESTRICTED")
	testdata.MarshalEqual(t, bse2, &data.Events[1])
	require.Equal(t, data.Events[1].Source.Security, "RESTRICTED")
}

func (s *RestapiTestSuite) TestPluginEvents() {
	var err error
	t := s.T()

	// set offset in kafka to live
	_, _, err = s.conn1.GetPluginEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)

	ev1 := testdata.GenEventRegister("b1")
	bulk := events.BulkPluginEvent{Events: []*events.PluginEvent{ev1}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	// only 1 event as live will be placed after the event we just published
	data, info, err := s.conn1.GetPluginEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched)

	// check published event against what we sent
	ev1.KafkaKey = data.Events[0].KafkaKey
	testdata.MarshalEqual(t, ev1, &data.Events[0])
}

/*
This test is used to verify that pausing events works as expected.
conn1 - simulate a plugin (isTask True and DenyHistoric is false)
conn2 - simulate an ingestor (isTask False)

Publish events
have the plugin ingest them creating a consumer group.
Publish events with pause_plugins set to true.

Plugin tries to consume again and is denied.
Ingestor tries to consume and gets all events.

Pause is manually moved to point where plugins should skip to latest.
Consume with plugin (it should jump to latest) and get no events.
Publish 1 more event.
Consume with plugin it should get the event.

(regression tests)
Verify that historic ingestor doesn't skip to live.
Verify that historic plugin does skip to live.
*/
func (s *RestapiTestSuite) TestPluginPausesEvents() {
	// Make sure pause is removed.
	defer pauser.ClearLastPauseTime(testdata.GetGlobalTestContext(), s.kvstore)
	var err error
	t := s.T()

	// set offset in kafka to live
	_, _, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	// set offset to live for connection 2 as well.
	_, _, err = s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireExpedite: true,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)

	// Generate first binary to setup plugin consumer
	ev1 := testdata.GenEventBinary(&testdata.BC{ID: "1"})
	bulk := events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev1}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	// 1 historic and 1 live event provided of the same event.
	_, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched)

	// Generate an event to pause plugins, and another event a plugin can in read.
	ev2 := testdata.GenEventBinary(&testdata.BC{ID: "2"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev2}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true, PausePlugins: true})
	require.Nil(t, err)

	ev3 := testdata.GenEventBinary(&testdata.BC{ID: "3"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev3}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	// Ensure the pause has been applied takes affect.
	require.Eventually(t, func() bool {
		paused, _ := pauser.IsPluginProcessingPaused(testdata.GetGlobalTestContext(), s.kvstore)
		return paused
	}, time.Duration(3)*time.Second, time.Duration(10)*time.Millisecond, "Plugin was never processing was never paused and it should have been.")

	// Plugin fails to read any events because plugins are paused.
	_, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireLive: true, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)

	// Ingestor fetching should still find 3 events (note no historic which is why 3 instead of 6)
	_, info, err = s.conn2.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireExpedite: true,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 3, info.Fetched)

	// Remove pause and fetch plugins should get 0 events because plugin (live and historic) are at latest
	// Set pause time to be just after pause ends.
	newTime := time.Now().Add(-pauser.PAUSE_TIME_BEFORE_RESUME - time.Duration(1)*time.Minute)
	s.kvstore.PausePluginProcessingStartTime.Set(testdata.GetGlobalTestContext(), pauser.PAUSE_PLUGIN_FLAG_KEY, newTime, 0)
	isPaused, err := pauser.IsPluginProcessingPaused(testdata.GetGlobalTestContext(), s.kvstore)
	require.Nil(t, err)
	require.False(t, isPaused)

	_, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true, RequireLive: true, RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)

	// Publish one more event and verify plugin can now read events that occurred after latest.
	ev4 := testdata.GenEventBinary(&testdata.BC{ID: "4"})
	bulk = events.BulkBinaryEvent{Events: []*events.BinaryEvent{ev4}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	_, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, IsTask: true, RequireLive: true, RequireHistoric: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched)

	// Ensure a historic ingestor doesn't skip to live.
	conn3 := GetConnection(s.server.URL, "p3")
	_, info, err = conn3.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: false,
	})
	require.Nil(t, err)
	// Must find at least the 4 events that were published during this test (may find more from other tests).
	require.GreaterOrEqual(t, info.Fetched, 4)

	// Ensure a historic Plugin skips to live. (as skip is still enabled)
	conn4 := GetConnection(s.server.URL, "p4")
	_, info, err = conn4.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, info.Fetched, 0)
}

func (s *RestapiTestSuite) TestDeleteEvents() {
	var err error
	t := s.T()

	// set offset in kafka to live
	_, _, err = s.conn1.GetDeleteEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)

	ev1 := testdata.GenEventDelete("b1")
	bulk := events.BulkDeleteEvent{Events: []*events.DeleteEvent{ev1}}
	rpe, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)
	log.Printf("PublishEvent: %v", rpe)
	require.Equal(t, rpe.TotalFailures, 0)
	require.Equal(t, rpe.TotalOk, 1)

	// only 1 event as live will be placed after the event we just published
	data, info, err := s.conn1.GetDeleteEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1,
		RequireLive: true, IsTask: false,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched)

	// check published event against what we sent
	ev1.KafkaKey = data.Events[0].KafkaKey
	testdata.MarshalEqual(t, ev1, &data.Events[0])
}

// post and get status event with results
func (s *RestapiTestSuite) TestStatusEventWithBinaryEvent() {
	var err error
	t := s.T()
	// publish a binary produced status with sub events
	sendEventStatus := testdata.GenEventStatus("1")
	sendEventEntities := sendEventStatus.Entity

	bulk := events.BulkStatusEvent{Events: []*events.StatusEvent{sendEventStatus}}
	resp, err := s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)
	require.Equal(t, resp.TotalOk, 1)

	// check published status message
	recvEventStatuses, info, err := s.conn1.GetStatusEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, info.Fetched) // expect event

	// compare status messages, ignoring entity component
	// recv status will have removed results
	sendEventStatus.Entity.Results = []events.BinaryEvent{}
	sendEventStatus.KafkaKey = recvEventStatuses.Events[0].KafkaKey
	MarshalEqual(t, sendEventStatus, &recvEventStatuses.Events[0])

	// check published binary messages 1
	recv_events_binary, info, err := s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireHistoric: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched) // expect event x2

	// check messages
	for i := 0; i < 2; i++ {
		event_binary := recv_events_binary.Events[i]
		if event_binary.Action == events.ActionEnriched {
			expected := sendEventEntities.Results[0]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			MarshalEqual(t, &expected, &event_binary)
		} else if event_binary.Action == events.ActionExtracted {
			expected := sendEventEntities.Results[1]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			MarshalEqual(t, &expected, &event_binary)
		} else {
			fmt.Printf("bad thing '%s'", event_binary.KafkaKey)
			panic(5)
		}
	}

	// Refetch events with historical disabled (only retrieve events after this has first asked for events)
	_, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireExpedite: true,
		RequireLive: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 0, info.Fetched)

	sendEventStatus = testdata.GenEventStatus("2")
	sendEventEntities = sendEventStatus.Entity
	bulk = events.BulkStatusEvent{Events: []*events.StatusEvent{sendEventStatus}}
	_, err = s.conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true})
	require.Nil(t, err)

	recv_events_binary, info, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{
		Count: 1000, Deadline: 1, RequireExpedite: true,
		RequireLive: true, IsTask: true,
	})
	require.Nil(t, err)
	require.Equal(t, 2, info.Fetched) // expect event x2

	// check messages
	for i := 0; i < 2; i++ {
		event_binary := recv_events_binary.Events[i]
		if event_binary.Action == events.ActionEnriched {
			expected := sendEventEntities.Results[0]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			MarshalEqual(t, &expected, &event_binary)
		} else if event_binary.Action == events.ActionExtracted {
			expected := sendEventEntities.Results[1]
			expected.UpdateTrackingFields()
			expected.KafkaKey = event_binary.KafkaKey
			expected.Dequeued = event_binary.Dequeued
			expected.Flags = events.BinaryFlags{}
			MarshalEqual(t, &expected, &event_binary)
		} else {
			fmt.Printf("bad thing '%s'", event_binary.KafkaKey)
			panic(5)
		}
	}
}

// Test a simple set of consumers work with the simulation against an event.
func (s *RestapiTestSuite) TestSimulateConsumersOnEventBytes() {
	var err error
	t := s.T()
	conn3 := GetConnection(s.server.URL, "restapi")

	// do some consumes to get registered
	_, _, err = s.conn1.GetBinaryEvents(&client.FetchEventsStruct{Deadline: 1, RequireUnderContentSize: 300,
		RequireLive: true, RequireHistoric: true})
	require.Nil(t, err)
	_, _, err = s.conn2.GetBinaryEvents(&client.FetchEventsStruct{Deadline: 1, RequireUnderContentSize: 2000,
		RequireLive: true, RequireHistoric: true})
	require.Nil(t, err)

	// simulate against custom event
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bse1.UpdateTrackingFields()
	marshalled, err := json.Marshal(bse1)
	require.Nil(t, err)

	result, err := conn3.SimulateConsumersOnEventBytes(marshalled)
	require.Nil(t, err)
	require.ElementsMatch(t, result.Consumers, []models.EventSimulateConsumer{
		{
			Name:             "testp1",
			Version:          "1",
			FilterOut:        true,
			FilterOutTrigger: "FilterConsumerRules-reject_content_too_large",
		},
		{
			Name:             "testp2",
			Version:          "1",
			FilterOut:        false,
			FilterOutTrigger: "",
		},
	})
}

func (s *RestapiTestSuite) TestPostStream() {
	var err error
	t := s.T()
	conn3 := GetConnection(s.server.URL, "restapi")

	// simple upload
	reader := bytes.NewReader([]byte("this is a file with a certain amount of content that it has within it that represents some information with text based data and some other things."))
	resp, err := conn3.PostStream("source", events.DataLabelContent, reader, &client.PostStreamStruct{})
	require.Nil(t, err)
	require.Equal(t, resp, &events.BinaryEntityDatastream{
		IdentifyVersion:  0x2,
		Label:            "content",
		Size:             146,
		Sha512:           "6f084d0b41d6b711b2fb576ccc29b2d9cd5c146cfec553b8c00b71d8e1f9887aa88a1386f426f92683d5c2013ce151524a093079b33cb4457fdf3f8a4bee48e1",
		Sha256:           "7e6257e8e45eb7330d846e05ca61ae9717f49ca0d2a3f29e64d16f846bfb1758",
		Sha1:             "c9b75f1dc6df8329ccb4b324bf59d1cbf7cb2b2a",
		Md5:              "f83621f2a9e7faed39f2aecdaeaa863e",
		Ssdeep:           "",
		Tlsh:             "T1abc08ce323800a6080c802a8268330087f04c0bc0904d03d2c88506d1645d333223f96",
		Mime:             "text/plain",
		Magic:            "ASCII text, with no line terminators",
		FileFormatLegacy: "Text",
		FileFormat:       "text/plain",
		FileExtension:    "txt",
		Language:         "",
	})

	// upload with trusted sha256
	reader = bytes.NewReader([]byte("this is a file with a certain amount of content that it has within it that represents some information with text based data and some other things."))
	resp, err = conn3.PostStream("source", events.DataLabelContent, reader, &client.PostStreamStruct{
		SkipIdentify:   true,
		ExpectedSha256: "7e6257e8e45eb7330d846e05ca61ae9717f49ca0d2a3f29e64d16f846bfb1758",
	})
	require.Nil(t, err)
	require.Equal(t, resp, &events.BinaryEntityDatastream{
		IdentifyVersion:  0,
		Label:            "content",
		Size:             146,
		Sha512:           "",
		Sha256:           "7e6257e8e45eb7330d846e05ca61ae9717f49ca0d2a3f29e64d16f846bfb1758",
		Sha1:             "",
		Md5:              "",
		Ssdeep:           "",
		Tlsh:             "",
		Mime:             "",
		Magic:            "",
		FileFormatLegacy: "",
		FileFormat:       "",
		FileExtension:    "",
		Language:         "",
	})
}
