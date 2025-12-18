package pipeline_consume

import (
	"testing"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

var (
	rdsDataBinarySourcedBasic = testdata.GetEventBytes("events/pipelines/consume/dequeued_status/basic.json")
	rdsDataBinarySourcedPath  = testdata.GetEventBytes("events/pipelines/consume/dequeued_status/path.json")
	rdsDataStatus             = testdata.GetEventBytes("events/pipelines/consume/dequeued_status/status.json")
)

type mockProducerCapture struct {
	lastMsg []byte
}

func (m *mockProducerCapture) TransformMsgInFlights(msgInFlights []*msginflight.MsgInFlight, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *pipeline.ProduceActionInfo) {
	return nil, nil
}
func (m *mockProducerCapture) TransformEvents(message []byte, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, *pipeline.ProduceActionInfo, error) {
	m.lastMsg = message
	return nil, nil, nil, nil
}
func (m *mockProducerCapture) ProduceAnyEvents(confirm bool, to_publish []*msginflight.MsgInFlight) error {
	return nil
}

// mockProducerNoop is a mock of ProducerInterface interface.
type mockProducerNoop struct {
}

// Publish mocks for benchmark
func (m *mockProducerNoop) TransformMsgInFlights(msgInFlights []*msginflight.MsgInFlight, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *pipeline.ProduceActionInfo) {
	return nil, nil
}
func (m *mockProducerNoop) TransformEvents(message []byte, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, *pipeline.ProduceActionInfo, error) {
	return nil, nil, nil, nil
}
func (m *mockProducerNoop) ProduceAnyEvents(confirm bool, to_publish []*msginflight.MsgInFlight) error {
	return nil
}

func TestRDSRepublish(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	// Check create status message
	mockProd := mockProducerCapture{}

	n, err := NewRaiseDequeuedStatus(&mockProd, tracker)
	require.Nil(t, err)

	inFlight, err := pipeline.NewMsgInFlightFromJson(rdsDataBinarySourcedBasic, events.ModelBinary)
	require.Nil(t, err)

	binary, ok := inFlight.GetBinary()
	require.True(t, ok)
	binary.Dequeued = "myid"

	err = n.publishDequeuedStatus(binary, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: true})
	require.Nil(t, err)
	raw, err := tracker.GetRedisTaskStarted("myid")
	require.Nil(t, err)
	require.NotNil(t, raw)
	print(string(raw))

	var ev events.BinaryEvent
	err = json.Unmarshal(raw, &ev)
	require.Nil(t, err)
	require.Equal(t, ev.Entity.Sha256, "dac804f3662b2228e43af80f6e0769614bf53d6c8ea16241c80d779de1308c20")
	require.Equal(t, ev.Action, events.ActionSourced)
	require.Equal(t, ev.Source.Name, "user")
}

func TestRDSConsumeMod(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	// Check create status message
	mockProd := mockProducerCapture{}
	n, err := NewRaiseDequeuedStatus(&mockProd, tracker)
	require.Nil(t, err)
	inFlight, err := pipeline.NewMsgInFlightFromJson(rdsDataBinarySourcedBasic, events.ModelBinary)
	require.Nil(t, err)

	warn, message := n.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: true})
	require.Equal(t, warn, "")
	binary, ok := inFlight.GetBinary()
	require.True(t, ok)
	dequeued_id := binary.Dequeued
	// can't know the timestamp so just test the first part
	// this ID changes when the original message is changed (such as if event is upgraded)
	require.Contains(t, dequeued_id, "c8a5760778f2724d1560f.test.1.")
	n.wg.Wait()
	raw, err := tracker.GetRedisTaskStarted(dequeued_id)
	require.Nil(t, err)
	require.NotNil(t, raw)

	var ev events.BinaryEvent
	err = json.Unmarshal(raw, &ev)
	require.Nil(t, err)
	require.Equal(t, ev.Entity.Sha256, "dac804f3662b2228e43af80f6e0769614bf53d6c8ea16241c80d779de1308c20")
	require.Equal(t, ev.Action, events.ActionSourced)
	require.Equal(t, ev.Source.Name, "user")

	// Check id correct and sort is correct
	require.Nil(t, err)
	mockLongPath := mockProducerCapture{}
	n, err = NewRaiseDequeuedStatus(&mockLongPath, tracker)
	require.Nil(t, err)
	inFlight, err = pipeline.NewMsgInFlightFromJson(rdsDataBinarySourcedPath, events.ModelBinary)
	require.Nil(t, err)
	binary, ok = inFlight.GetBinary()
	require.True(t, ok)
	require.Equal(t, len(binary.Entity.Datastreams), 1)
	warn, message = n.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: true})
	require.Equal(t, warn, "")
	require.Equal(t, len(binary.Entity.Datastreams), 1) // ensure that datastreams are not dropped in copy
	dequeued_id = binary.Dequeued
	// can't know the timestamp so just test the first part
	// this ID changes when the original message is changed (such as if event is upgraded)
	require.Contains(t, dequeued_id, "c8a5760778f2724d1560f.test.1.")
	n.wg.Wait()

	// Check if isTask is false that nothing is returned
	require.Nil(t, err)
	mockProdNeverCalled := mockProducerCapture{}
	n, err = NewRaiseDequeuedStatus(&mockProdNeverCalled, tracker)
	require.Nil(t, err)
	inFlight, err = pipeline.NewMsgInFlightFromJson(rdsDataBinarySourcedPath, events.ModelBinary)
	require.Nil(t, err)
	warn, message = n.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: false})
	require.Equal(t, warn, "")
	require.NotNil(t, message)
	binary, ok = inFlight.GetBinary()
	require.True(t, ok)
	require.Equal(t, binary.Dequeued, "")
	require.Equal(t, len(mockProdNeverCalled.lastMsg), 0)

	// Check exit early if status message
	inFlight, err = pipeline.NewMsgInFlightFromJson(rdsDataStatus, events.ModelStatus)
	require.Nil(t, err)
	warn, message = n.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: true})
	require.Equal(t, warn, "")
	require.NotNil(t, message)
}

func BenchmarkRDSStatus(b *testing.B) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(b, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(b, err)
	mockProd := mockProducerNoop{}

	n, err := NewRaiseDequeuedStatus(&mockProd, tracker)
	if err != nil {
		panic(err)
	}

	inFlight, err := pipeline.NewMsgInFlightFromJson(rdsDataBinarySourcedBasic, events.ModelBinary)
	require.Nil(b, err)

	for i := 0; i < b.N; i++ {
		_, _ = n.ConsumeMod(inFlight, &consumer.ConsumeParams{Name: "test", Version: "1", IsTask: true})
	}
}
