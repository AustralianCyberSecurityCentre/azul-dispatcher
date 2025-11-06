package pipeline_produce

import (
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const psr_dir = "events/pipelines/produce_status_results/"

var psr_unrelated = testdata.GetEventBytes(psr_dir + "simple.json")

var psr_example1 = testdata.GetEventBytes(psr_dir + "example1.json")
var psr_example1_nores = testdata.GetEventBytes(psr_dir + "example1_nores.json")
var psr_example1_res = testdata.GetEvent(psr_dir + "example1_res.json")

var psr_expedite_binary = testdata.GetEventBytes(psr_dir + "expedite_binary.json")

var r1_example = testdata.GetEventBytes(psr_dir + "example1_res.json")
var r2_example = testdata.GetEventBytes(psr_dir + "example1.json")

// Confirm filter doesnt do anything on binary events
func TestPSRBinaryEvent(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	prod := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		NewProduceStatusResults(tracker),
	})
	beforeAction := gjson.Get(string(r1_example), "entity.results")
	require.Equal(t, beforeAction.Exists(), false)

	inFlight, err := pipeline.NewMsgInFlightFromJson(r1_example, events.ModelBinary)
	require.Nil(t, err)
	res, _ := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight}, &produceParams)
	msg, err := json.Marshal(res[0])
	require.Nil(t, err)
	parsedResults := gjson.Get(string(msg), "entity.results")
	require.Equal(t, parsedResults.Exists(), false)

}

// confirm filter works on standard status
func TestPSRRemoveStatusResults(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	prod := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		NewProduceStatusResults(tracker),
	})

	// make dequeued
	bin := testdata.GenEventBinary(nil)
	bin.Dequeued = "8c5e67a7ad83fd5cd19fffb7d94941af-MimeDecoder-2021.02.14"

	tracker.DoProcessingStarted(bin, "p1", "v1")
	track, err := tracker.GetRedisTaskStarted(bin.Dequeued)
	require.Nil(t, err)
	require.Greater(t, len(track), 1000)

	// Confirm filter works when it removes a value.
	beforeAction := gjson.Get(string(r2_example), "entity.results")
	require.Equal(t, beforeAction.Exists(), true)
	inFlight, err := pipeline.NewMsgInFlightFromJson(r2_example, events.ModelStatus)
	require.Nil(t, err)
	status, _ := inFlight.GetStatus()
	status.Entity.Input.Dequeued = bin.Dequeued
	res, _ := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight}, &produceParams)
	msg, err := json.Marshal(res[0])
	require.Nil(t, err)
	parsedResults := gjson.Get(string(msg), "entity.results")
	require.Equal(t, parsedResults.Exists(), false)

	// Wait for deletion to finish in async function
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// confirm not dequeued
		track, err = tracker.GetRedisTaskStarted(bin.Dequeued)
		assert.Nil(collect, err)
		assert.Nil(collect, track, string(track))
	}, 2*time.Second, 5*time.Millisecond)
}

func TestPSRSimple(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	// check that the example status event results in creation of binary event as well
	prod := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		NewProduceStatusResults(tracker),
	})
	inFlight, err := pipeline.NewMsgInFlightFromJson(psr_unrelated, events.ModelBinary)
	require.Nil(t, err)
	fin, _ := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight}, &produceParams)
	require.Equal(t, len(fin), 1)
	msg, err := json.Marshal(fin[0])
	require.Nil(t, err)
	require.JSONEq(t, string(msg), string(psr_unrelated))

	inFlight, err = pipeline.NewMsgInFlightFromJson(psr_example1, events.ModelStatus)
	require.Nil(t, err)
	fin, _ = prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight}, &produceParams)
	require.Equal(t, len(fin), 2)
	msg, err = json.Marshal(fin[0])
	require.Nil(t, err)
	require.JSONEq(t, string(psr_example1_nores), string(msg))
	msg, err = json.Marshal(fin[1])
	require.Nil(t, err)
	require.JSONEq(t, psr_example1_res, string(msg))
}

func TestPSRExpedite(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	prod := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		NewProduceStatusResults(tracker),
	})

	// test binary children with input expedite
	inFlight, err := pipeline.NewMsgInFlightFromJson(psr_expedite_binary, events.ModelStatus)
	require.Nil(t, err)
	fin, _ := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight}, &produceParams)
	require.Equal(t, 2, len(fin))
	msg, err := json.Marshal(fin[1])
	require.Nil(t, err)
	require.Equal(t, true, gjson.GetBytes(msg, "flags.expedite").Bool())
}
