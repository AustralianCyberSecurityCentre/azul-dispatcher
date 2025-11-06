package pipeline

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

const testPath = "events/pipelines/produce/"

var simple = testdata.GetEventBytes(testPath + "simple.json")
var source_security_change = testdata.GetEventBytes(testPath + "source_security_change.json")
var path_security_change = testdata.GetEventBytes(testPath + "path_security_change.json")
var source_ref_change = testdata.GetEventBytes(testPath + "source_ref_change.json")
var author_change = testdata.GetEventBytes(testPath + "author_change.json")

type testProduceAction1 struct{}
type testProduceAction2 struct{}
type testProduceAction3 struct{}
type testProduceAction4 struct{}

var counter1 = 0
var counter2 = 0
var counter3 = 0

func (tp testProduceAction1) ProduceMod(message *msginflight.MsgInFlight, meta *ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	counter1++
	// add another event
	inFlight, err := NewMsgInFlightFromJson(source_ref_change, events.ModelBinary)
	if err != nil {
		panic(err)
	}
	return message, []*msginflight.MsgInFlight{inFlight}
}
func (tp testProduceAction1) GetName() string { return "pa1" }

func (tp testProduceAction2) ProduceMod(message *msginflight.MsgInFlight, meta *ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	counter2++
	return message, nil
}
func (tp testProduceAction2) GetName() string { return "pa2" }

func (tp testProduceAction3) ProduceMod(message *msginflight.MsgInFlight, meta *ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	counter3++
	inFlight, err := NewMsgInFlightFromJson(author_change, events.ModelBinary)
	if err != nil {
		panic(err)
	}
	return inFlight, nil
}
func (tp testProduceAction3) GetName() string { return "pa3" }

func (tp testProduceAction4) ProduceMod(message *msginflight.MsgInFlight, meta *ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	return nil, nil
}
func (tp testProduceAction4) GetName() string { return "pa4" }

func resetCounter() {
	counter1 = 0
	counter2 = 0
	counter3 = 0
}

func forceToJson(t *testing.T, inFlight *msginflight.MsgInFlight) string {
	ret, err := inFlight.MarshalJSON()
	require.Nil(t, err)
	return string(ret)
}

func TestSimple(t *testing.T) {
	produceParams := ProduceParams{UserAgent: "test"}
	prod := NewProducePipeline([]ProduceAction{&testProduceAction1{}, &testProduceAction2{}})
	resetCounter()

	inFlight1, err := NewMsgInFlightFromJson(simple, events.ModelBinary)
	require.Nil(t, err)
	fin, _ := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight1}, &produceParams)
	require.Equal(t, len(fin), 2)
	require.Equal(t, counter1, 1)
	require.Equal(t, counter2, 2)

	require.JSONEq(t, forceToJson(t, fin[0]), string(simple))
	require.JSONEq(t, forceToJson(t, fin[1]), string(source_ref_change))

	resetCounter()
	inFlight1, err = NewMsgInFlightFromJson(source_security_change, events.ModelBinary)
	require.Nil(t, err)
	inFlight2, err := NewMsgInFlightFromJson(path_security_change, events.ModelBinary)
	require.Nil(t, err)
	fin, _ = prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight1, inFlight2}, &produceParams)
	require.Equal(t, len(fin), 4)
	require.Equal(t, counter1, 2)
	require.Equal(t, counter2, 4)
	require.JSONEq(t, forceToJson(t, fin[0]), string(source_security_change))
	require.JSONEq(t, forceToJson(t, fin[1]), string(path_security_change))
	require.JSONEq(t, forceToJson(t, fin[2]), string(source_ref_change))
	require.JSONEq(t, forceToJson(t, fin[3]), string(source_ref_change))

	prod = NewProducePipeline([]ProduceAction{&testProduceAction1{}, &testProduceAction3{}})
	resetCounter()
	inFlight1, err = NewMsgInFlightFromJson(simple, events.ModelBinary)
	require.Nil(t, err)
	fin, _ = prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight1}, &produceParams)
	require.Equal(t, len(fin), 2)
	require.Equal(t, counter1, 1)
	require.Equal(t, counter3, 2)
	require.JSONEq(t, forceToJson(t, fin[0]), string(author_change))
	require.JSONEq(t, forceToJson(t, fin[1]), string(author_change))
}

/*Test that when a producer drops a message it provides it's name which is useful for the restapi to give users.*/
func TestProducerThatDropsMessages(t *testing.T) {
	produceParams := ProduceParams{UserAgent: "test"}
	prod := NewProducePipeline([]ProduceAction{&testProduceAction4{}})

	inFlight1, err := NewMsgInFlightFromJson(simple, events.ModelBinary)
	require.Nil(t, err)
	fin, produceActionInfo := prod.RunProduceActions([]*msginflight.MsgInFlight{inFlight1}, &produceParams)
	// No results as everything gets filtered out.
	require.Equal(t, len(fin), 0)
	// Filtered results display the name of the filter that dropped them
	require.Equal(t, produceActionInfo.ProducersThatDroppedEvents, map[string]int{"pa4": 1})
}
