package producer

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	bedsettings "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

var flExampleLarge = testdata.GetEventBytes("events/pipelines/produce_status_results/example1.json")

func TestChooseTopic(t *testing.T) {
	defer st.ResetSettings()

	// test status message
	inFlight, err := pipeline.NewMsgInFlightFromJson(flExampleLarge, events.ModelStatus)
	require.Nil(t, err)
	topic, err := chooseTopic(inFlight)
	require.Nil(t, err)
	require.Equal(t, topic, "azul.test01.system.status")

	// test binary event
	ev := testdata.GenEventBinary(&testdata.BC{})
	inFlight, err = msginflight.NewMsgInFlightFromEvent(ev)
	require.Nil(t, err)
	topic, err = chooseTopic(inFlight)
	require.Nil(t, err)
	require.Equal(t, topic, "azul.test01.testing.binary.sourced")

	// test nil flags
	ev.Flags = events.BinaryFlags{}
	inFlight, err = msginflight.NewMsgInFlightFromEvent(ev)
	require.Nil(t, err)
	topic, err = chooseTopic(inFlight)
	require.Nil(t, err)
	require.Equal(t, topic, "azul.test01.testing.binary.sourced")

}

func TestProduce(t *testing.T) {
	defer st.ResetSettings()

	// message size can change after upgrades and such
	// must ensure we set boundary to size of basic event
	inFlight, err := pipeline.NewMsgInFlightFromJson(flExampleLarge, events.ModelStatus)
	require.Nil(t, err)
	out, err := inFlight.Event.ToAvro()
	require.Nil(t, err)
	st.Settings.Events.Kafka.MessageMaxBytes = bedsettings.HumanReadableBytes(len(out) + 10)

	qprov, err := provider.NewMemoryProvider()
	require.Nil(t, err)

	st.Events.Sources = `
sources:
  virustotal: {}
`

	topics.RegenTopics()
	ac, err := topics.NewTopicControl(qprov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	err = ac.EnsureAllTopics()
	require.Nil(t, err)

	p, err := NewProducer(qprov)
	defer p.Stop()
	require.Nil(t, err)

	inFlight, err = pipeline.NewMsgInFlightFromJson(flExampleLarge, events.ModelStatus)
	require.Nil(t, err)
	err = p.publish(true, inFlight)
	require.Nil(t, err)

	// larger event should fail
	inFlight, err = pipeline.NewMsgInFlightFromJson(flExampleLarge, events.ModelStatus)
	require.Nil(t, err)
	status, _ := inFlight.GetStatus()
	status.Entity.Input.Author.Name += "0000000000000000000000000000000000000"
	err = p.publish(true, inFlight)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "message too large for kafka: ")
}
