package events_benchmark

// Last run done on a VM with 16GB RAM, CPU Intel(R) Xeon(R) Platinum 8171M CPU @ 2.60GHz - 2 processors

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

var isMemoryBenchmarkTopicsSetup bool

func (s *Benchmarker) SetupMemorySuite(t *testing.B) {
	st.Events.Sources = `
sources:
  testing: {}
`
	st.Events.Kafka.TopicPrefix = "unit01"
	st.Streams.Backend = "local"
	st.Events.GlobalPartitionCount = 2
	// logging can be expensive so turn off debug and such
	st.RecreateLogger("WARN")
	s.cancel = testdata.InitGlobalContext()
	prov, err := provider.NewMemoryProvider()
	require.Nil(t, err)
	topics.RegenTopics()

	topicControl, err := topics.NewTopicControl(prov)
	require.Nil(t, err)
	err = topicControl.CreateAllTopics()
	require.Nil(t, err)

	s.prov = prov
	topicName, err := topics.GetBinaryTopic("testing", events.ActionSourced)
	require.Nil(t, err)
	s.testTopicName = topicName
	t.Logf("Topic in use is %s", s.testTopicName)
}

func (s *Benchmarker) TearDownMemorySuite() {
	defer st.ResetSettings()
	s.cancel()
}

// 721.0 ns/op
func BenchmarkProduceEventsMemory(t *testing.B) {
	s := Benchmarker{}
	s.SetupMemorySuite(t)
	defer s.TearDownMemorySuite()
	produceNMessages(t, &s)
}

// 1884 ns/op            530711 events/s
func BenchmarkConsumerPollMemory(t *testing.B) {
	s := Benchmarker{}
	s.SetupMemorySuite(t)
	defer s.TearDownMemorySuite()
	benchmarkConsumerPoll(t, &s)
}

// 986.3 ns/op         1011161 events/s
func BenchmarkConsumerChannelMemory(t *testing.B) {
	s := Benchmarker{}
	s.SetupMemorySuite(t)
	defer s.TearDownMemorySuite()
	benchmarkConsumerChannel(t, &s)
}

// 2019 ns/op            495362 events/s
func BenchmarkChannelMultiMemory(t *testing.B) {
	s := Benchmarker{}
	s.SetupMemorySuite(t)
	defer s.TearDownMemorySuite()
	benchmarkMultiConsumer(t, &s)
}
