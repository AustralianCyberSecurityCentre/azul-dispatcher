package events_benchmark

// Last run done on a VM with 16GB RAM, CPU Intel(R) Xeon(R) Platinum 8171M CPU @ 2.60GHz - 2 processors

/*
IMPORTANT:
Due to the caching sarama does and the batching of requests and kafka doing it's own caching

These tests were made consistent by doing two runs of the consumers and only timing the second run.
This gives the most consistent results as long as the number of runs is above about 10,000. (-benchtime=5000x) or more

Testing in this manner was found to be a bit slower but at least gave consistent results and was testing sarama
consumers in steady state.

An important consideration with the events/s is it will be different in production depending on the size of the events.
*/

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

var isSaramaBenchmarkTopicsSetup bool

func (s *Benchmarker) SetupSaramaSuite(t *testing.B) {
	st.Events.Sources = `
sources:
  testing: {}
`
	st.Events.Kafka.TopicPrefix = "unit01"
	st.Streams.Backend = "local"
	st.Events.GlobalPartitionCount = 2
	// logging can be expensive so turn off debug and such
	bedSet.RecreateLogger("WARN")
	s.cancel = testdata.InitGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	if !isSaramaBenchmarkTopicsSetup {
		topicControl, err := topics.NewTopicControl(prov)
		require.Nil(t, err)
		// Delete and re-create all topics - this adds about 30seconds to the benchmarking.
		// err = topicControl.DeleteAllTopics()
		// require.Nil(t, err)
		err = topicControl.CreateAllTopics()
		require.Nil(t, err)
		isSaramaBenchmarkTopicsSetup = true
	}

	topics.RegenTopics()

	s.prov = prov
	topicName, err := topics.GetBinaryTopic("testing", events.ActionSourced)
	require.Nil(t, err)
	s.testTopicName = topicName
	t.Logf("Topic in use is %s", s.testTopicName)
}

func (s *Benchmarker) TearDownSaramaSuite() {
	defer st.ResetSettings()
	s.cancel()
}

// 430796 ns/op
// 408275 ns/op
func BenchmarkSaramaProduceEventsSarama(t *testing.B) {
	s := Benchmarker{}
	s.SetupSaramaSuite(t)
	defer s.TearDownSaramaSuite()
	produceNMessages(t, &s)
}

// 9671 ns/op            103399 events/s
// 10060 ns/op             99406 events/s
func BenchmarkSaramaConsumerPollSarama(t *testing.B) {
	s := Benchmarker{}
	s.SetupSaramaSuite(t)
	defer s.TearDownSaramaSuite()
	benchmarkConsumerPoll(t, &s)
}

// 8344 ns/op            119681 events/s
// 8578 ns/op            116483 events/s
func BenchmarkSaramaConsumerChannelSarama(t *testing.B) {
	s := Benchmarker{}
	s.SetupSaramaSuite(t)
	defer s.TearDownSaramaSuite()
	benchmarkConsumerChannel(t, &s)
}

// 7761 ns/op            128859 events/s
// 9669 ns/op            103425 events/s
func BenchmarkSaramaChannelMultiSarama(t *testing.B) {
	s := Benchmarker{}
	s.SetupSaramaSuite(t)
	defer s.TearDownSaramaSuite()
	benchmarkMultiConsumer(t, &s)
}
