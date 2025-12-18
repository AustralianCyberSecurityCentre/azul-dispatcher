//go:build integration

package restapi

import (
	"net/http/httptest"
	"testing"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/testutils"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
)

func (s *Benchmarker) SetupSuiteInt() {
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
`
	st.Events.Kafka.TopicPrefix = "test02"
	topics.RegenTopics()
	// logging can be expensive so turn off debug and such
	bedSet.RecreateLogger("WARN")
	s.cancel = testdata.InitGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewRedisProviders()
	if err != nil {
		panic(err)
	}
	s.dp = NewDispatcher(prov, kvprov, testdata.GetGlobalTestContext())
	s.server = httptest.NewServer(s.dp.Router)
	s.fileManager, err = testutils.NewFileManager()
	if err != nil {
		panic(err)
	}
}

func (s *Benchmarker) TearDownSuiteInt() {
	defer st.ResetSettings()
	s.server.Close()
	s.dp.Stop()
	s.cancel()
}

func BenchmarkGetEventsInt(t *testing.B) {
	s := Benchmarker{}
	s.SetupSuiteInt()
	defer s.TearDownSuiteInt()
	benchmarkGetEvents(t, &s)
}

func BenchmarkPostEventsInt(t *testing.B) {
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteInt()
	defer s.TearDownSuiteInt()
	benchmarkPostEvents(t, &s)
}

func BenchmarkPostStreamsInt(t *testing.B) {
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteInt()
	defer s.TearDownSuiteInt()
	benchmarkPostStreams(t, &s)
}

func BenchmarkPostStreamsSkipIdentifyInt(t *testing.B) {
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteInt()
	defer s.TearDownSuiteInt()
	benchmarkPostStreamsSkipIdentify(t, &s)
}
