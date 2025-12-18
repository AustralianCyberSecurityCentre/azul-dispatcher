package restapi

import (
	"net/http/httptest"
	"testing"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/testutils"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
)

func (s *Benchmarker) SetupSuiteMock() {
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
	st.Streams.Backend = "local"
	// clear local store
	_, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	if err != nil {
		panic(err.Error())
	}
	topics.RegenTopics()
	// logging can be expensive so turn off debug and such
	bedSet.RecreateLogger("WARN")
	s.cancel = testdata.InitGlobalContext()
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	if err != nil {
		panic(err)
	}
	cancelFunc := testdata.InitGlobalContext()
	defer cancelFunc()
	s.dp = NewDispatcher(prov, kvprov, testdata.GetGlobalTestContext())
	s.server = httptest.NewServer(s.dp.Router)
	s.fileManager, err = testutils.NewFileManager()
	if err != nil {
		panic(err)
	}
}

func (s *Benchmarker) TearDownSuiteMock() {
	defer st.ResetSettings()
	s.server.Close()
	s.dp.Stop()
	s.cancel()
}

func BenchmarkGetEventsMock(t *testing.B) {
	s := Benchmarker{}
	s.SetupSuiteMock()
	defer s.TearDownSuiteMock()
	benchmarkGetEvents(t, &s)
}

func BenchmarkPostEventsMock(t *testing.B) {
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteMock()
	defer s.TearDownSuiteMock()
	benchmarkPostEvents(t, &s)
}

func BenchmarkPostStreamsMock(t *testing.B) {
	// perChunk := 100
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteMock()
	defer s.TearDownSuiteMock()
	benchmarkPostStreams(t, &s)
}
func BenchmarkPostStreamsMockSkipIdentify(t *testing.B) {
	// suspect there is some kind of caching going on here as its much faster
	// benchmark speed of uploading events to dispatcher
	s := Benchmarker{}
	s.SetupSuiteMock()
	defer s.TearDownSuiteMock()
	benchmarkPostStreamsSkipIdentify(t, &s)
}
