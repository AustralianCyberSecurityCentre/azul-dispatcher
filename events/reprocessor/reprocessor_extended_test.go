package reprocessor

import (
	"context"
	"fmt"
	"log"
	"net/http/httptest"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"

	"github.com/IBM/sarama"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// compare two structures by first dumping to json
// this removes problems with time, and other non-normalised data
func testMarshalEqual(t *testing.T, in1, in2 any) {
	raw1, err := json.Marshal(in1)
	require.Nil(t, err)
	raw2, err := json.Marshal(in2)
	require.Nil(t, err)
	require.JSONEq(t, string(raw1), string(raw2))
}

func getAllBinaryEvents(t *testing.T) []*events.BinaryEvent {
	maxSecondsToWait := 1
	var data []*events.BinaryEvent
	for i := 1; i <= maxSecondsToWait; i++ {
		var err error
		newdata, info, err := conn.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 1, IsTask: false,
			RequireExpedite: true,
			RequireLive:     true,
			RequireHistoric: true,
		})
		require.Nil(t, err)
		bedSet.Logger.Printf("topics ready? %v - info %v", info.Ready, info)
		data = append(data, newdata.Events...)
		if info.Ready && int(info.Fetched) == 0 {
			bedSet.Logger.Printf("consumed all")
			break
		}
	}
	return data
}

func getAllInsertEvents(t *testing.T) []*events.InsertEvent {
	maxSecondsToWait := 1
	var data []*events.InsertEvent
	for i := 1; i <= maxSecondsToWait; i++ {
		var err error
		newdata, info, err := conn.GetInsertEvents(&client.FetchEventsStruct{
			Count: 1000, Deadline: 1, IsTask: false,
			RequireLive:     true,
			RequireHistoric: true,
		})
		require.Nil(t, err)
		bedSet.Logger.Printf("topics ready? %v - info %v", info.Ready, info)
		data = append(data, newdata.Events...)
		if info.Ready && int(info.Fetched) == 0 {
			bedSet.Logger.Printf("consumed all")
			break
		}
	}
	return data
}

// must use same server for all tests here as otherwise the partitions will not get deallocated from consumers
// and new consumers with then not see any events
var dp *restapi.Dispatcher
var server *httptest.Server
var conn *client.Client
var prov provider.ProviderInterface
var provmem *provider.MemoryProvider
var kvprov *kvprovider.KVMulti
var tc *topics.TopicControl

func testCreateProviders(t *testing.T) {
	var err error

	provmem, err = provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}
	prov = provmem
	kvprov, err = kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tc, err = topics.NewTopicControl(prov)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}
}

// Configures environmental variables for qa01 topics (src of topics to migrate)
func configureTopicsForQA01() {
	st.Events.Kafka.TopicPrefix = "qa01"
	// st.Events.Reprocess.PreviousPrefix = "qa01"
	st.Events.Sources = `
sources:
  testing: {}
  tasking: {}
  samples: {}
`

	topics.RegenTopics()
	tc.EnsureAllTopics()
}

// Configures environmental variables for qa02 topics (dst of topics to migrate)
func configureTopicsForQA02() {
	st.Events.Kafka.TopicPrefix = "qa02"
	st.Events.Reprocess.PreviousPrefix = "qa01"
	st.Events.Sources = `
sources:
  testing: {}
  tasking: {}
  samples: {}
`

	topics.RegenTopics()
	tc.EnsureAllTopics()
}

var ctx context.Context
var cancelFunc context.CancelFunc

// Opens a regular dispatcher server (/client to Kafka)
func testOpenClient() {
	ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	dp = restapi.NewDispatcher(prov, kvprov, ctx)
	server = httptest.NewServer(dp.Router)
	conn = testdata.GetConnection(server.URL, "events-reprocessor")
}

// Closes a previously running dispatcher instance
func testCloseClient() {
	log.Printf("cancelling dispatcher")
	cancelFunc()
	log.Printf("closing server")
	server.Close()
	log.Printf("stopping dispatcher")
	dp.Stop()
}

func testDeleteTopics() {
	// Need to do this twice for both legacy and qa01 prefixes as
	// both may have been created through this process
	configureTopicsForQA01()
	err := tc.DeleteAllTopics()
	if err != nil {
		panic("could not delete topics")
	}

	configureTopicsForQA02()
	err = tc.DeleteAllTopics()
	if err != nil {
		panic("could not delete topics")
	}
}

// testCreateTopics creates topics needed for tests to run properly
// This is needed as otherwise consumers fail to register properly
func testCreateTopics() {
	// Need to do this twice for both legacy and qa01 prefixes to
	// simulate migrating from an old instance of Azul to a newer
	// one
	configureTopicsForQA01()

	err := tc.CreateAllTopics()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msgf("Failed to create topics")
	}

	configureTopicsForQA02()
	err = tc.CreateAllTopics()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msgf("Failed to create topics")
	}
}

func testQA01AddEvent(t *testing.T, dstTopic string, event []byte) {
	// must create without dispatcher, as otherwise we aren't testing any upgrade logic during reprocessing
	tc.Admin.CreateTopics([]st.GenericKafkaTopicSpecification{{Topic: dstTopic}})

	prod, err := prov.CreateProducer()
	require.Nil(t, err)
	msg := sarama.ProducerMessage{
		Topic:     dstTopic,
		Partition: 0,
		Value:     sarama.ByteEncoder(event),
		Key:       sarama.StringEncoder("id"),
	}
	err = prod.Produce(&msg, true)
	require.Nil(t, err)
}

// Inserts a binary event into the legacy topics
func testAddEventBinarySourced(t *testing.T, bc *testdata.BC, legacyJson bool) *events.BinaryEvent {
	event := testdata.GenEventBinary(bc)
	event.Source.Security = "RESTRICTED"
	var err error
	var encoded []byte
	if legacyJson {
		encoded, err = json.Marshal(event)
	} else {
		encoded, err = event.ToAvro()
	}
	require.Nil(t, err)

	topic := fmt.Sprintf("azul.qa01.%s.binary.sourced", bc.Source)
	if bc.Flags.Expedite {
		topic = "azul.qa01.system.expedite"
	}
	testQA01AddEvent(t, topic, encoded)

	// ensure that returned data has event tracking set
	event.UpdateTrackingFields()
	return event
}

func testAddEventsBinarySourced(t *testing.T, legacyJson bool) []*events.BinaryEvent {
	events := []*events.BinaryEvent{}
	for _, source := range []string{"testing", "tasking"} {
		for i := range 100 {
			bc := &testdata.BC{Source: source, ID: fmt.Sprintf("uniq-%d", i)}
			ev := testAddEventBinarySourced(t, bc, legacyJson)
			events = append(events, ev)
		}
	}
	return events
}

func testAddEventInsert(t *testing.T, legacyJson bool) *events.InsertEvent {
	// not really the correct type but should be fine
	event := testdata.GenEventInsert("inserted")
	var err error
	var encoded []byte
	if legacyJson {
		encoded, err = json.Marshal(event)
	} else {
		encoded, err = event.ToAvro()
	}
	require.Nil(t, err)

	testQA01AddEvent(t, "azul.qa01.system.insert", encoded)
	return event
}

// test that an event published to qa01 is correctly copied to qa02
func TestCopyEventBinaryAvro(t *testing.T) {
	defer st.ResetSettings()
	st.Events.Reprocess.UpgradeSources = true
	st.Events.Reprocess.UpgradeSystem = false
	st.Settings.Events.IgnoreTopicMismatch = true
	st.Settings.Streams.Backend = "local"
	bedSet.Settings.LogLevel = "debug"
	testCreateProviders(t)

	// publish a new binary sourced event
	testDeleteTopics() // remove any previous data in the system
	testCreateTopics()

	evs := testAddEventsBinarySourced(t, false)
	log.Printf("inserted %d events", len(evs))

	configureTopicsForQA02()

	testOpenClient()
	defer testCloseClient()

	// perform a reprocess
	log.Printf("start reprocessing")
	err := Start(prov, kvprov, true, true)
	require.Nil(t, err)

	for k := range provmem.Mem.Topics {
		for k2 := range provmem.Mem.Topics[k] {
			log.Printf("mem topic %s partition %d len %d", k, k2, len(provmem.Mem.Topics[k][k2]))
		}
	}

	require.Equal(t, len(provmem.Mem.Topics["azul.qa02.tasking.binary.sourced"][0]), 100)
	require.Equal(t, len(provmem.Mem.Topics["azul.qa02.testing.binary.sourced"][0]), 100)

	// fetch the published event from dispatcher
	data := getAllBinaryEvents(t)

	require.Nil(t, err)
	require.Equal(t, 200, len(data))

	// map published events for lookup
	mappedData := map[string]map[string]*events.BinaryEvent{}
	for _, ev := range data {
		_, ok := mappedData[ev.Source.Name]
		if !ok {
			mappedData[ev.Source.Name] = map[string]*events.BinaryEvent{}
		}
		mappedData[ev.Source.Name][ev.Entity.Sha256] = ev
	}

	// check published events against the originals
	for _, ev1 := range evs {
		ev2 := mappedData[ev1.Source.Name][ev1.Entity.Sha256]
		// generated in pipeline
		ev1.KafkaKey = ev2.KafkaKey
		testdata.MarshalEqual(t, ev1, ev2)
	}
}

// test that an event published to qa01 is correctly copied to qa02
func TestCopyEventSystemAvro(t *testing.T) {
	defer st.ResetSettings()
	st.Events.Reprocess.UpgradeSources = false
	st.Events.Reprocess.UpgradeSystem = true
	st.Settings.Events.IgnoreTopicMismatch = true
	st.Settings.Streams.Backend = "local"
	bedSet.Settings.LogLevel = "debug"
	testCreateProviders(t)

	// publish a new binary sourced event
	testDeleteTopics() // remove any previous data in the system
	testCreateTopics()

	event := testAddEventInsert(t, false)
	log.Printf("inserted binary sourced event")

	// add expedited event
	bc := &testdata.BC{Source: "tasking", ID: "uniq-e-1", Flags: &events.BinaryFlags{Expedite: true}}
	testAddEventBinarySourced(t, bc, false)

	// perform a reprocess
	configureTopicsForQA02()

	testOpenClient()
	defer testCloseClient()

	log.Printf("start reprocessing")
	err := Start(prov, kvprov, true, true)
	require.Nil(t, err)

	// fetch the published insert event from dispatcher
	data := getAllInsertEvents(t)

	require.Nil(t, err)
	require.Equal(t, 1, len(data))

	// check published event against the original
	event.KafkaKey = data[0].KafkaKey
	event.TrackAuthor = ".myauthor.1.2"
	event.TrackLink = "sha2561.sha2562.0001_01_01T00_00_00Z_00..myauthor.1.2"
	testMarshalEqual(t, event, &data[0])

	// check events were migrated
	require.Equal(t, len(provmem.Mem.Topics["azul.qa01.system.expedite"][0]), 1)
	require.Equal(t, len(provmem.Mem.Topics["azul.qa02.system.expedite"][0]), 1)
	require.Equal(t, len(provmem.Mem.Topics["azul.qa01.system.insert"][0]), 1)
	require.Equal(t, len(provmem.Mem.Topics["azul.qa02.system.insert"][0]), 1)
}
