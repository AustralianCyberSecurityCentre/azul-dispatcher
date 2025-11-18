package pipeline_dual

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/goccy/go-json"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// statuses used to fill cache
var status_results_plug1_bin1 = testdata.GetEventBytes("events/metacache/status_results_plug1_bin1.json")
var status_results_plug1_bin2 = testdata.GetEventBytes("events/metacache/status_results_plug1_bin2.json")
var status_empty_plug1_bin3 = testdata.GetEventBytes("events/metacache/status_empty_plug1_bin3.json")

// binary events to trigger a cache lookup
var input_bin1 = testdata.GetEventBytes("events/metacache/binary_1.json")
var input_bin3 = testdata.GetEventBytes("events/metacache/binary_3.json")
var input_bin4 = testdata.GetEventBytes("events/metacache/binary_4.json")
var input_bin1_bypass = testdata.GetEventBytes("events/metacache/binary_1_bypass.json")

// results of replay from cache
var merged_plug1_bin1 = testdata.GetEventBytes("events/metacache/binary_1_replayed.json")
var merged_plug1_bin3 = testdata.GetEventBytes("events/metacache/binary_3_replayed.json")

var when = time.Date(2021, time.January, 10, 10, 0, 0, 0, time.UTC)

// MockStatusProducer is a mock of ProducerInterface.
type MockStatusProducer struct {
	ctl *gomock.Controller
}

func (m *MockStatusProducer) TransformMsgInFlights(msgInFlights []*msginflight.MsgInFlight, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *pipeline.ProduceActionInfo) {
	return nil, nil
}
func (p *MockStatusProducer) TransformEvents(message []byte, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, *pipeline.ProduceActionInfo, error) {
	resp := models.ResponsePostEvent{}
	return nil, &resp, nil, nil
}
func (p *MockStatusProducer) ProduceAnyEvents(confirm bool, to_publish []*msginflight.MsgInFlight) error {
	return nil
}

// TestMerge tests the ability to merge a new input event with cached results for the sample plugin.
func TestMetaCacheMerge(t *testing.T) {
	var err error
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000000

	tables := []struct {
		test     string
		event    []byte
		cached   []byte
		expected [][]byte
	}{
		{"merge_no_results", input_bin3, status_empty_plug1_bin3, [][]byte{}},
		// test bugfix where cached results without 'results' element would generate invalid json output
		{"merge_missing_results", input_bin3, bytes.Replace(status_empty_plug1_bin3, []byte(`,"results":[]`), []byte{}, 1), [][]byte{}},
		// test we actually replay correct results
		{"merge_results", input_bin1, status_results_plug1_bin1, [][]byte{merged_plug1_bin1}},
	}
	for _, table := range tables {
		var status events.StatusEvent
		err = json.Unmarshal(table.cached, &status)
		require.Nil(t, err, table.test)
		var binary events.BinaryEvent
		err = json.Unmarshal(table.event, &binary)
		require.Nil(t, err, table.test)
		m := formCacheCompletedEvents(&binary, &status, when)
		bedSet.Logger.Printf("checking %v", table.test)
		require.Equal(t, len(m), len(table.expected), table.test)
		for i := range table.expected {
			result := table.expected[i]
			got, err := json.Marshal(&m[i])
			require.Nil(t, err, table.test)
			require.JSONEq(t, string(result), string(got), table.test)
		}
	}
}

type testCaseRPC struct {
	test    string   // name of the test being run
	seed    [][]byte // seed value for the test
	event   []byte   // input event type
	plugin  string   // Name of the plugin
	version string   // Version of the plugin
	cached  bool     // Should the result hit the cache or not
}

func testRPCGeneric(t *testing.T, tables []testCaseRPC) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	p := MockStatusProducer{ctl}
	for _, table := range tables {
		c, _ := NewReplayPluginCompletion(&p, fstore)
		for _, s := range table.seed {
			msg, err := pipeline.NewMsgInFlightFromJson(s, events.ModelStatus)
			require.Nil(t, err, table.test)
			c.ProduceMod(msg, &pipeline.ProduceParams{UserAgent: "tester"})
		}

		inFlight, err := pipeline.NewMsgInFlightFromJson(table.event, events.ModelBinary)
		require.Nil(t, err)

		reason, msg := c.ConsumeMod(inFlight, &consumer.ConsumeParams{
			Name: table.plugin, Version: table.version, IsTask: true},
		)
		if table.cached {
			require.Equal(t, reason, "cached", "Test: %s Expected cached publish but received event", table.test)
			require.Nil(t, msg, "Test: %s Expected filtered event due to cache handling", table.test)
		} else {
			require.NotNil(t, msg, "Test: %s Expected non-cached event message", table.test)
		}
	}
}

// TestBigCache tests cache hits and misses using BigCache store.
func TestBigCache(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000000
	tables := []testCaseRPC{
		{"empty_cache", [][]byte{}, input_bin1, "MimeDecodeer", "2021.02.14", false},
		{"different_plugin", [][]byte{status_results_plug1_bin1}, input_bin1, "Random", "2021.02.14", false},
		{"different_version", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.15", false},
		{"different_binary", [][]byte{status_results_plug1_bin1}, input_bin4, "MimeDecoder", "2021.02.14", false},
		{"cache_bypass", [][]byte{status_results_plug1_bin1}, input_bin1_bypass, "MimeDecoder", "2021.02.14", false},
		{"cache_hit", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.14", true},
		{"cache_hit_multiple", [][]byte{status_results_plug1_bin1, status_results_plug1_bin2}, input_bin1, "MimeDecoder", "2021.02.14", true},
		{"cache_empty_results", [][]byte{status_empty_plug1_bin3}, input_bin3, "MimeDecoder", "2021.02.14", true},
		{"cache_same_source_results", [][]byte{merged_plug1_bin3}, input_bin3, "MimeDecoder", "2021.02.14", true},
	}
	testRPCGeneric(t, tables)
}

func TestBigCacheMaxSize(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000
	tables := []testCaseRPC{
		{"empty_cache", [][]byte{}, input_bin1, "MimeDecodeer", "2021.02.14", false},
		{"different_plugin", [][]byte{status_results_plug1_bin1}, input_bin1, "Random", "2021.02.14", false},
		{"different_version", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.15", false},
		{"different_binary", [][]byte{status_results_plug1_bin1}, input_bin4, "MimeDecoder", "2021.02.14", false},
		{"cache_bypass", [][]byte{status_results_plug1_bin1}, input_bin1_bypass, "MimeDecoder", "2021.02.14", false},
		{"cache_hit", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.14", false},
		{"cache_hit_multiple", [][]byte{status_results_plug1_bin1, status_results_plug1_bin2}, input_bin1, "MimeDecoder", "2021.02.14", false},
		{"cache_empty_results", [][]byte{status_empty_plug1_bin3}, input_bin3, "MimeDecoder", "2021.02.14", false},
		{"cache_same_source_results", [][]byte{merged_plug1_bin3}, input_bin3, "MimeDecoder", "2021.02.14", false},
	}
	testRPCGeneric(t, tables)
}

// TestNoCache tests that the cache is bypassed if no cache is configured.
func TestNoCache(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 0

	tables := []struct {
		test    string
		seed    [][]byte
		event   []byte
		plugin  string
		version string
		cached  bool
	}{
		// {"empty_cache", [][]byte{[]byte("{}")}, input_bin1, "MimeDecodeer", "2021.02.14", false},
		{"different_plugin", [][]byte{status_results_plug1_bin1}, input_bin1, "Random", "2021.02.14", false},
		{"different_version", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.15", false},
		{"different_binary", [][]byte{status_results_plug1_bin1}, input_bin4, "MimeDecoder", "2021.02.14", false},
		{"cache_bypass", [][]byte{status_results_plug1_bin1}, input_bin1_bypass, "MimeDecoder", "2021.02.14", false},
		{"cache_hit_skipped", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.14", false},
		{"cache_hit_multiple_skipped", [][]byte{status_results_plug1_bin1, status_results_plug1_bin2}, input_bin1, "MimeDecoder", "2021.02.14", false},
		{"cache_empty_results_skipped", [][]byte{status_empty_plug1_bin3}, input_bin3, "MimeDecoder", "2021.02.14", false},
	}
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	p := MockStatusProducer{ctl}

	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	for _, table := range tables {
		c, _ := NewReplayPluginCompletion(&p, fstore)
		for _, s := range table.seed {
			var ev events.StatusEvent
			err := json.Unmarshal(s, &ev)
			require.Nil(t, err)
			err = c.put(&ev)
			require.Nil(t, err, table.test)
		}

		inFlight, err := pipeline.NewMsgInFlightFromJson(table.event, events.ModelBinary)
		require.Nil(t, err)

		reason, msg := c.ConsumeMod(inFlight, &consumer.ConsumeParams{
			Name: table.plugin, Version: table.version, IsTask: true},
		)
		if table.cached {
			require.Equal(t, reason, "cached", "Test: %s Expected cached publish but received event", table.test)
			require.Nil(t, msg, "Test: %s Expected filtered event due to cache handling", table.test)
		} else {
			require.Equal(t, reason, "", "Test: %s Expected no reason '%s'", table.test, reason)
			require.NotNil(t, msg, "Test: %s Expected non-cached event message", table.test)
		}
	}
}

// TestKey tests the cache key id generation for given events/consumers.
func TestMetaCacheKey(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000000

	tables := []struct {
		test     string
		event    string
		plugin   string
		version  string
		expected string
	}{
		{"full_event_and_plugin_info", "abcdef", "foobar", "2021.01.01", "abcdef-foobar-2021.01.01"},
		{"plugin_no_version", "abcdef", "foobar", "", "abcdef-foobar-"},
	}
	for _, table := range tables {
		out := key(table.event, table.plugin, table.version)
		require.Equal(t, out, table.expected, "Test: %s Expected: %s Got: %s", table.test, table.expected, out)
	}
}

func TestCopyCachedResults(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000000

	table := struct {
		test    string   // name of the test being run
		seed    [][]byte // seed value for the test
		event   []byte   // input event type
		plugin  string   // Name of the plugin
		version string   // Version of the plugin
		cached  bool     // Should the result hit the cache or not
	}{"cache_hit", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.14", true}

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	// set up a store for testing
	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	// write some data that corresponds to status_results_plug1_bin1 cached event
	test_data := []byte("Test data content")
	test_hash := "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69"
	test_source := "virustotal"
	test_label := events.DataLabelContent

	// Create a temporary file
	f, err := os.CreateTemp("", "replay-plugin-complete-test")
	if err != nil {
		t.Fatalf("Failed to create tempfile %s", err.Error())
	}
	defer f.Close()
	defer os.Remove(f.Name())
	_, err = f.Write(test_data)
	require.Nil(t, err)

	_, err = f.Seek(0, 0)
	require.Nil(t, err, "Error when seeking back to 0 during copy.")
	err = fstore.Put(test_source, test_label.Str(), test_hash, f, int64(len(test_data)))
	require.Nil(t, err, "Error writing to file store")

	// check that the file has been written
	exists, err := fstore.Exists(test_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.True(t, exists)

	p := MockStatusProducer{ctl}
	// run the test
	c, _ := NewReplayPluginCompletion(&p, fstore)
	for _, s := range table.seed {
		var ev events.StatusEvent
		err := json.Unmarshal(s, &ev)
		require.Nil(t, err)
		err = c.put(&ev)
		require.Nil(t, err, table.test)
	}

	inFlight, err := pipeline.NewMsgInFlightFromJson(table.event, events.ModelBinary)
	require.Nil(t, err)

	reason, msg := c.ConsumeMod(inFlight, &consumer.ConsumeParams{
		Name: table.plugin, Version: table.version, IsTask: true},
	)
	if table.cached {
		require.Equal(t, reason, "cached", "Test: %s Expected cached publish but received event", table.test)
		require.Nil(t, msg, "Test: %s Expected filtered event due to cache handling", table.test)
	} else {
		require.NotNil(t, msg, "Test: %s Expected non-cached event message", table.test)
	}

	// check that it is in the new source
	expected_source := "testing"
	exists, err = fstore.Exists(expected_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.True(t, exists)

	// clean up test files
	deleted, err := fstore.Delete(test_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.True(t, deleted)
	deleted, err = fstore.Delete(expected_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.True(t, deleted)
}

func TestCopyCachedResultsNoFile(t *testing.T) {
	st.Events.ReplayPluginCache.TTLSeconds = 600
	st.Events.ReplayPluginCache.SizeBytes = 10000000

	// Test case when the source file does not exist
	// we expect no error to occur in this event
	table := struct {
		test    string   // name of the test being run
		seed    [][]byte // seed value for the test
		event   []byte   // input event type
		plugin  string   // Name of the plugin
		version string   // Version of the plugin
		cached  bool     // Should the result hit the cache or not
	}{"cache_hit", [][]byte{status_results_plug1_bin1}, input_bin1, "MimeDecoder", "2021.02.14", true}

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	// set up a store for testing
	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	// setup parameters but do not write file to file store. data corresponds to status_results_plug1_bin1 cached event
	test_hash := "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69"
	//test_source := "virustotal"
	test_label := events.DataLabelContent

	p := MockStatusProducer{ctl}
	// run the test
	c, _ := NewReplayPluginCompletion(&p, fstore)
	for _, s := range table.seed {
		var ev events.StatusEvent
		err := json.Unmarshal(s, &ev)
		require.Nil(t, err)
		err = c.put(&ev)
		require.Nil(t, err, table.test)
	}

	inFlight, err := pipeline.NewMsgInFlightFromJson(table.event, events.ModelBinary)
	require.Nil(t, err)

	reason, msg := c.ConsumeMod(inFlight, &consumer.ConsumeParams{
		Name: table.plugin, Version: table.version, IsTask: true},
	)

	if table.cached {
		require.Equal(t, reason, "cached", "Test: %s Expected cached publish but received event", table.test)
		require.Nil(t, msg, "Test: %s Expected filtered event due to cache handling", table.test)
	} else {
		require.NotNil(t, msg, "Test: %s Expected non-cached event message", table.test)
	}

	// check that the file is not in the new source, as the source file did not exist.
	expected_source := "testing"
	exists, err := fstore.Exists(expected_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.False(t, exists)
}
