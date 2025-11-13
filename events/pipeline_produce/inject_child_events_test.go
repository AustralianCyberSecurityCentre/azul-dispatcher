package pipeline_produce

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var extracted = testdata.GetEvent("events/pipelines/injector/extracted.json")
var enriched = testdata.GetEvent("events/pipelines/injector/enriched.json")
var mapped = testdata.GetEvent("events/pipelines/injector/mapped.json")
var downloaded = testdata.GetEvent("events/pipelines/injector/downloaded.json")
var extracted_whitespace = testdata.GetEvent("events/pipelines/injector/extracted_whitespace.json")
var download_child = testdata.GetEvent("events/pipelines/injector/download_child.json")

var download_attached = testdata.GetEvent("events/pipelines/injector/download_attached.json")
var download_child2 = testdata.GetEvent("events/pipelines/injector/download_child2.json")
var download_attached2 = testdata.GetEvent("events/pipelines/injector/download_attached2.json")
var insert_event = testdata.GetEvent("events/pipelines/injector/insert_event.json")
var insert_event2 = testdata.GetEvent("events/pipelines/injector/insert_event2.json")
var insert_event_overwrite = testdata.GetEvent("events/pipelines/injector/insert_event_overwrite.json")

var produceParams = pipeline.ProduceParams{UserAgent: "test", AvroFormat: false}

func TestInjectable(t *testing.T) {
	var err error
	tables := []struct {
		test     string
		input    []byte
		expected bool
	}{
		// wrong event/history
		{"enriched", []byte(enriched), false},
		{"downloaded", []byte(downloaded), false},
		{"mapped", []byte(mapped), false},
		// right event/history to possibly attach children to
		{"extracted", []byte(extracted), true},
		{"extracted_whitespace", []byte(extracted_whitespace), true},
	}
	for _, table := range tables {
		var ev events.BinaryEvent
		err = json.Unmarshal(table.input, &ev)
		require.Nil(t, err)
		res := injectable(&ev)
		require.Equal(t, table.expected, res, table.test)
	}
}

func TestMerge(t *testing.T) {
	var err error
	tables := []struct {
		test     string
		parent   []byte
		insert   []byte
		expected []byte
	}{
		{"downloaded", []byte(downloaded), []byte(download_child), []byte(download_attached)},
	}
	for _, table := range tables {
		var parent events.BinaryEvent
		var insert events.InsertEvent
		err = json.Unmarshal(table.parent, &parent)
		require.Nil(t, err, table.test)
		err = json.Unmarshal(table.insert, &insert)
		require.Nil(t, err, table.test, string(table.insert))
		out, err := merge(&parent, &insert)
		require.Nil(t, err, table.test)
		res, err := json.Marshal(out)
		require.Nil(t, err, table.test)

		// strip non-deterministic fields
		res, _ = sjson.SetBytes(res, "timestamp", "2021-03-29T14:23:50+11:00")
		last := gjson.GetBytes(res, "source.path.#").Int() - 1
		res, _ = sjson.SetBytes(res, fmt.Sprintf("source.path.%d.timestamp", last), "2021-03-29T14:23:50+11:00")
		tmpExpected, err := pipeline.NewMsgInFlightFromJson(table.expected, events.ModelBinary)
		require.Nil(t, err, table.test)
		expected, err := json.Marshal(tmpExpected)
		require.Nil(t, err, table.test)
		require.JSONEq(t, string(expected), string(res), table.test)
	}
}

func TestMergeSecurityString(t *testing.T) {
	var err error
	tables := []struct {
		test     string
		parent   []byte
		insert   []byte
		expected []byte
	}{
		{"downloaded", []byte(downloaded), []byte(download_child2), []byte(download_attached2)},
	}
	for _, table := range tables {
		var parent events.BinaryEvent
		var insert events.InsertEvent
		err = json.Unmarshal(table.parent, &parent)
		require.Nil(t, err, table.test)
		err = json.Unmarshal(table.insert, &insert)
		require.Nil(t, err, table.test)
		out, err := merge(&parent, &insert)
		require.Nil(t, err, table.test)
		res, err := json.Marshal(out)
		require.Nil(t, err, table.test)
		// strip non-deterministic fields
		res, _ = sjson.SetBytes(res, "timestamp", "2021-03-29T14:23:50+11:00")
		last := gjson.GetBytes(res, "source.path.#").Int() - 1
		res, _ = sjson.SetBytes(res, fmt.Sprintf("source.path.%d.timestamp", last), "2021-03-29T14:23:50+11:00")
		tmpExpected, err := pipeline.NewMsgInFlightFromJson(table.expected, events.ModelBinary)
		require.Nil(t, err, table.test)
		expected, err := json.Marshal(tmpExpected)
		require.Nil(t, err, table.test)
		require.JSONEq(t, string(expected), string(res), table.test)
	}
}

func TestExpand(t *testing.T) {
	var err error
	tables := []struct {
		test     string
		parent   []byte
		insert   [][]byte
		expected int // can expand to full content but already tested in merge
	}{
		{"noinsert", []byte(extracted), [][]byte{}, 0},
		{"noninjectable", []byte(downloaded), [][]byte{[]byte(insert_event)}, 0},
		{"extracted", []byte(extracted), [][]byte{[]byte(insert_event)}, 1},
		{"multiple", []byte(extracted), [][]byte{[]byte(insert_event), []byte(insert_event2)}, 2},
		{"duplicate", []byte(extracted), [][]byte{[]byte(insert_event), []byte(insert_event)}, 1},
		{"overwrite", []byte(extracted), [][]byte{[]byte(insert_event), []byte(insert_event_overwrite)}, 1},
	}
	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	for _, table := range tables {
		// create directly without New as don't want to set up/mock kafka
		inj := &InjectChildEvents{
			consumer: nil,
			run:      true,
			store:    fstore,
			inserts:  map[string][]*events.InsertEvent{},
		}
		for _, i := range table.insert {
			inFlight, err := pipeline.NewMsgInFlightFromJson(i, events.ModelInsert)
			require.Nil(t, err)
			insert, _ := inFlight.GetInsert()
			err = inj.registerInsert("", insert)
			require.Nil(t, err, table.test)
		}
		msg, err := pipeline.NewMsgInFlightFromJson(table.parent, events.ModelBinary)
		require.Nil(t, err, table.test)
		_, res := inj.ProduceMod(msg, &produceParams)
		require.Equal(t, table.expected, len(res), table.test)
	}
}

func TestChildBinaryCopy(t *testing.T) {
	tables := []struct {
		test     string
		parent   []byte
		insert   [][]byte
		expected int // can expand to full content but already tested in merge
	}{
		{"extracted", []byte(extracted), [][]byte{[]byte(insert_event)}, 1},
	}
	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	// write some child binary that corresponds to selected event
	test_data := []byte("Test data content")
	test_hash := "b123456789"
	test_source := "azul"
	test_label := events.DataLabelContent

	// Create a temporary file
	f, err := os.CreateTemp("", "store-cache-test")
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
	require.Nil(t, err, "Error reading from file store")
	require.True(t, exists, "Expected file in file store but it is not found")

	for _, table := range tables {
		// create directly without New as don't want to set up/mock kafka
		inj := &InjectChildEvents{
			consumer: nil,
			run:      true,
			store:    fstore,
			inserts:  map[string][]*events.InsertEvent{},
		}
		for _, i := range table.insert {
			inFlight, err := pipeline.NewMsgInFlightFromJson(i, events.ModelInsert)
			require.Nil(t, err)
			insert, _ := inFlight.GetInsert()
			log.Printf("src %v", insert.Entity.OriginalSource)
			err = inj.registerInsert("", insert)
			require.Nil(t, err, table.test)
		}
		msg, err := pipeline.NewMsgInFlightFromJson(table.parent, events.ModelBinary)
		require.Nil(t, err, table.test)
		_, res := inj.ProduceMod(msg, &produceParams)
		require.Equal(t, len(res), table.expected, table.test)
	}

	// check that the file is found in the parent's source
	expected_source := "virustotal"
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

func TestChildBinaryCopyNoSource(t *testing.T) {
	// Test case when the source file does not exist
	// we expect no error to occur in this event
	tables := []struct {
		test     string
		parent   []byte
		insert   [][]byte
		expected int // can expand to full content but already tested in merge
	}{
		{"extracted", []byte(extracted), [][]byte{[]byte(insert_event)}, 1},
	}
	fstore, err := store.NewEmptyLocalStore(st.Streams.Local.Path)
	if err != nil {
		panic(err.Error())
	}
	require.Nil(t, err, "Error creating NewEmptyLocalStore")

	// write some child binary that corresponds to selected event
	test_hash := "b123456789"
	test_source := "azul"
	test_label := events.DataLabelContent
	// Do not write the file
	//err = fstore.Put(test_source, test_label, test_hash, test_data)
	//require.Nil(t, err, "Error writing to file store")

	// check that the file is not found
	exists, err := fstore.Exists(test_source, test_label.Str(), test_hash)
	require.Nil(t, err, "Error reading from file store")
	require.False(t, exists, "Expected no file in file store but it exists")

	for _, table := range tables {
		// create directly without New as don't want to set up/mock kafka
		inj := &InjectChildEvents{
			consumer: nil,
			run:      true,
			store:    fstore,
			inserts:  map[string][]*events.InsertEvent{},
		}
		for _, i := range table.insert {
			inFlight, err := pipeline.NewMsgInFlightFromJson(i, events.ModelInsert)
			require.Nil(t, err)
			insert, _ := inFlight.GetInsert()
			err = inj.registerInsert("", insert)
			require.Nil(t, err, table.test)
		}
		msg, err := pipeline.NewMsgInFlightFromJson(table.parent, events.ModelBinary)
		require.Nil(t, err)
		_, res := inj.ProduceMod(msg, &produceParams)
		require.Equal(t, len(res), table.expected)
	}

	// check that the file is not found in the parent's source (as no source file existed)
	expected_source := "virustotal"
	exists, err = fstore.Exists(expected_source, test_label.Str(), test_hash)
	require.Nil(t, err)
	require.False(t, exists)
}

func TestRemove(t *testing.T) {
	tables := []struct {
		test     string
		insert   [][]byte
		key      string
		expected int
	}{
		{"empty", [][]byte{}, "1", 0},
		{"single", [][]byte{[]byte(insert_event)}, "1", 0},
		{"unmatched", [][]byte{[]byte(insert_event)}, "2", 1},
		{"multiple", [][]byte{[]byte(insert_event), []byte(insert_event2)}, "1", 1},
	}
	for _, table := range tables {
		inj := &InjectChildEvents{
			consumer: nil,
			run:      true,
			inserts:  map[string][]*events.InsertEvent{},
		}
		for _, i := range table.insert {
			inFlight, err := pipeline.NewMsgInFlightFromJson(i, events.ModelInsert)
			require.Nil(t, err)
			insert, _ := inFlight.GetInsert()
			err = inj.registerInsert("", insert)
			require.Nil(t, err, table.test)
		}
		inj.remove(table.key)
		n := 0
		for _, i := range inj.inserts {
			n += len(i)
		}
		require.Equal(t, n, table.expected)
	}
}
