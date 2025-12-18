package events

import (
	"testing"
	"time"

	stdjson "encoding/json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	goccyjson "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var rawEntityLarge = testdata.GetEventBytes("events/benchmark/entityLarge.json")

var rawEntitySmall = testdata.GetEventBytes("events/benchmark/entitySmall.json")

var testStatusEvent = testdata.GetEventBytes("events/pipelines/produce_status_results/example1.json")

func BenchmarkEventLargeGoccy(t *testing.B) {
	var tmp events.BinaryEvent
	for n := 0; n < t.N; n++ {
		err := goccyjson.Unmarshal(rawEntityLarge, &tmp)
		if err != nil {
			panic(err)
		}
		data, err := goccyjson.Marshal(tmp)
		if err != nil {
			panic(err)
		}
		require.Equal(t, len(data), 4603)
	}
}
func BenchmarkEventLargeStdlib(t *testing.B) {
	var tmp events.BinaryEvent
	for n := 0; n < t.N; n++ {
		err := stdjson.Unmarshal(rawEntityLarge, &tmp)
		if err != nil {
			panic(err)
		}
		data, err := stdjson.Marshal(tmp)
		if err != nil {
			panic(err)
		}
		require.Equal(t, len(data), 4603)
	}
}

func BenchmarkEventSmallGoccy(t *testing.B) {
	var tmp events.BinaryEvent
	for n := 0; n < t.N; n++ {
		err := goccyjson.Unmarshal(rawEntitySmall, &tmp)
		if err != nil {
			panic(err)
		}
		data, err := goccyjson.Marshal(tmp)
		if err != nil {
			panic(err)
		}
		require.Equal(t, len(data), 1381)
	}
}
func BenchmarkEventSmallStdlib(t *testing.B) {
	var tmp events.BinaryEvent
	for n := 0; n < t.N; n++ {
		err := stdjson.Unmarshal(rawEntitySmall, &tmp)
		if err != nil {
			panic(err)
		}
		data, err := stdjson.Marshal(tmp)
		if err != nil {
			panic(err)
		}
		require.Equal(t, len(data), 1381)
	}
}

func BenchmarkEventsStatusGJSON(t *testing.B) {
	for n := 0; n < t.N; n++ {
		// read several properties
		parsed := gjson.ParseBytes(testStatusEvent)
		results := parsed.Get("entity.results").Raw
		withoutResults, err := sjson.DeleteBytes(testStatusEvent, "entity.results")
		require.Nil(t, err)
		require.Greater(t, len(withoutResults), 100)
		require.Greater(t, len(results), 100)

		require.Equal(t, parsed.Get("author.name").String(), "MimeDecoder")
		require.Equal(t, parsed.Get("source.name").String(), "testing")
		require.Equal(t, parsed.Get("event").String(), "status_update")
		require.Equal(t, parsed.Get("entity.status").String(), "completed")

		require.Equal(t, parsed.Get("entity.results.0.source.name").String(), "testing")
		require.Equal(t, parsed.Get("entity.results.0.source.security").String(), "")
		require.Equal(t, parsed.Get("entity.results.0.source.timestamp").String(), "2018-02-06T16:43:23Z")
		require.JSONEq(t, parsed.Get("entity.results.0.source.references").String(), `{"foo": "bar", "user": "bob"}`)
		require.Equal(t, parsed.Get("entity.results.0.author.name").String(), "MimeDecoder")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")

		// repeat
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
		require.Equal(t, parsed.Get("entity.results.0.author.version").String(), "2021.02.14")
	}
}

func BenchmarkEventsStatusStruct(t *testing.B) {
	var err error
	var event events.StatusEvent
	var status events.StatusEntity
	for n := 0; n < t.N; n++ {
		// read several properties
		err = goccyjson.Unmarshal(testStatusEvent, &event)
		require.Nil(t, err)

		decodedResults := status.Results
		results, err := goccyjson.Marshal(status.Results)
		require.Nil(t, err)
		status.Results = nil
		event.Entity = status
		withoutResults, err := goccyjson.Marshal(&event)

		require.Nil(t, err)
		require.Greater(t, len(withoutResults), 100)
		require.Greater(t, len(results), 100)

		require.Equal(t, event.Author.Name, "MimeDecoder")
		require.Equal(t, event.Entity.Input.Source.Name, "testing")
		require.Equal(t, status.Status, "completed")

		require.Equal(t, decodedResults[0].Source.Name, "testing")
		require.Equal(t, decodedResults[0].Source.Security, "")
		expectedTime, err := time.Parse(time.RFC3339, "2018-02-06T16:43:23Z")
		require.Nil(t, err)
		require.Equal(t, decodedResults[0].Source.Timestamp, expectedTime)
		require.Equal(t, decodedResults[0].Source.References, map[string]string{"foo": "bar", "user": "bob"})
		require.Equal(t, decodedResults[0].Author.Name, "MimeDecoder")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")

		// repeat
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")
		require.Equal(t, decodedResults[0].Author.Version, "2021.02.14")

	}
}
