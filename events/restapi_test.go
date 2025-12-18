package events

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_consume"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func testDoParseParams(query string) (*consumer.ConsumeParams, error) {
	resp_recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://localhost", nil)
	c, _ := gin.CreateTestContext(resp_recorder)
	req.URL = &url.URL{
		RawQuery: query,
	}
	c.Request = req
	c.Params = []gin.Param{{Key: "model", Value: "binary"}}
	return parseParams(c)
}

func testFilters(t *testing.T, params *consumer.ConsumeParams, evs []*events.BinaryEvent) []int {
	pipe := pipeline_consume.FilterConsumerRules{}
	valid := []int{}
	for i, event := range evs {
		raw, err := json.Marshal(&event)
		require.Nil(t, err)
		msg, err := pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
		require.Nil(t, err, string(raw))
		_, msg2 := pipe.ConsumeMod(msg, params)
		if msg2 != nil {
			valid = append(valid, i)
		}
	}
	return valid
}

func TestParseParams(t *testing.T) {
	params, err := testDoParseParams("name=test&version=5&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
	})
	require.Equal(
		t,
		testFilters(t, params, []*events.BinaryEvent{
			testdata.GenEventBinary(&testdata.BC{}),
		}),
		[]int{0},
	)

	params, err = testDoParseParams("name=test&version=5&r-under-content-size=1020&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:                   "binary",
		Name:                    "test",
		Version:                 "5",
		RequireExpedite:         true,
		RequireLive:             true,
		Count:                   1,
		Deadline:                5,
		IsTask:                  false,
		RequireUnderContentSize: 1020,
	})
	evs := []*events.BinaryEvent{
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
	}
	evs[0].Action = events.ActionEnriched
	evs[0].Entity.Datastreams = []events.BinaryEntityDatastream{}
	evs[0].Source.Name = "mysource"
	evs[1].Entity.Datastreams[0].Size = 100
	evs[2].Entity.Datastreams[0].Size = 1000
	evs[3].Action = events.ActionExtracted
	evs[3].Entity.Datastreams[0].Size = 10000
	// case 4 has another stream within the size range
	evs[4].Entity.Datastreams[0].Size = 10000
	evs[4].Action = events.ActionAugmented
	evs[4].Entity.Datastreams = append(evs[4].Entity.Datastreams, testdata.GenBinaryStream("1"))
	evs[4].Entity.Datastreams[1].Label = "pcap"
	evs[4].Entity.Datastreams[1].Size = 100

	require.Equal(t, testFilters(t, params, evs), []int{1, 2})

	// Test max and min content size
	params, err = testDoParseParams("name=test&version=5&r-under-content-size=1020&r-over-content-size=1000&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:                   "binary",
		Name:                    "test",
		Version:                 "5",
		RequireExpedite:         true,
		RequireLive:             true,
		Count:                   1,
		Deadline:                5,
		IsTask:                  false,
		RequireUnderContentSize: 1020,
		RequireOverContentSize:  1000,
	})

	require.Equal(t, testFilters(t, params, evs), []int{2})

	// test min greater than max content size creates an error
	_, err = testDoParseParams("name=test&version=5&r-under-content-size=1020&r-over-content-size=2000")
	require.Error(t, err)

	// test min content size only
	params, err = testDoParseParams("name=test&version=5&r-over-content-size=2000&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:                  "binary",
		Name:                   "test",
		Version:                "5",
		RequireExpedite:        true,
		RequireLive:            true,
		Count:                  1,
		Deadline:               5,
		IsTask:                 false,
		RequireOverContentSize: 2000,
	})
	require.Equal(t, testFilters(t, params, evs), []int{3, 4})

	params, err = testDoParseParams("name=test&version=5&r-action=extracted&r-action=augmented&r-expedite=true&r-live=true")
	require.Nil(t, err)
	log.Printf("%v", params)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:            "binary",
		Name:             "test",
		Version:          "5",
		RequireExpedite:  true,
		RequireLive:      true,
		Count:            1,
		Deadline:         5,
		IsTask:           false,
		RequireEvents:    []events.BinaryAction{events.ActionExtracted, events.ActionAugmented},
		RequireEventsMap: map[events.BinaryAction]bool{events.ActionExtracted: true, events.ActionAugmented: true},
	})
	require.Equal(t, testFilters(t, params, evs), []int{3, 4})

	params, err = testDoParseParams("name=test&version=5&r-source=testing&r-historic=true&r-action=sourced")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:             "binary",
		Name:              "test",
		Version:           "5",
		RequireSources:    []string{"testing"},
		RequireHistoric:   true,
		Count:             1,
		Deadline:          5,
		IsTask:            false,
		RequireEvents:     []events.BinaryAction{events.ActionSourced},
		RequireEventsMap:  map[events.BinaryAction]bool{events.ActionSourced: true},
		RequireSourcesMap: map[string]bool{"testing": true},
	})
	require.Equal(t, testFilters(t, params, evs), []int{1, 2})

	params, err = testDoParseParams("name=test&version=5&d-action=mapped&d-action=enriched&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		DenyEvents:      []events.BinaryAction{events.ActionMapped, events.ActionEnriched},
		DenyEventsMap:   map[events.BinaryAction]bool{events.ActionMapped: true, events.ActionEnriched: true},
	})
	require.Equal(t, testFilters(t, params, evs), []int{1, 2, 3, 4})

	params, err = testDoParseParams("name=test&version=5&d-action=enriched&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		DenyEvents:      []events.BinaryAction{events.ActionEnriched},
		DenyEventsMap:   map[events.BinaryAction]bool{events.ActionEnriched: true},
	})

	params, err = testDoParseParams("name=test&version=5&r-content=true&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		RequireContent:  true,
		RequireEvents:   []events.BinaryAction{events.ActionSourced, events.ActionExtracted},
		RequireEventsMap: map[events.BinaryAction]bool{
			events.ActionSourced:   true,
			events.ActionExtracted: true,
		},
	})

	evs = []*events.BinaryEvent{
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
	}
	evs[0].Author.Name = "test"
	evs[1].Author.Name = "tester"
	evs[2].Author.Name = "intest"
	evs[3].Author.Name = "apple"
	evs[4].Author.Name = "test"
	params, err = testDoParseParams("name=test&version=5&d-self=true&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		DenySelf:        true,
	})
	require.Equal(
		t,
		testFilters(t, params, evs),
		[]int{1, 2, 3},
	)

	evs = []*events.BinaryEvent{
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
		testdata.GenEventBinary(&testdata.BC{}),
	}
	evs[0].Entity.Datastreams[0].FileFormat = "executable/windows/pe32"
	evs[1].Entity.Datastreams[0].FileFormat = "executable/windows/dll32"
	evs[2].Entity.Datastreams[0].FileFormat = "executable/windows/dll64"
	evs[4].Entity.Datastreams[0].Label = "content"
	evs[4].Entity.Datastreams[0].FileFormat = "text/plain"
	evs[4].Entity.Datastreams = append(evs[4].Entity.Datastreams, testdata.GenBinaryStream("1"))
	evs[4].Entity.Datastreams[1].Label = "safe_png"
	evs[4].Entity.Datastreams[0].FileFormat = "executable/windows/pe32"

	// data types
	params, err = testDoParseParams("name=test&version=5&r-streams=*,executable/windows/pe32,executable/windows/dll32&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		RequireStreams:  map[events.DatastreamLabel]map[string]bool{"*": {"executable/windows/pe32": true, "executable/windows/dll32": true}},
	})
	require.Equal(
		t,
		testFilters(t, params, evs),
		[]int{0, 1},
	)

	// one event with exe and network capture
	evs[4].Entity.Datastreams[0].Label = "content"
	evs[4].Entity.Datastreams[0].FileFormat = "executable/windows/pe32"
	evs[4].Entity.Datastreams = append(evs[4].Entity.Datastreams, testdata.GenBinaryStream("1"))
	evs[4].Entity.Datastreams[1].Label = "pcap"
	evs[4].Entity.Datastreams[1].FileFormat = "network/tcpdump"
	params, err = testDoParseParams("name=test&version=5&r-streams=content&r-streams=pcap,network/tcpdump&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		RequireStreams:  map[events.DatastreamLabel]map[string]bool{"content": {}, "pcap": {"network/tcpdump": true}},
	})
	require.Equal(
		t,
		testFilters(t, params, evs),
		[]int{4},
	)

	evs[0].Entity.Datastreams[0].FileFormat = "executable/windows/pe32"
	evs[1].Entity.Datastreams[0].FileFormat = "executable/windows/dll32"
	evs[2].Entity.Datastreams[0].FileFormat = "executable/windows/com"
	evs[3].Entity.Datastreams[0].FileFormat = "executable/windows/pe64"
	evs[4].Entity.Datastreams[0].FileFormat = "text/plain"

	// data types al
	params, err = testDoParseParams("name=test&version=5&r-streams=content,executable/windows/pe32,executable/windows/dll32&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		RequireStreams: map[events.DatastreamLabel]map[string]bool{
			"content": {"executable/windows/pe32": true, "executable/windows/dll32": true},
		},
	})
	require.Equal(
		t,
		testFilters(t, params, evs),
		[]int{0, 1},
	)

	// check partial matches
	params, err = testDoParseParams("name=test&version=5&r-streams=content,executable/&r-expedite=true&r-live=true")
	require.Nil(t, err)
	require.Equal(t, params, &consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        5,
		IsTask:          false,
		RequireStreams: map[events.DatastreamLabel]map[string]bool{
			"content": {"executable/": true},
		},
	})
	require.Equal(
		t,
		testFilters(t, params, evs),
		[]int{0, 1, 2, 3},
	)

}
