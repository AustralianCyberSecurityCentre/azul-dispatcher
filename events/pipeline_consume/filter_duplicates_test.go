package pipeline_consume

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/dedupe"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

const fd_dir = "events/pipelines/consume/filter_dupe/"

var fd_basic_binary = testdata.GetEventBytes(fd_dir + "basic.json")
var fd_basic_binary2 = testdata.GetEventBytes(fd_dir + "basic2.json")
var fd_noid = testdata.GetEventBytes(fd_dir + "noid.json")
var fd_status = testdata.GetEventBytes(fd_dir + "status.json")

func TestFilterDuplicatesEntityKey(t *testing.T) {
	var binary events.BinaryEvent
	log.Printf("thing %v", string(fd_basic_binary))
	err := json.Unmarshal(fd_basic_binary, &binary)
	require.Nil(t, err)
	// require.Nil(t, err)
	key, err := filterDuplicatesKey(&binary, "myplugin", "1.0.0")
	require.Nil(t, err)
	require.Equal(t, string(key), `myplugin1.0.0ab3808f6d20b3c01513f89186becdcde`)

	binary = events.BinaryEvent{}
	err = json.Unmarshal(fd_basic_binary2, &binary)
	require.Nil(t, err)
	key, err = filterDuplicatesKey(&binary, "myplugin", "1.0.0")
	require.Nil(t, err)
	require.Equal(t, string(key), `myplugin1.0.0zb3808f6d20b3c01513f89186becdcde`)

	// has no id so should fail
	binary = events.BinaryEvent{}
	err = json.Unmarshal(fd_noid, &binary)
	require.Nil(t, err)
	_, err = filterDuplicatesKey(&binary, "myplugin", "1.0.0")
	require.NotNil(t, err)
}

func TestFilterDuplicates(t *testing.T) {
	// 1 mb cache
	dedupeCache := dedupe.New(uint64(1024 * 1024 * 1))
	d := FilterDuplicates{Seen: dedupeCache}

	p1 := &consumer.ConsumeParams{Name: "p1", Version: "1", IsTask: true}
	p2 := &consumer.ConsumeParams{Name: "p2", Version: "1", IsTask: true}

	inFlight, err := pipeline.NewMsgInFlightFromJson(fd_basic_binary, events.ModelBinary)
	inFlightExpedited, err := pipeline.NewMsgInFlightFromJson(fd_basic_binary, events.ModelBinary)
	binaryEvent, ok := inFlightExpedited.GetBinary()
	require.True(t, ok)
	binaryEvent.Flags.Expedite = true

	// Publish an expedited version of the event
	// (done to ensure expedite events don't block regular events.)
	require.Nil(t, err)
	warn, msg := d.ConsumeMod(inFlightExpedited, p1)
	require.Equal(t, warn, "")
	raw, err := msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	// Publish a non-expedited version of the event and it should work.
	require.Nil(t, err)
	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	// when seen again, should be skipped
	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "skipped")
	require.Nil(t, msg)

	// When expedited seen again should still go through again.
	// (done to ensure expedite events don't block regular events.)
	require.Nil(t, err)
	warn, msg = d.ConsumeMod(inFlightExpedited, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	// new plugin should still receive
	warn, msg = d.ConsumeMod(inFlight, p2)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	// when seen again, should be skipped
	warn, msg = d.ConsumeMod(inFlight, p2)
	require.Equal(t, warn, "skipped")
	require.Nil(t, msg)

	// new event
	inFlight, err = pipeline.NewMsgInFlightFromJson(fd_basic_binary2, events.ModelBinary)
	require.Nil(t, err)
	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	warn, msg = d.ConsumeMod(inFlight, p2)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 1000)

	// same status event multiple times
	inFlight, err = pipeline.NewMsgInFlightFromJson(fd_status, events.ModelStatus)
	require.Nil(t, err)
	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 3400)

	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 3400)

	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 3400)

	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 3400)

	warn, msg = d.ConsumeMod(inFlight, p1)
	require.Equal(t, warn, "")
	raw, err = msg.MarshalJSON()
	require.Nil(t, err)
	require.Greater(t, len(raw), 3400)
}
