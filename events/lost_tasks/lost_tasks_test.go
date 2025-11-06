package lost_tasks

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTime = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
var testNilTime = time.Time{}

func TestMain(m *testing.M) {
	// Needed for task tracker to immediately delete dequeuedIds
	st.Events.LostTasksBulkCreateLimit = 0
	// Global Context setup.
	cancelFunc := testdata.InitGlobalContext()
	defer cancelFunc()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestGenFailureEvent(t *testing.T) {
	var statusEvent *events.StatusEvent
	qprov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	ltp, err := NewLostTaskProcessor(qprov, kvprov, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	msg, err := ltp.genFailureEvent(&tracking.TaskStarted{
		Dequeued: "track_id",
		Time:     0,
		Event:    testdata.GetDequeuedBinaryEvent("1", "track_id"),
	})
	require.Nil(t, err)

	statusEvent, _ = msg.GetStatus()
	statusEvent.Timestamp = testTime
	require.Equal(t, statusEvent, &events.StatusEvent{
		Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
		ModelVersion: events.CurrentModelVersion,
		Timestamp:    testTime,
		Entity: events.StatusEntity{
			Status: "error-network",
			Error:  "lost task: track_id",
			Input: events.BinaryEvent{
				ModelVersion: events.CurrentModelVersion,
				Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
				Timestamp:    testNilTime,
				Action:       "enriched",
				Source:       events.EventSource{Name: "truck", References: map[string]string(nil), Security: "", Path: []events.EventSourcePathNode(nil), Timestamp: testNilTime},
				Entity: events.BinaryEntity{
					FileFormatLegacy: "ZIP",
					FileFormat:       "compression/zip",
					Sha256:           "1",
					Size:             5,
				},
				Retries:  0,
				Dequeued: "track_id",
			},
			Results: []events.BinaryEvent(nil),
			RunTime: 0},
	})

}

func TestHandleLost(t *testing.T) {
	var event events.StatusEvent
	qprov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tc, err := topics.NewTopicControl(qprov)
	require.Nil(t, err)
	tc.CreateAllTopics()

	ltp, err := NewLostTaskProcessor(qprov, kvprov, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	err = ltp.handleLost(&tracking.TaskStarted{
		Dequeued: "track_id",
		Time:     0,
		Event:    testdata.GetDequeuedBinaryEvent("1", "track_id"),
	})
	require.Nil(t, err)

	statusPartitions, ok := qprov.Mem.Topics["azul.test01.system.status"]
	require.True(t, ok)
	statuses := statusPartitions[0]
	require.Equal(t, len(statuses), 1)

	msg := statuses[0]
	err = event.FromAvro(msg)
	require.Nil(t, err)
	event.Timestamp = testTime
	event.KafkaKey = ""
	event.Entity.Input.Entity.Info = nil
	require.Equal(t, event, events.StatusEvent{
		KafkaKey:     "",
		Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
		ModelVersion: events.CurrentModelVersion,
		Timestamp:    testTime,
		Entity: events.StatusEntity{
			Status: "error-network",
			Error:  "lost task: track_id",
			Input: events.BinaryEvent{
				ModelVersion: events.CurrentModelVersion,
				Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
				Timestamp:    testNilTime,
				Action:       "enriched",
				Source:       events.EventSource{Name: "truck", References: map[string]string{}, Security: "", Path: []events.EventSourcePathNode{}, Timestamp: testNilTime, Settings: map[string]string{}},
				Entity: events.BinaryEntity{
					FileFormatLegacy: "ZIP",
					FileFormat:       "compression/zip",
					Sha256:           "1",
					Size:             5,
					Datastreams:      []events.BinaryEntityDatastream{},
					Features:         []events.BinaryEntityFeature{},
				},
				Retries:      0,
				Dequeued:     "track_id",
				TrackLinks:   []string{},
				TrackAuthors: []string{},
			},
			Results: []events.BinaryEvent{},
			RunTime: 0},
	})
}

func TestLTStart(t *testing.T) {
	var event events.StatusEvent
	// consistent generated ids
	qprov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	tc, err := topics.NewTopicControl(qprov)
	require.Nil(t, err)
	tc.CreateAllTopics()

	ltp, err := NewLostTaskProcessor(qprov, kvprov, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	tracker, err := tracking.NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	// dequeued first second after 1970 whatever
	err = tracker.DoProcessingStarted(testdata.GetDequeuedBinaryEvent("1", "abc.myplugin.my.version.1"), "my-plugin", "my.version")
	require.Nil(t, err)
	// dequeued but completed
	err = tracker.DoProcessingStarted(testdata.GetDequeuedBinaryEvent("1", "abc.myplugin.my.version2.1"), "my-plugin", "my.version2")
	require.Nil(t, err)
	tracker.DoProcessingFinished("abc.myplugin.my.version2.1")
	// Wait for the asynchronous delete to occur.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ids, err := tracker.GetAllDequeuedIDs()
		assert.Nil(collect, err)
		assert.Equal(collect, 1, len(ids))
	}, 2*time.Second, 5*time.Millisecond)

	// dequeued just now+100s, not a dropped task
	future := strconv.Itoa(int(time.Now().UTC().Unix() + 100))
	err = tracker.DoProcessingStarted(testdata.GetDequeuedBinaryEvent("1", "def.myplugin.my.version."+future), "my-plugin", "my.version")
	require.Nil(t, err)

	// process dropped job
	err = ltp.RunOnce()
	require.Nil(t, err)
	// read events, should be a error-network available
	statusPartitions, ok := qprov.Mem.Topics["azul.test01.system.status"]
	require.True(t, ok)
	require.Equal(t, len(statusPartitions), 1)
	statuses := statusPartitions[0]
	require.Equal(t, len(statuses), 1)

	msg := statuses[0]
	err = event.FromAvro(msg)
	require.Nil(t, err)
	event.Timestamp = testTime
	event.KafkaKey = ""
	event.Entity.Input.Entity.Info = nil
	require.Equal(t, event, events.StatusEvent{
		KafkaKey:     "",
		Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
		ModelVersion: events.CurrentModelVersion,
		Timestamp:    testTime,
		Entity: events.StatusEntity{
			Status: "error-network",
			Error:  "lost task: abc.myplugin.my.version.1",
			Input: events.BinaryEvent{
				ModelVersion: events.CurrentModelVersion,
				Author:       events.EventAuthor{Name: "FilePublisher", Version: "1.0", Category: "Loader", Security: ""},
				Timestamp:    testNilTime,
				Action:       "enriched",
				Source:       events.EventSource{Name: "truck", References: map[string]string{}, Security: "", Path: []events.EventSourcePathNode{}, Timestamp: testNilTime, Settings: map[string]string{}},
				Entity: events.BinaryEntity{
					FileFormatLegacy: "ZIP",
					FileFormat:       "compression/zip",
					Sha256:           "1",
					Size:             5,
					Datastreams:      []events.BinaryEntityDatastream{},
					Features:         []events.BinaryEntityFeature{},
				},
				Retries:      0,
				Dequeued:     "abc.myplugin.my.version.1",
				TrackLinks:   []string{},
				TrackAuthors: []string{},
			},
			Results: []events.BinaryEvent{},
			RunTime: 0,
		},
	})

	data, err := kvprov.TrackPluginExecution.GetBytes(ctx, "azul.dev.tracking.abc.my-plugin.my.version.1")
	require.Nil(t, err)
	require.Nil(t, data)
}
