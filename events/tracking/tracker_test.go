package tracking

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Needed for task tracker to immediately delete dequeuedIds
	st.Events.LostTasksBulkCreateLimit = 0
	// Global Context setup.
	cancelFunc := testdata.InitGlobalContext()
	defer cancelFunc()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestTracking(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	tracker, err := NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	entries, err := tracker.GetAllDequeuedIDs()
	require.Nil(t, err)
	for _, k := range entries {
		tracker.Prov.Del(testdata.GetGlobalTestContext(), k)
	}

	val, err := tracker.ParseDequeuedIDTimestamp("md5sum.my-plugin.my.version.123754")
	require.Nil(t, err)
	require.Equal(t, val, int64(123754))

	bin := testdata.GenEventBinary(nil)
	bin.Dequeued = tracker.GenerateDequeuedID("md5sum", "p-1", "v.1")
	err = tracker.DoProcessingStarted(bin, "p-1", "v.1")
	require.Nil(t, err)
	entry, err := tracker.GetRedisTaskStarted(bin.Dequeued)
	require.Nil(t, err)
	require.Greater(t, len(entry), 1000)

	entries, err = tracker.GetAllDequeuedIDs()
	require.Nil(t, err)
	require.Contains(t, entries[0], "md5sum.p-1.v.1")

	tracker.DoProcessingFinished(bin.Dequeued)

	// Wait for deletion to finish in async function
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		entries, err := tracker.GetAllDequeuedIDs()
		assert.Nil(collect, err)
		assert.Equal(collect, entries, []string{})
	}, 2*time.Second, 5*time.Millisecond)

}

func TestTrackingBulkOperation(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	st.Events.LostTasksBulkCreateLimit = 10 // Delete only once there are 10 dequeuedIds
	tracker, err := NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	entries, err := tracker.GetAllDequeuedIDs()
	require.Nil(t, err)
	for _, k := range entries {
		tracker.Prov.Del(testdata.GetGlobalTestContext(), k)
	}

	val, err := tracker.ParseDequeuedIDTimestamp("md5sum.my-plugin.my.version.123754")
	require.Nil(t, err)
	require.Equal(t, val, int64(123754))

	// Start and finish task.
	bin := testdata.GenEventBinary(nil)
	bin.Dequeued = tracker.GenerateDequeuedID("md5sum", "p-0", "v.1")
	err = tracker.DoProcessingStarted(bin, "p-0", "v.1")
	require.Nil(t, err)
	tracker.DoProcessingFinished(bin.Dequeued)

	// Still one item waiting to be deleted because threshold of 10 hasn't been reached.
	entries, err = tracker.GetAllDequeuedIDs()
	require.Nil(t, err)
	require.Equal(t, 1, len(entries))

	for i := 1; i < 10; i++ {
		// Start and finish 9 other tasks.
		pluginName := fmt.Sprintf("p-%d", i)
		bin.Dequeued = tracker.GenerateDequeuedID("md5sum", pluginName, "v.1")
		err = tracker.DoProcessingStarted(bin, pluginName, "v.1")
		require.Nil(t, err)
		tracker.DoProcessingFinished(bin.Dequeued)
	}

	// Wait for deletion to finish in async function now there are 10 entries
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		entries, err = tracker.GetAllDequeuedIDs()
		assert.Nil(collect, err)
		assert.Equal(collect, 0, len(entries))
	}, 2*time.Second, 5*time.Millisecond)

}
