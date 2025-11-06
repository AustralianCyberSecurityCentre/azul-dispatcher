//go:build integration

package tracking

import (
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisTracking(t *testing.T) {
	kvprov, err := kvprovider.NewRedisProviders()
	require.Nil(t, err)
	tracker, err := NewTaskTracker(kvprov.TrackPluginExecution, testdata.GetGlobalTestContext())
	require.Nil(t, err)

	entries, err := tracker.GetAllDequeuedIDs()
	require.Nil(t, err)
	for _, k := range entries {
		tracker.Prov.Del(testdata.GetGlobalTestContext(), k)
	}

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
	// Give deletion a chance to run.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		entries, err = tracker.GetAllDequeuedIDs()
		assert.Nil(collect, err)
		assert.Equal(collect, entries, []string{})
	}, 2*time.Second, 5*time.Millisecond)

}
