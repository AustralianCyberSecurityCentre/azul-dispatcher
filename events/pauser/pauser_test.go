package pauser

import (
	"context"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/stretchr/testify/require"
)

func setNowTo(t *testing.T, val string) time.Time {
	testTime, err := time.Parse(time.RFC3339, val)
	require.Nil(t, err)
	nowFunc = func() time.Time { return testTime }
	timeSince = nowFunc().Sub
	return testTime
}

func TestGettingPauserValueBeforeSet(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	testGettingPauserValueBeforeSet(t, kvprov)
}

func testGettingPauserValueBeforeSet(t *testing.T, kvprov *kvprovider.KVMulti) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Get last pause time before it's been set.
	lastPauseTime, err := WhenWasLastPause(ctx, kvprov)
	require.Nil(t, err)
	// Verify the acquired time is older than the start time and younger than the endPause time.
	require.Equal(t, time.Time{}, lastPauseTime)

	// Plugin processing should be paused.
	val, err := IsPluginProcessingPaused(ctx, kvprov)
	require.False(t, val)
}

func TestBasicPauser(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	testBasicPauser(t, kvprov)
}

func testBasicPauser(t *testing.T, kvprov *kvprovider.KVMulti) {
	// override time.Now() for this test.
	startTestTime := setNowTo(t, "2024-06-06T16:25:05Z")

	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	err = PausePluginProcessing(ctx, kvprov)
	require.Nil(t, err)

	lastPauseTime, err := WhenWasLastPause(ctx, kvprov)
	// Verify the acquired time is older than the start time and younger than the endPause time.
	require.Equal(t, startTestTime, lastPauseTime)

	// Plugin processing should be paused.
	val, err := IsPluginProcessingPaused(ctx, kvprov)
	require.True(t, val)

	// Verify pause stops after precisely PAUSE_TIME_BEFORE_RESUME
	currentTestTime := startTestTime.Add(PAUSE_TIME_BEFORE_RESUME)
	currentTestTime = setNowTo(t, currentTestTime.Format(time.RFC3339))
	val, err = IsPluginProcessingPaused(ctx, kvprov)
	require.Nil(t, err)
	require.False(t, val)

	// Verify it is still paused 1 second before.
	currentTestTime = startTestTime.Add(PAUSE_TIME_BEFORE_RESUME - time.Duration(1)*time.Second)
	currentTestTime = setNowTo(t, currentTestTime.Format(time.RFC3339))
	val, err = IsPluginProcessingPaused(ctx, kvprov)
	require.Nil(t, err)
	require.True(t, val)
}

func TestBackgroundPauseProcessingWithLock(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	testBackgroundPauseProcessingWithLock(t, kvprov)
}

func testBackgroundPauseProcessingWithLock(t *testing.T, kvprov *kvprovider.KVMulti) {
	// override time.Now() for this test.
	startTestTime := setNowTo(t, "2024-06-06T16:25:05Z")

	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Wait until background processing has been paused.
	wg := BackgroundPauseProcessing(ctx, kvprov)
	wg.Wait()

	lastPauseTime, err := WhenWasLastPause(ctx, kvprov)
	require.Nil(t, err)
	require.Equal(t, startTestTime, lastPauseTime)

	// Try setting the time to one second ahead and it should be ignored.
	nextTime := startTestTime.Add(time.Duration(5) * time.Second)
	nextTime = setNowTo(t, nextTime.Format(time.RFC3339))
	wg = BackgroundPauseProcessing(ctx, kvprov)
	wg.Wait()

	// Time shouldn't have changed.
	lastPauseTime, err = WhenWasLastPause(ctx, kvprov)
	require.Nil(t, err)
	require.Equal(t, startTestTime, lastPauseTime)
}

func TestMinutelyBackgroundPauser(t *testing.T) {
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	testMinutelyBackgroundPauser(t, kvprov)
}

func testMinutelyBackgroundPauser(t *testing.T, kvprov *kvprovider.KVMulti) {
	// override time.Now() for this test.
	startTestTime := setNowTo(t, "2024-06-06T16:25:05Z")

	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Wait until background processing has been paused.
	wg := PauseUntilContextDone(ctx, kvprov)
	cancelFunc()
	wg.Wait()

	lastPauseTime, err := WhenWasLastPause(ctx, kvprov)
	require.Nil(t, err)
	require.Equal(t, startTestTime, lastPauseTime)
}
