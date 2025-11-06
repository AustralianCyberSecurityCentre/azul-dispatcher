package manager

import (
	"context"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/stretchr/testify/require"
)

func setupConsumerManager(t *testing.T) *ConsumerManager {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewMemoryProviders()
	require.Nil(t, err)
	emptyPipe := pipeline.NewConsumePipeline([]pipeline.ConsumeAction{}, nil)
	consumerManager := NewConsumerManager(prov, emptyPipe, emptyPipe, kvprov)
	return consumerManager
}

func TestGetLastPauseTimeAndDeletePluginConsumers(t *testing.T) {
	// Setup event readers that will eventually be deleted.
	cm := setupConsumerManager(t)
	reader, err := newEventReader(cm.prov, "a", &consumer.ConsumeParams{IsTask: true}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["a"] = reader

	reader, err = newEventReader(cm.prov, "b", &consumer.ConsumeParams{IsTask: false}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["b"] = reader

	ctx := context.Background()
	// No Pause time set
	err = pauser.ClearLastPauseTime(ctx, cm.kvstore)
	require.Nil(t, err)
	isPaused, lastPauseTime, err := cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: true})
	require.False(t, isPaused)
	require.Equal(t, time.Time{}, lastPauseTime)
	isPaused, lastPauseTime, err = cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: false})
	require.False(t, isPaused)
	require.Equal(t, time.Time{}, lastPauseTime)
	_, ok := cm.eventReaders["a"]
	require.True(t, ok)
	// Pause time set to roughly now
	err = pauser.PausePluginProcessing(ctx, cm.kvstore)
	require.Nil(t, err)
	isPaused, lastPauseTime, err = cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: true})
	require.True(t, isPaused)
	require.Equal(t, time.Time{}, lastPauseTime)
	isPaused, lastPauseTime, err = cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: false})
	require.False(t, isPaused)
	require.Equal(t, time.Time{}, lastPauseTime)
	// Plugin consumer should be deleted but not ingestor
	_, ok = cm.eventReaders["a"]
	require.False(t, ok)
	_, ok = cm.eventReaders["b"]
	require.True(t, ok)
	setLastPauseTime := time.Now().Add(-pauser.PAUSE_TIME_BEFORE_RESUME - time.Duration(1)*time.Minute)
	// pause time set to unpause but reboot plugin consumer time.
	cm.kvstore.PausePluginProcessingStartTime.Set(ctx, pauser.PAUSE_PLUGIN_FLAG_KEY, setLastPauseTime, 0)
	isPaused, lastPauseTime, err = cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: true})
	require.False(t, isPaused)
	require.Equal(t, setLastPauseTime.Format(time.RFC3339), lastPauseTime.Format(time.RFC3339))
	isPaused, lastPauseTime, err = cm.getLastPauseTimeAndDeletePluginConsumers(&consumer.ConsumeParams{IsTask: false})
	require.False(t, isPaused)
	require.Equal(t, time.Time{}, lastPauseTime)
}

func TestDeleteAllPluginEventReaders(t *testing.T) {
	cm := setupConsumerManager(t)
	reader, err := newEventReader(cm.prov, "a", &consumer.ConsumeParams{IsTask: true}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["a"] = reader
	reader, err = newEventReader(cm.prov, "c", &consumer.ConsumeParams{IsTask: true}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["c"] = reader

	reader, err = newEventReader(cm.prov, "b", &consumer.ConsumeParams{IsTask: false}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["b"] = reader
	reader, err = newEventReader(cm.prov, "d", &consumer.ConsumeParams{IsTask: false}, time.Now())
	require.Nil(t, err)
	cm.eventReaders["d"] = reader

	// Verify all plugins with istask set to true are deleted.
	cm.DeleteAllPluginEventReaders()
	_, ok := cm.eventReaders["b"]
	require.True(t, ok)
	_, ok = cm.eventReaders["d"]
	require.True(t, ok)

	_, ok = cm.eventReaders["a"]
	require.False(t, ok)
	_, ok = cm.eventReaders["c"]
	require.False(t, ok)
	require.True(t, true)

}
