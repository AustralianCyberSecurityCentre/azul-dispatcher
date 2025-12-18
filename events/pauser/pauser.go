package pauser

import (
	"context"
	"sync"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/redis/go-redis/v9"
)

var nowFunc = time.Now
var timeSince = time.Since

// Key value for the pausePlugins key to be stored under.
const PAUSE_PLUGIN_FLAG_KEY = "pausePlugins"

// Number of minutes to wait until starting to process again.
const PAUSE_TIME_BEFORE_RESUME = time.Duration(10) * time.Minute

// How long to wait until setting PausePluginProcessing again.
const TIME_BETWEEN_REDIS_UPDATES = time.Duration(1) * time.Minute

const MAX_TIME_TO_SKIP_PLUGIN_TO_LATEST = time.Duration(30) * time.Minute

func PausePluginProcessing(ctx context.Context, kvmulti *kvprovider.KVMulti) error {
	nowTime := nowFunc()
	bedSet.Logger.Info().Msgf("Pausing plugin processing at time %s", nowTime.Format(time.RFC3339))
	return kvmulti.PausePluginProcessingStartTime.Set(ctx, PAUSE_PLUGIN_FLAG_KEY, nowTime, MAX_TIME_TO_SKIP_PLUGIN_TO_LATEST)
}

var pauseUpdateLock sync.Mutex

func BackgroundPauseProcessing(ctx context.Context, kvmulti *kvprovider.KVMulti) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, innerWg *sync.WaitGroup, kvmulti *kvprovider.KVMulti) {
		defer innerWg.Done()
		// Only allow an update every minute
		lastPauseTime, _ := WhenWasLastPause(ctx, kvmulti)
		if timeSince(lastPauseTime) > TIME_BETWEEN_REDIS_UPDATES {
			pauseUpdateLock.Lock()
			defer pauseUpdateLock.Unlock()

			lastPauseTime, _ = WhenWasLastPause(ctx, kvmulti)
			if timeSince(lastPauseTime) > TIME_BETWEEN_REDIS_UPDATES {
				err := PausePluginProcessing(ctx, kvmulti)
				if err != nil {
					bedSet.Logger.Err(err).Msg("Failed to pause plugin event handling.")
				}
			}
		}
	}(ctx, &wg, kvmulti)
	return &wg
}

func PauseUntilContextDone(ctx context.Context, kvmulti *kvprovider.KVMulti) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, innerWg *sync.WaitGroup, kvmulti *kvprovider.KVMulti) {
		defer innerWg.Done()
		minutelyTicker := time.NewTicker(TIME_BETWEEN_REDIS_UPDATES)
		err := PausePluginProcessing(ctx, kvmulti)
		if err != nil {
			bedSet.Logger.Err(err).Msg("Couldn't pause plugin processing during minutely routine.")
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-minutelyTicker.C:
				err = PausePluginProcessing(ctx, kvmulti)
				if err != nil {
					bedSet.Logger.Err(err).Msg("Couldn't pause plugin processing during minutely routine.")
				}
			}

		}
	}(ctx, &wg, kvmulti)
	return &wg

}

func IsPluginProcessingPaused(ctx context.Context, kvmulti *kvprovider.KVMulti) (bool, error) {
	val, err := WhenWasLastPause(ctx, kvmulti)
	if err != nil {
		return false, err
	}
	// Paused if the time since the last time seen is less than the maximum wait time.
	return timeSince(val) < PAUSE_TIME_BEFORE_RESUME, nil
}

func WhenWasLastPause(ctx context.Context, kvmulti *kvprovider.KVMulti) (time.Time, error) {
	val, err := kvmulti.PausePluginProcessingStartTime.GetTime(ctx, PAUSE_PLUGIN_FLAG_KEY)
	if err != nil {
		// Simply doesn't exist yet.
		if err == redis.Nil {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	return val, err
}

func ClearLastPauseTime(ctx context.Context, kvmulti *kvprovider.KVMulti) error {
	_, err := kvmulti.PausePluginProcessingStartTime.Del(ctx, PAUSE_PLUGIN_FLAG_KEY)
	return err
}
