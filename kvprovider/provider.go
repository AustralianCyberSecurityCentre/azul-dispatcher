package kvprovider

import (
	"context"
	"time"
)

// A KVInterface provides a fast key-value store. This is intended so we can write unit tests without connecting
// to redis.
type KVInterface interface {
	GetBytes(ctx context.Context, key string) ([]byte, error)
	GetTime(ctx context.Context, key string) (time.Time, error)
	Set(ctx context.Context, key string, value any, expiration time.Duration) error
	/*Delete any number of keys and return the number of elements that were deleted and any errors.*/
	Del(ctx context.Context, keys ...string) (int64, error)
	Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error)
	GetDBSize(ctx context.Context) int64
}

type KVMulti struct {
	// Holds plugin execution tracking information to determine when tasks are lost
	TrackPluginExecution KVInterface
	// Holds information about registered plugins and the events they are looking for
	RegisteredPlugins KVInterface
	// Holds relations between deployment keys/names and their plugins
	DeployedPlugins KVInterface
	//  Holds the value to indicate if Plugins should stop processing because a reporcess or restore is in progress.
	PausePluginProcessingStartTime KVInterface
}
