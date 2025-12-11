package kvprovider

import (
	"context"
	"errors"
	"time"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"

	"github.com/redis/go-redis/v9"
)

/*Initialise all redis providers.*/
func NewRedisProviders() (*KVMulti, error) {
	var err error
	ret := KVMulti{}
	// be very careful about changing the db number
	ret.TrackPluginExecution, err = newRedisProvider(0)
	if err != nil {
		return nil, err
	}
	// be very careful about changing the db number
	ret.RegisteredPlugins, err = newRedisProvider(1)
	if err != nil {
		return nil, err
	}
	// be very careful about changing the db number
	ret.DeployedPlugins, err = newRedisProvider(2)
	if err != nil {
		return nil, err
	}
	// be very careful about changing the db number
	ret.PausePluginProcessingStartTime, err = newRedisProvider(3)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// try to impose some sanity into redis usage
// Must not use one redis deployment with multiple Azuls

type RedisProvider struct {
	Redis *redis.Client
}

func newRedisProvider(dbnum int) (*RedisProvider, error) {
	if len(st.Events.Redis.Endpoint) == 0 {
		return nil, errors.New("no endpoint for redis")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         st.Events.Redis.Endpoint,
		Username:     st.Events.Redis.Username,
		Password:     st.Events.Redis.Password,
		MaxRetries:   st.Events.Redis.MaxRetries,
		DialTimeout:  time.Second * time.Duration(st.Events.Redis.ConnectionTimeoutSeconds),
		ReadTimeout:  time.Second * time.Duration(st.Events.Redis.ConnectionTimeoutSeconds),
		WriteTimeout: time.Second * time.Duration(st.Events.Redis.ConnectionTimeoutSeconds),
		DB:           dbnum,
	})
	ret := RedisProvider{
		Redis: rdb,
	}
	return &ret, nil
}

func retry[T any](genericCall func() (T, error)) (T, error) {
	var val T
	var err error
	for i := 0; i < st.Events.Redis.MaxRetries; i++ {
		val, err = genericCall()
		if err == nil {
			return val, err
		}
		time.Sleep(time.Second * time.Duration(st.Events.Redis.ConnectionTimeoutSeconds))
	}
	return val, err
}

func retryThree[T any, Z any](genericCall func() (T, Z, error)) (T, Z, error) {
	var val T
	var val2 Z
	var err error
	for i := 0; i < st.Events.Redis.MaxRetries; i++ {
		val, val2, err = genericCall()
		if err == nil {
			return val, val2, err
		}
		time.Sleep(time.Second * time.Duration(st.Events.Redis.ConnectionTimeoutSeconds))
	}
	return val, val2, err
}

func (prov *RedisProvider) GetDBSize(ctx context.Context) int64 {
	currentFunc := func() (int64, error) {
		response := prov.Redis.DBSize(ctx)
		if response.Err() != nil {
			return response.Val(), response.Err()
		}
		return response.Val(), nil
	}
	val, _ := retry(currentFunc)
	return val
}

func (prov *RedisProvider) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return retry(func() ([]byte, error) { return prov.Redis.Get(ctx, key).Bytes() })
}

func (prov *RedisProvider) GetTime(ctx context.Context, key string) (time.Time, error) {
	return retry(func() (time.Time, error) { return prov.Redis.Get(ctx, key).Time() })
}

func (prov *RedisProvider) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	_, err := retry(func() (int64, error) { return -1, prov.Redis.Set(ctx, key, value, expiration).Err() })
	return err
}

func (prov *RedisProvider) Del(ctx context.Context, key ...string) (int64, error) {
	return retry(func() (int64, error) { return prov.Redis.Del(ctx, key...).Result() })
}

func (prov *RedisProvider) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return retryThree(func() ([]string, uint64, error) { return prov.Redis.Scan(ctx, cursor, match, count).Result() })
}
