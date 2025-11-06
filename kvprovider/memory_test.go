package kvprovider

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestMemoryProvider(t *testing.T) {
	prov, err := newMemoryProvider()
	require.Nil(t, err)

	// should be same tests as redis provider as they should function identically
	keys, cursor, err := prov.Scan(ctx, 0, "domain.*", 1000)
	require.Nil(t, err)
	require.Equal(t, cursor, uint64(0))
	require.Equal(t, keys, []string{})

	res, err := prov.GetBytes(ctx, "domain.test")
	require.Nil(t, err)
	require.Nil(t, res)

	err = prov.Set(ctx, "domain.test", []byte("my message"), 0)
	require.Nil(t, err)

	res, err = prov.GetBytes(ctx, "domain.test")
	require.Nil(t, err)
	require.Equal(t, res, []byte("my message"))

	resTime, err := prov.GetTime(ctx, "domain.testtime")
	require.Equal(t, redis.Nil, err)
	require.Equal(t, time.Time{}, resTime)

	keys, cursor, err = prov.Scan(ctx, 0, "domain.*", 1000)
	require.Nil(t, err)
	require.Equal(t, cursor, uint64(0))
	require.ElementsMatch(t, keys, []string{"domain.test"})

	err = prov.Set(ctx, "domain.test2", []byte("my message"), 0)
	require.Nil(t, err)
	err = prov.Set(ctx, "domain.test3", []byte("my message"), 0)
	require.Nil(t, err)
	err = prov.Set(ctx, "domain.test4", []byte("my message"), 0)
	require.Nil(t, err)

	keys, cursor, err = prov.Scan(ctx, 0, "domain.*", 1000)
	require.Nil(t, err)
	require.Equal(t, cursor, uint64(0))
	require.ElementsMatch(t, keys, []string{
		"domain.test2",
		"domain.test3",
		"domain.test4",
		"domain.test",
	})

	currentTime := time.Now()
	err = prov.Set(ctx, "domain.testtime", currentTime, 0)
	require.Nil(t, err)

	resTime, err = prov.GetTime(ctx, "domain.testtime")
	require.Nil(t, err)
	// Standardise formatting to remove nano seconds etc.
	require.Equal(t, currentTime.Format(time.RFC3339), resTime.Format(time.RFC3339))
}
