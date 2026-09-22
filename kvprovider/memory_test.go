package kvprovider

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func baseKvTest(t *testing.T, prov KVInterface) {
	// clear previous run
	keys, cursor, err := prov.Scan(ctx, 0, "domain.*", 1000)
	require.Nil(t, err)
	for _, k := range keys {
		_, err := prov.Del(ctx, k)
		require.Nil(t, err)
	}

	// should be same tests as memory provider as they should function identically
	keys, cursor, err = prov.Scan(ctx, 0, "domain.*", 1000)
	require.Nil(t, err)
	require.Equal(t, cursor, uint64(0))
	require.Equal(t, keys, []string{})

	res, err := prov.GetBytes(ctx, "domain.test")
	require.Equal(t, err, redis.Nil)
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

	// Push and Pop operations.
	notListKey := "notList"
	listKey := "testList"

	// Try and set the key to a value and then attempt to append and pop from the list
	err = prov.Set(ctx, notListKey, []byte("abcdef"), 0)
	require.Nil(t, err)
	err = prov.PushToQueue(ctx, notListKey, []byte("abcdef"))
	require.NotNil(t, err)
	_, err = prov.PopFromQueue(ctx, notListKey)
	require.NotNil(t, err)

	err = prov.PushToQueue(ctx, listKey, []byte("abcdef"))
	require.Nil(t, err)
	listVal, err := prov.PopFromQueue(ctx, listKey)
	require.Nil(t, err)
	require.Equal(t, listVal, []byte("abcdef"))
	listVal, err = prov.PopFromQueue(ctx, listKey)
	require.Equal(t, err, redis.Nil)
	require.Equal(t, listVal, []byte(nil))
}

func TestMemoryProvider(t *testing.T) {
	prov, err := newMemoryProvider()
	require.Nil(t, err)
	baseKvTest(t, prov)
}
