//go:build integration

package kvprovider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisProvider(t *testing.T) {
	prov, err := newRedisProvider(0)
	require.Nil(t, err)

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

	_, err = prov.GetBytes(ctx, "domain.test")
	require.NotNil(t, err)

	err = prov.Set(ctx, "domain.test", []byte("my message"), 0)
	require.Nil(t, err)

	res, err := prov.GetBytes(ctx, "domain.test")
	require.Nil(t, err)
	require.Equal(t, res, []byte("my message"))

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
}
