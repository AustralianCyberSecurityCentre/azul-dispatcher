//go:build integration

package pauser

import (
	"context"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/stretchr/testify/require"
)

func TestGettingPauserValueBeforeSetRedis(t *testing.T) {
	kvprov, err := kvprovider.NewRedisProviders()
	require.Nil(t, err)
	defer ClearLastPauseTime(context.Background(), kvprov)
	testGettingPauserValueBeforeSet(t, kvprov)
}

func TestBasicPauserRedis(t *testing.T) {
	kvprov, err := kvprovider.NewRedisProviders()
	require.Nil(t, err)
	defer ClearLastPauseTime(context.Background(), kvprov)
	testBasicPauser(t, kvprov)
}

func TestBackgroundPauseProcessingWithLockRedis(t *testing.T) {
	kvprov, err := kvprovider.NewRedisProviders()
	require.Nil(t, err)
	defer ClearLastPauseTime(context.Background(), kvprov)
	testBackgroundPauseProcessingWithLock(t, kvprov)
}

func TestMinutelyBackgroundPauserRedis(t *testing.T) {
	kvprov, err := kvprovider.NewRedisProviders()
	require.Nil(t, err)
	defer ClearLastPauseTime(context.Background(), kvprov)
	testMinutelyBackgroundPauser(t, kvprov)
}
