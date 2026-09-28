//go:build integration

package kvprovider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisProvider(t *testing.T) {
	prov, err := newRedisProvider(0)
	require.Nil(t, err)

	baseKvTest(t, prov)
}
