//go:build integration_azure

package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

func TestAzureStore(t *testing.T) {
	azureStore, err := NewAzureStore(st.Streams.Azure.Endpoint, st.Streams.Azure.Container, st.Streams.Azure.StorageAccount)
	require.NoError(t, err)

	StoreImplementationBaseTests(t, azureStore)
}

func TestAzureStoreWithCache(t *testing.T) {
	azureStore, err := NewAzureStore(st.Streams.Azure.Endpoint, st.Streams.Azure.Container, st.Streams.Azure.StorageAccount)
	require.NoError(t, err)

	// Ensure max file size stored is 2kb.
	cacheStore, err := NewDataCache(1, 300, 256, azureStore)
	require.NoError(t, err, "Error creating LocalStore Cache")

	StoreImplementationBaseTests(t, cacheStore)
}
