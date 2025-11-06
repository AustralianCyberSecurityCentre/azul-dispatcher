//go:build integration

package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

func TestStoreS3(t *testing.T) {
	s3Store, err := NewS3Store(
		st.Streams.S3.Endpoint,
		st.Streams.S3.AccessKey,
		st.Streams.S3.SecretKey,
		st.Streams.S3.Secure,
		st.Streams.S3.Bucket,
		st.Streams.S3.Region,
	)
	require.NoError(t, err)

	StoreImplementationBaseTests(t, s3Store)
}

func TestStoreS3WithCache(t *testing.T) {
	s3Store, err := NewS3Store(
		st.Streams.S3.Endpoint,
		st.Streams.S3.AccessKey,
		st.Streams.S3.SecretKey,
		st.Streams.S3.Secure,
		st.Streams.S3.Bucket,
		st.Streams.S3.Region,
	)
	require.NoError(t, err)
	// Ensure max file size stored is 2kb.
	cacheStore, err := NewDataCache(1, 300, 256, s3Store)
	require.NoError(t, err, "Error creating LocalStore Cache")

	StoreImplementationBaseTests(t, cacheStore)
}
