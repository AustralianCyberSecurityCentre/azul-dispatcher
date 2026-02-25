//go:build integration

package integration_streams

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

// must use same server for all tests here as otherwise the partitions will not get deallocated from consumers
// and new consumers with then not see any events
var server *httptest.Server
var conn *client.Client

func TestMain(m *testing.M) {
	defer st.ResetSettings()
	st.Events.Sources = `
sources:
  source: {}
  testing: {}
`
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
	if err != nil {
		panic(err)
	}
	kvprov, err := kvprovider.NewRedisProviders()
	if err != nil {
		panic(err)
	}
	dp := restapi.NewDispatcher(prov, kvprov, ctx)
	server = httptest.NewServer(dp.Router)
	conn = testdata.GetConnection(server.URL, "streams")
	defer server.Close()
	exitVal := m.Run()
	dp.Stop()
	os.Exit(exitVal)
}

type MockReader struct{}

func (self MockReader) Read(p []byte) (n int, err error) {
	val := []byte("blahblahblah")
	return copy(p, val), io.EOF
}

// upload and download binary
func TestUploadDownloadBinary(t *testing.T) {
	var exists bool
	// upload
	var mockRead io.Reader = MockReader{}
	bin, err := conn.PostStreamContent("source", mockRead)
	require.Nil(t, err)
	require.Equal(t, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f", bin.Sha256)

	// download
	bin2, err := conn.DownloadBinary("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Nil(t, err)
	readData, err := io.ReadAll(bin2)
	require.Nil(t, err)
	require.Equal(t, []byte("blahblahblah"), readData)

	// Download Chunk (first 7 bytes 0-6)
	binChunk, err := conn.DownloadBinaryChunk("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f", 0, 6)
	require.Nil(t, err)
	require.Equal(t, []byte("blahbla"), binChunk)

	// doesnt exist, but shouldn't error
	exists, err = conn.Exists("source", events.DataLabelContent, "000000000000000065965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, false)
	deleted, err := conn.DeleteBinary("source", events.DataLabelContent, "000000000000000065965076290a61638dfde0f2972474d73b954a10962a392f", nil)
	require.Equal(t, err, nil)
	require.Equal(t, deleted, false)
	exists, err = conn.Exists("source", events.DataLabelContent, "000000000000000065965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, false)

	// delete
	exists, err = conn.Exists("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, true)
	deleted, err = conn.DeleteBinary("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f", nil)
	require.Equal(t, err, nil)
	require.Equal(t, deleted, true)
	exists, err = conn.Exists("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, false)

	// delete based on modification time
	older := time.Date(2000, 01, 01, 01, 0, 0, 0, time.UTC)
	newer := time.Now().Add(time.Hour * 24)
	bin, err = conn.PostStreamContent("source", mockRead)
	require.Nil(t, err)
	exists, err = conn.Exists("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, true)
	deleted, err = conn.DeleteBinary("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f", &older)
	require.Equal(t, err, nil)
	require.Equal(t, deleted, false)
	exists, err = conn.Exists("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, true)
	deleted, err = conn.DeleteBinary("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f", &newer)
	require.Equal(t, err, nil)
	require.Equal(t, deleted, true)
	exists, err = conn.Exists("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962a392f")
	require.Equal(t, err, nil)
	require.Equal(t, exists, false)

	// download that will fail
	bin2, err = conn.DownloadBinary("source", events.DataLabelContent, "6e80a5bf1f6e165f65965076290a61638dfde0f2972474d73b954a10962aaaaa")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), `http response error 404 - {"status":"404","title":"not found","detail":"not found","error_enum":"ErrorStringEnumUnset"}`)
}
