package store

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXORStore(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(t, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(t, err, "Error creating local store", err)

	xorStore := NewXORStore(store, true)

	StoreImplementationBaseTests(t, xorStore)
}

func TestPlainStore(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(t, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(t, err, "Error creating local store", err)

	xorStore := NewXORStore(store, false)

	StoreImplementationBaseTests(t, xorStore)
}

func TestAtRest(t *testing.T) {
	/* Assert that data is actually encoded when stored */
	assert := assert.New(t)

	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(t, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(t, err, "Error creating local store", err)

	xorStore := NewXORStore(store, true)

	file, err := os.CreateTemp("/tmp", "test-dispatcher-store")
	require.NoError(t, err, "Error creating temp file", err)
	defer file.Close()
	defer os.Remove(file.Name())

	var probMalware = []byte("Hello, this is malware!")
	_, err = file.Write(probMalware)

	require.NoError(t, err, "Error writing to temp file", err)

	err = xorStore.Put("testsource", "testlabel", "testid", file.Name(), int64(len(probMalware)))
	require.NoError(t, err, "Error writing to XOR store", err)

	// The XOR store should return the original text
	testData, err := xorStore.Fetch("testsource", "testlabel", "testid", 0, -1)
	require.NoError(t, err, "Error reading from XOR store", err)

	readBuffer := getDataSliceBytesInterfaceTest(t, testData)
	assert.Equal(probMalware, readBuffer)

	// The filesystem store should not
	testData, err = store.Fetch("testsource", "testlabel", "testid"+XOR_FILE_EXT, 0, -1)
	require.NoError(t, err, "Error reading from local store", err)

	readBuffer = getDataSliceBytesInterfaceTest(t, testData)
	assert.NotEqual(probMalware, readBuffer)
}

func TestPlainAfterXOR(t *testing.T) {
	/* Asserts that a disabled XOR wrapper correctly finds XOR'd files & that files afterwards
	   are stored without a XOR */
	assert := assert.New(t)

	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(t, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(t, err, "Error creating local store", err)

	xorStore := NewXORStore(store, true)

	file, err := os.CreateTemp("/tmp", "test-dispatcher-store")
	require.NoError(t, err, "Error creating temp file", err)
	defer file.Close()
	defer os.Remove(file.Name())

	var probMalware = []byte("Hello, this is malware!")
	_, err = file.Write(probMalware)

	require.NoError(t, err, "Error writing to temp file", err)

	err = xorStore.Put("testsource", "testlabel", "xordfile", file.Name(), int64(len(probMalware)))
	require.NoError(t, err, "Error writing to XOR store", err)

	// The filesystem store should not return the original string while XORing was on
	testData, err := store.Fetch("testsource", "testlabel", "xordfile"+XOR_FILE_EXT, 0, -1)
	require.NoError(t, err, "Error reading from local store", err)

	readBuffer := getDataSliceBytesInterfaceTest(t, testData)
	assert.NotEqual(probMalware, readBuffer)

	// Disabling XORing should still return valid contents for a file stored with XORing on when
	// fetched via the XOR store
	xorStore = NewXORStore(store, false)

	testData, err = xorStore.Fetch("testsource", "testlabel", "xordfile", 0, -1)
	require.NoError(t, err, "Error reading from XOR store", err)

	readBuffer = getDataSliceBytesInterfaceTest(t, testData)
	assert.Equal(probMalware, readBuffer)

	// Storing a 'new' file should result in it not being XOR'd
	err = xorStore.Put("testsource", "testlabel", "notxoredfile", file.Name(), int64(len(probMalware)))
	require.NoError(t, err, "Error writing to XOR store", err)

	// The filesystem provider should return the correct content
	testData, err = store.Fetch("testsource", "testlabel", "notxoredfile", 0, -1)
	require.NoError(t, err, "Error reading from local store", err)

	readBuffer = getDataSliceBytesInterfaceTest(t, testData)
	assert.Equal(probMalware, readBuffer)
}

func BenchmarkXORReadStore(b *testing.B) {
	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(b, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(b, err, "Error creating local store", err)

	xorStore := NewXORStore(store, true)

	BaseBenchmarkReadStore(b, xorStore)
}

func BenchmarkXORWriteStore(b *testing.B) {
	dir, err := os.MkdirTemp("/tmp", "test-dispatcher-store")
	defer os.RemoveAll(dir)
	require.NoError(b, err, "Error creating temp dir", err)

	store, err := NewEmptyLocalStore(dir)
	require.NoError(b, err, "Error creating local store", err)

	xorStore := NewXORStore(store, true)

	BaseBenchmarkWriteStore(b, xorStore)
}
