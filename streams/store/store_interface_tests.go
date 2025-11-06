package store

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/identify"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDataSliceBytesInterfaceTest(t *testing.T, ds DataSlice) []byte {
	// ReadAll is used because the restAPI will attempt to read everything that dataReader provides it.
	// This can cause issues when requesting partial content (range headers) and the dataReader isn't limiting the number of bytes.
	// It will cause an over read meaning the response will be partial content but actually have more bytes in the body then expected (e.g body is meant to be 10 bytes but is actually 20)
	// This causes 500 errors on the client side.
	data, err := io.ReadAll(ds.DataReader)

	if err != nil && err != io.EOF {
		t.Logf("input was %v", ds)
		t.Errorf("Failed to read the data from dataslice in tests! with error %s", err.Error())
	}
	return data
}

func loadFileBytes(t *testing.T, relPath string) []byte {
	path := testdata.Dir + relPath
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to check stats for test file %s with error %s", path, err.Error())
	}

	rawTestFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open test file %s with error %s", path, err.Error())
	}
	defer rawTestFile.Close()
	// Verify test file is at leat 50kb
	fileSize := fileInfo.Size()
	require.Greater(t, fileSize, int64(50000))
	rawFileBytes := make([]byte, fileSize)
	_, err = rawTestFile.Read(rawFileBytes)
	if err != nil {
		t.Fatalf("Failed to read all of mid size test file. %s", err.Error())
	}
	return rawFileBytes
}

// Generic tests that anything implementing the store interface should pass.
func StoreImplementationBaseTests(t *testing.T, fs FileStorage) {
	identifier, err := identify.NewIdentifier()
	require.NoError(t, err)
	defer identifier.Close()

	// test table to run over multiple "files"
	tests := []struct {
		name  string
		input []byte
	}{
		{"EmptyFile", []byte("")},
		{"SimpleFile", []byte("This is a really boring sentence.")},
		{"MediumSizeFile", loadFileBytes(t, "/streams/random-long-text-file.txt")},
	}
	largeFileTestDoneOnce := false
	// run the tests over each "file"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			f, err := os.CreateTemp("", "store-cache-test")
			if err != nil {
				t.Fatalf("Failed to create tempfile %s", err.Error())
			}
			defer f.Close()
			defer os.Remove(f.Name())
			_, err = f.Write(test.input)
			if err != nil {
				t.Fatalf("Failed to write input data to temp file %s", err.Error())
			}

			m, err := identifier.HashAndIdentify(f.Name())
			require.NoError(err, "Error occurred identifying file")

			// Check for non-existent file
			exists, err := fs.Exists("source", events.DataLabelContent.Str(), m.Sha256)
			assert.NoError(err, "No error was returned when checking existance of non-existant file")
			assert.False(exists, "Exists check did not return False for a non-existant file")

			// Attempt to Get a file that doesn't exist
			ds, err := fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, -1)
			assert.ErrorIs(err, &NotFoundError{}, "Did not get a NotFound error for non-existant file")
			assert.Zero(len(getDataSliceBytesInterfaceTest(t, ds)), "Data returned for non-existant file")
			// Attempt to delete a file that doesn't exist
			didDelete, err := fs.Delete("source", events.DataLabelContent.Str(), m.Sha256, -1)
			assert.ErrorIs(err, &NotFoundError{}, "Did not get a NotFound error when deleting a non-existant file")
			assert.False(didDelete, "Delete did not return False for non-existant object")

			// Save file
			err = fs.Put("source", events.DataLabelContent.Str(), m.Sha256, f.Name(), int64(len(test.input)))
			assert.NoError(err, "Error occured while saving file")
			defer func() {
				if _, err := fs.Delete("source", events.DataLabelContent.Str(), m.Sha256, -1); err != nil {
					if !errors.Is(err, &NotFoundError{}) {
						panic("Failed to cleanup test file")
					}
				}
			}()

			// Check exists
			exists, err = fs.Exists("source", events.DataLabelContent.Str(), m.Sha256)
			assert.NoError(err, "Got error when checking for file")
			assert.True(exists, "File exists check did not return true")

			// Copy file (use Sha512 for target)
			dstFile := m.Sha512
			err = fs.Copy("source", events.DataLabelContent.Str(), m.Sha256, "source2", "content2", dstFile)
			assert.NoError(err, "Got error when copying file")
			// Check new file exists
			exists, err = fs.Exists("source2", "content2", m.Sha512)
			assert.NoError(err, "Got error when checking if copied file exists")
			assert.True(exists, "The Copied file does not exist")

			// Get entire file using a negative size
			ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, -1)
			assert.NoError(err, "Error occured fetching file with a negative size")
			assert.Equal(test.input, getDataSliceBytesInterfaceTest(t, ds), "Entire file was not returned using a negative size")

			// Get entire file using a zero size
			ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, 0)
			assert.NoError(err, "Error occured while fetching file with a zero size input")
			assert.Equal(int(m.Size), len(getDataSliceBytesInterfaceTest(t, ds)), "Entire file not returned using a zero size")

			// skip this portion for zero/small size file as we test out of bounds conditions further below
			if len(test.input) > 1 {
				// Get a partial file from a positive offset
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 1, -1)
				assert.NoError(err, "Error returned when fetching partial file using positive offset and negative size")
				assert.Equal(test.input[1:], getDataSliceBytesInterfaceTest(t, ds), "Fetched data from positive offset returned different content to input")

				// get partial file from zero offset and positive size
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, 1)
				assert.NoError(err, "Error returned when fetching partial file using zero offset and positive size")
				assert.Equal(test.input[0:1], getDataSliceBytesInterfaceTest(t, ds), "Fetched data with non-zero size returned different content to input")

				// get partial file from positive offset and positive size
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 1, 1)
				assert.NoError(err, "Error returned when fetching partial file using positive offset and positive size")
				assert.Equal(test.input[1:2], getDataSliceBytesInterfaceTest(t, ds), "Fetched data from non-zero offset and size returned different content to input")

				// get partial file from negative offset
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, -1, 0)
				assert.NoError(err, "Error returned when fetching partial file using negative offset and zero size")
				assert.Equal(test.input[len(test.input)-1:len(test.input)], getDataSliceBytesInterfaceTest(t, ds),
					"Fetched data from non-zero offset and size returned different content to input")
			}

			if len(test.input) > 9000 {
				offsetToFind := int64(8192)
				// Get a partial file with a specific larger non-MiB offset
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, offsetToFind)
				assert.NoError(err, fmt.Sprintf("Error returned when trying to fetch a fixed offset of '%d'", offsetToFind))
				assert.Equal(test.input[0:offsetToFind], getDataSliceBytesInterfaceTest(t, ds), "Fetched precisely %d bytes from store", offsetToFind)
				largeFileTestDoneOnce = true
			}

			// get file with offset out of bounds
			ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, int64(m.Size)+1, 1)
			assert.NotNil(err, "Fetched data with offset outside of bounds was not handled")
			var offsetAfterEnd *OffsetAfterEnd
			assert.ErrorAs(err, &offsetAfterEnd, "Fetched data with offset outside of bounds, should provide a RangeInputError")
			empty := NewDataSlice()
			assert.Equal(empty, ds, "Fetched data from outside bounds returned result")
			if len(test.input) > 1 {
				// offset at exactly end of file.
				// this doesn't make sense for a zero size file, as a zero offset means the whole file
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, int64(m.Size), 0)
				assert.NotNil(err, "No error fetching with offset of max_filesize and zero size")
				assert.ErrorAs(err, &offsetAfterEnd, "Fetched data with offset of filesize, should provide a RangeInputError")
				assert.Equal(empty, ds, "Fetched data from offset = filesize returned result")
			}
			if len(test.input) > 2 {
				// request more data than size of file with a combination of offset + size
				// can't test this on a zero size file as that would be a negative offset which is tested else where
				// can't test this on a one byte file as that would trigger the test above
				ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, int64(m.Size-1), 5)
				assert.NoError(err, "Error fetching file with more data than size of file using a combination of offset + size")
				assert.Equal(1, len(getDataSliceBytesInterfaceTest(t, ds)), "Did not return remaining file when offset+size > file size")
			}

			// get file with negative offset larger than file
			ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, -int64(m.Size*2), -1)
			assert.NoError(err, "Error returned fetching file with negative offset larger than file")
			assert.Equal(int(m.Size), len(getDataSliceBytesInterfaceTest(t, ds)), "Did not return expected buffer size")

			// request size greater than size of file
			ds, err = fs.Fetch("source", events.DataLabelContent.Str(), m.Sha256, 0, int64(m.Size+1))
			assert.NoError(err, "Error returned fetching file with size greater than size of file")
			assert.Equal(int(m.Size), len(getDataSliceBytesInterfaceTest(t, ds)), "Did not return whole file when size larger than file requested")

			// Delete file
			didDelete, err = fs.Delete("source", events.DataLabelContent.Str(), m.Sha256, 0)
			assert.NoError(err, "Error returned deleting file")
			assert.True(didDelete, "Delete returned false on successful delete")

			exists, err = fs.Exists("source", events.DataLabelContent.Str(), m.Sha256)
			assert.NoError(err, "Error returned checking for non-existent file")
			assert.False(exists, "Data returned for non-existent file")
		})
	}
	require.True(t, largeFileTestDoneOnce, "Large file test did not run ensure at least one of the test files is large enough.")
}

func benchmarkWriteStoreWithSize(b *testing.B, fs FileStorage, size int) {
	randomData := make([]byte, size)
	_, err := rand.Read(randomData)
	if err != nil {
		b.Fatalf("Failed to generate random data: %s", err.Error())
	}

	tmpFile, err := os.CreateTemp("", "azul-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp file: %s", err.Error())
	}

	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(randomData)
	if err != nil {
		b.Fatalf("Failed to write to temp file: %s", err.Error())
	}

	b.ReportAllocs()

	for i := range b.N {
		// Generate a ID for this file - not a hash, but will be unique across benchmarks
		fileName := fmt.Sprintf("test-file-%d-%d", size, i)
		err = fs.Put("source", "label", fileName, tmpFile.Name(), int64(size))
		if err != nil {
			b.Fatalf("Failed to read data from provider: %s", err.Error())
		}
	}
}

func benchmarkReadStoreWithSize(b *testing.B, fs FileStorage, size int) {
	randomData := make([]byte, size)
	_, err := rand.Read(randomData)
	if err != nil {
		b.Fatalf("Failed to generate random data: %s", err.Error())
	}

	tmpFile, err := os.CreateTemp("", "azul-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp file: %s", err.Error())
	}

	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(randomData)
	if err != nil {
		b.Fatalf("Failed to write to temp file: %s", err.Error())
	}

	// Just store the file once
	// Generate a ID for this file - not a hash, but will be unique across benchmarks
	fileName := fmt.Sprintf("test-file-%d", size)
	err = fs.Put("source", "label", fileName, tmpFile.Name(), int64(size))
	if err != nil {
		b.Fatalf("Failed to store file for test: %s", err.Error())
	}

	b.ReportAllocs()

	for range b.N {
		_, err = fs.Fetch("source", "label", fileName, 0, -1)
		if err != nil {
			b.Fatalf("Failed to read data from provider: %s", err.Error())
		}
	}
}

func BaseBenchmarkReadStore(b *testing.B, fs FileStorage) {
	sizes := []int{1, 33, 1024, 1024 * 1024}

	for _, size := range sizes {
		runName := fmt.Sprintf("%d", size)
		b.Run(runName, func(b *testing.B) {
			benchmarkReadStoreWithSize(b, fs, size)
		})
	}
}

func BaseBenchmarkWriteStore(b *testing.B, fs FileStorage) {
	sizes := []int{1, 33, 1024, 1024 * 1024}

	for _, size := range sizes {
		runName := fmt.Sprintf("%d", size)
		b.Run(runName, func(b *testing.B) {
			benchmarkWriteStoreWithSize(b, fs, size)
		})
	}
}
