package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// One is all that is required, if you see EOF more than twice it's the end of the actual file not just a blob.
const MAX_CONSECUTIVE_EOF = 1

type RetryReaderWrapper struct {
	innerRetryReader *blob.RetryReader
}

/*
Continually retry reading the retryReader until we are confident we have all the data from azure.

This wrapper is required because the azure RetryReader will return EOF at the end of each chunk of a blob rather than
at the end of the file.
So even if an EOF is received you have to try the read again and every it's the actual EOF and not end of a blob.
*/
func (rrw *RetryReaderWrapper) Read(p []byte) (n int, err error) {
	readBytes, err := rrw.innerRetryReader.Read(p)
	if err != nil && err != io.EOF {
		return readBytes, err
	}
	totalReadBytes := readBytes
	consecutiveEofWithNoDataRead := 0
	for consecutiveEofWithNoDataRead < MAX_CONSECUTIVE_EOF {
		// We've filled up the byte buffer p so return what we have.
		if totalReadBytes == len(p) {
			return totalReadBytes, nil
		}

		readBytes, err = rrw.innerRetryReader.Read(p[totalReadBytes:])
		totalReadBytes += readBytes
		if err != nil && err != io.EOF {
			return readBytes, err
		}
		if err == io.EOF && readBytes == 0 {
			consecutiveEofWithNoDataRead += 1
		} else if readBytes != 0 {
			consecutiveEofWithNoDataRead = 0
		}
	}
	return totalReadBytes, err
}

func (rrw *RetryReaderWrapper) Close() error {
	err := rrw.innerRetryReader.Close()
	return err
}

// StoreAzure is a FileStorage implementation to store files via an Azure blob store.
type StoreAzure struct {
	client        *azblob.Client  // A reference to the initialised Azure blob store client
	containerName string          // Name of the container files will be stored in
	ctx           context.Context // Parent context of the AzureStore instance
}

var contentType = "binary/octet-stream"

// NewAzureStore instantiates a new StoreAzure FileStore instance.
// The storage account name is specified by endpoint and must be provided in the
// format: "https://<storage-account-name>.blob.core.windows.net/".
// Files will be stored in the container named containerName.
// storageAccount is optional, and if empty the name will be extracted from the endpoint.
func NewAzureStore(endpoint string, containerName string, storageAccount string) (FileStorage, error) {
	var client *azblob.Client
	var err error

	// Parent context for this instance of the filestore
	ctx := context.Background()

	if st.Streams.Azure.AccessKey != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			log.Fatal(err)
		}
		// cloud storage is in format: https://<storage-account-name>.blob.core.windows.net/
		// Azurite local storage emulator will be in format http://<ip>:<port>/<storage-account-name>/
		// therefore storageAccount must be set manually for Azurite support
		storeName := storageAccount
		if storeName == "" {
			storeName = strings.Split(u.Hostname(), ".")[0]
		}
		cred, err := azblob.NewSharedKeyCredential(storeName, st.Streams.Azure.AccessKey)
		if err != nil {
			log.Fatalf("failed to obtain a credential: %v", err)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
		if err != nil {
			log.Fatalf("failed to obtain blobstore: %v", err)
		}
	} else {
		// This method is called service principal with secret and requires the following environment variables to be set:
		// AZURE_CLIENT_SECRET, AZURE_TENANT_ID, AZURE_CLIENT_ID
		// relevant documentation: https://github.com/Azure/azure-sdk-for-go/tree/main/sdk/azidentity
		// Note this is done via an Application registration.
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			log.Fatalf("failed to obtain a credential: %v", err)
		}

		client, err = azblob.NewClient(endpoint, cred, nil)
		if err != nil {
			log.Fatalf("failed to obtain blobstore: %v", err)
		}
	}

	// create container if required
	_, err = client.CreateContainer(ctx, containerName, nil)
	if err == nil {
		log.Printf("created container %s\n", containerName)
	} else if !bloberror.HasCode(err, bloberror.ResourceAlreadyExists, bloberror.ContainerAlreadyExists) {
		log.Fatalf("unhandled error of type %T: %s", err, err)
	} else {
		err = nil
	}

	return &StoreAzure{
		client,
		containerName,
		ctx,
	}, err

}

// Put uploads a file from the data buffer to the blob store.
//
// It only returns the first error encountered, if any.
func (s *StoreAzure) Put(source, label, id string, filePath string, fileSize int64) error {
	var err error

	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "put", err)
	}()
	id = strings.Join([]string{source, label, id}, "/")
	options := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		},
	}
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	bufferedReader := bufio.NewReaderSize(f, MAX_BUFFERED_READER_BYTES)

	// If file is large enough use concurrency.
	if fileSize > MAX_FILE_BYTES_BEFORE_CONCURRENT_UPLOAD {
		options.BlockSize = int64(CONCURRENT_BUFFER_SIZE_BYTES)
		options.Concurrency = NUM_CONCURRENT_UPLOAD_THREADS
	}

	_, err = s.client.UploadStream(s.ctx, s.containerName, id, bufferedReader, options)
	return err
}

// Fetch downloads the requested file given by the id and returns the data in a DataSlice.
// Partial files can be retrieved by specifying the offset and/or size.
//
// A zero or negative size indicates the whole file.
// A zero offset indicates the start of a file.
// A negative offset is treated as relative to the end of the file and if the negative offset is larger than the max
// filesize then the offset will be set to the start of the file.
// If an offset has a value in combination with a zero value size, this indicates from the offset to the end of the file.
//
// An offset that is equal to or greater than the size of the file will result in an OffsetAfterEnd error.
// A file that does not exist will return a NotFoundError error.
//
// Warning: If the size + offset is larger than the filesize then the data from the given offset
// to the end of file will be given instead.
func (s *StoreAzure) Fetch(source, label, id string, offset int64, size int64) (DataSlice, error) {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "fetch", err)
	}()
	id = updateIdPath(id, source, label)
	empty := NewDataSlice()
	c := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(id)
	// Custom logic to ensure all implementations of the storage interface handle offset and size
	// in the same way.
	stat, err := c.GetProperties(s.ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return empty, fmt.Errorf("%w", &NotFoundError{})
		}
		e := fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			e = fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", string(respErr.ErrorCode))})
		}
		return empty, e
	}

	// -ve file size => read all
	if size <= 0 {
		size = *stat.ContentLength
	}
	// treat -ve as relative to end
	if offset < 0 {
		offset = *stat.ContentLength + offset
	}
	// still -ve (-ve offset was bigger than file)
	if offset < 0 {
		offset = 0
	}
	// offset after or at end of file
	if offset > 0 && offset >= *stat.ContentLength {
		return empty, &OffsetAfterEnd{msg: fmt.Sprintf("offset after EOF: %d", *stat.ContentLength)}
	}

	// requested more than available (either though total size, or offset + size)
	if offset+size > *stat.ContentLength {
		size = (*stat.ContentLength - offset)
	}

	// Download the blob
	r := azblob.HTTPRange{Offset: offset, Count: size}
	get, err := c.DownloadStream(s.ctx, &blob.DownloadStreamOptions{Range: r})
	if err != nil {
		return empty, err
	}

	retryReader := get.NewRetryReader(s.ctx, &blob.RetryReaderOptions{})
	rrw := &RetryReaderWrapper{innerRetryReader: retryReader}
	return DataSlice{rrw, offset, size, *stat.ContentLength}, err
}

// Exists reports whether the file given by id exists in the filestore.
//
// A successful Exists check will return true if the file exists and false if the files does not exist.
// In the event of an error, a false value will be returned along side err == the error.
func (s *StoreAzure) Exists(source, label, id string) (bool, error) {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "exists", err)
	}()
	id = updateIdPath(id, source, label)
	prom.DataExists.Inc()

	c := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(id)
	_, err = c.GetProperties(s.ctx, &blob.GetPropertiesOptions{})
	if err == nil {
		return true, nil
	}

	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return false, nil
	} else {
		e := fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			e = fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", string(respErr.ErrorCode))})
		}
		return false, e
	}
}

// Delete marks the specified file for deletion. The file is later deleted during garbage collection.
// Note that deleting a file also deletes all its snapshots. For more information, see
// https://docs.microsoft.com/rest/api/storageservices/delete-blob.
//
// If onlyIfOlderThan is a zero or negative value, then the file is always deleted. If onlyIfOlderThan is a non-zero
// value the file is only deleted if the file has not been modifed since the given number of seconds since epoch.
//
// A successful Delete will return true. If the file or container do not exist then Delete will return NotFoundError.
// In the event of an error, a false value will be returned along side err == AccessError.
func (s *StoreAzure) Delete(source, label, id string, onlyIfOlderThan int64) (bool, error) {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "delete", err)
	}()
	id = updateIdPath(id, source, label)
	deleteOptions := &azblob.DeleteBlobOptions{}
	if onlyIfOlderThan > 0 {
		t := time.Unix(onlyIfOlderThan, 0)
		deleteOptions.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfUnmodifiedSince: &t,
			},
		}
	}
	_, err = s.client.DeleteBlob(s.ctx, s.containerName, id, deleteOptions)

	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound, bloberror.ContainerBeingDeleted) {
			return false, fmt.Errorf("%w", &NotFoundError{})
		} else {
			e := fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", err)})
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) {
				e = fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", string(respErr.ErrorCode))})
			}
			return false, e
		}
	}
	return true, nil
}

func (s *StoreAzure) Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "copy", err)
	}()
	// default srcObj to just the object name/hash
	srcObj := idOld
	existsAtRoot := false

	existsUnderSource, err := s.Exists(sourceOld, labelOld, idOld)
	if err != nil {
		return fmt.Errorf("error locating source object %s", srcObj)
	}
	if !existsUnderSource { // check under root if not found under source/label
		existsAtRoot, err = s.Exists("", "", idOld)
		if err != nil {
			return fmt.Errorf("error locating source object %s", srcObj)
		}
	}

	if existsUnderSource {
		srcObj = updateIdPath(idOld, sourceOld, labelOld)
	} else if existsAtRoot {
		// the object being copied does not exist at source/label/ as expected, copy from root
		srcObj = updateIdPath(idOld, "", "")
	} else {
		// silently fail copy as we could not find the source file under root or source/label
		st.Logger.Debug().Msgf("Object %s not found for copy operation", idOld)
		return nil
	}

	idNew = updateIdPath(idNew, sourceNew, labelNew)
	srcBlob := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(srcObj)
	dstBlob := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlockBlobClient(idNew)

	_, err = dstBlob.StartCopyFromURL(s.ctx, srcBlob.URL(), nil)
	if err != nil {
		st.Logger.Debug().Msgf("Object %s not found for copy operation", idOld)
		return nil
	}

	// Wait for copy operation to complete
	for {
		props, err := dstBlob.GetProperties(s.ctx, nil)
		if err != nil {
			return err
		}

		copyStatus := *props.CopyStatus
		if copyStatus != blob.CopyStatusTypePending {
			st.Logger.Info().Msgf("Copy operation completed with status: %v", copyStatus)
			break
		}

		// If the copy operation isn't complete, wait before trying again.
		time.Sleep(time.Second * 2)
	}
	return nil
}
