package store

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

/* Store files via s3 provider. */
type StoreS3 struct {
	client *minio.Client
	bucket string
}

/** Creates a new S3 store with static credentials. */
func NewS3Store(endpoint string, accessKey string, secretKey string, secure bool, bucket string, region string) (FileStorage, error) {
	var client *minio.Client
	var err error
	opts := minio.Options{
		Secure: secure,
		Region: region,
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
	}
	// accessKey, secretKey
	client, err = minio.New(endpoint, &opts)
	if err != nil {
		return nil, err
	}
	b, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, err
	}
	if !b {
		err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	}
	if err != nil {
		return nil, err
	}

	return &StoreS3{
		client,
		bucket,
	}, err
}

/** Creates a new S3 store using IAM credentials. */
func NewS3StoreIAM(endpoint string, secure bool, bucket string, region string) (FileStorage, error) {
	var client *minio.Client
	var err error
	opts := minio.Options{
		Secure: secure,
		Region: region,
		Creds:  credentials.NewIAM(""),
	}

	client, err = minio.New(endpoint, &opts)
	if err != nil {
		return nil, err
	}
	b, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, err
	}
	if !b {
		err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	}
	if err != nil {
		return nil, err
	}

	return &StoreS3{
		client,
		bucket,
	}, err
}

func (s *StoreS3) Put(source, label, id string, filePath string, fileSize int64) error {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "put", err)
	}()
	id = updateIdPath(id, source, label)
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open cache file when streaming to S3 file id is: %s", id)
	}
	defer f.Close()
	bufRead := bufio.NewReaderSize(f, MAX_BUFFERED_READER_BYTES)

	options := minio.PutObjectOptions{ContentType: "binary/octet-stream"}
	// If file is large enough use concurrency.
	if fileSize > MAX_FILE_BYTES_BEFORE_CONCURRENT_UPLOAD {
		options.NumThreads = uint(NUM_CONCURRENT_UPLOAD_THREADS)
		options.ConcurrentStreamParts = true
		options.PartSize = uint64(CONCURRENT_BUFFER_SIZE_BYTES)
	}
	_, err = s.client.PutObject(
		context.Background(),
		s.bucket,
		id,
		bufRead,
		fileSize, // set to -1 unless size is known
		options,
	)
	if err != nil {
		return err
	}
	return nil
}

func (s *StoreS3) Fetch(source, label, id string, offset int64, size int64) (DataSlice, error) {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "fetch", err)
	}()
	id = updateIdPath(id, source, label)
	empty := NewDataSlice()
	reader, err := s.client.GetObject(context.Background(), s.bucket, id, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return empty, fmt.Errorf("%w", &NotFoundError{})
		}
		return empty, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}

	// Custom logic to ensure all implementations of the storage interface handle offset and size
	// in the same way.
	stat, err := reader.Stat()
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			reader.Close()
			return empty, fmt.Errorf("%w", &NotFoundError{})
		}
		reader.Close()
		return empty, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}

	// -ve or zero is read all
	if size <= 0 {
		size = stat.Size
	}
	// treat -ve as relative to end
	if offset < 0 {
		offset = stat.Size + offset
	}
	// still -ve (-ve offset was bigger than file)
	if offset < 0 {
		offset = 0
	}
	// offset after end of file
	if offset > 0 && offset >= stat.Size {
		reader.Close()
		return empty, &OffsetAfterEnd{msg: fmt.Sprintf("offset after EOF: %d", stat.Size)}
	}
	// requested more than available
	// should we error or be lenient?
	if offset+size > stat.Size {
		size = stat.Size - offset
	}

	// don't bother going back to remote for a 0 byte object, just return the empty dataslice with details
	if stat.Size == 0 {
		reader.Close()
		return empty, nil
	}

	_, err = reader.Seek(offset, 0)
	if err != nil {
		reader.Close()
		return empty, fmt.Errorf("%w", &ReadError{msg: fmt.Sprintf("%v", err)})
	}
	// Limit the reader so gin doesn't read beyond the selected file size.
	wrappedLimitedReader := NewCloseWrapper(io.LimitReader(reader, size), reader)
	return DataSlice{wrappedLimitedReader, offset, size, stat.Size}, nil
}

func (s *StoreS3) Exists(source, label, id string) (bool, error) {
	prom.DataExists.Inc()
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "exists", err)
	}()
	id = updateIdPath(id, source, label)
	_, err = s.client.StatObject(context.Background(), s.bucket, id, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.Code == "NoSuchBucket" {
		return false, nil
	}
	return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", resp.Code)})
}

func (s *StoreS3) Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error {
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
	src := minio.CopySrcOptions{
		Bucket: s.bucket,
		Object: srcObj,
	}
	dst := minio.CopyDestOptions{
		Bucket: s.bucket,
		Object: idNew,
	}
	ui, err := s.client.CopyObject(context.Background(), dst, src)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		return fmt.Errorf("s3 copy operation retured error %s", resp.Code)
	}
	fmt.Printf("Copied %s, successfully to %s - UploadInfo %v\n", src.Object, dst.Object, ui)

	return nil
}

func (s *StoreS3) Delete(source, label, id string, onlyIfOlderThan int64) (bool, error) {
	var err error
	startTime := time.Now().UnixNano()
	defer func() {
		reportStreamsOpMetric(startTime, "delete", err)
	}()
	// a timing issue exists here as the read and delete are not atomic operations
	// we could enable versioning to fix this, but probably not worth it
	id = updateIdPath(id, source, label)

	obj, err := s.client.StatObject(context.Background(), s.bucket, id, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return false, fmt.Errorf("%w", &NotFoundError{})
		}
		return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}
	if onlyIfOlderThan > 0 && obj.LastModified.Unix() >= onlyIfOlderThan {
		// don't delete if object is newer than the required timestamp
		return false, nil
	}
	err = s.client.RemoveObject(context.Background(), s.bucket, id, minio.RemoveObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return false, fmt.Errorf("%w", &NotFoundError{})
		}
		return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}
	return true, nil

}
