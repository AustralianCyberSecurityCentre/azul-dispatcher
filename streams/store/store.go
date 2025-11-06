package store

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
)

// Size of the buffered readers buffer 1MB
const MAX_BUFFERED_READER_BYTES = 1024 * 1024

// Perform a concurrent upload at 50MiB
const MAX_FILE_BYTES_BEFORE_CONCURRENT_UPLOAD = 50 * 1024 * 1024

// Concurrent upload threads
const NUM_CONCURRENT_UPLOAD_THREADS = 10

// Buffer_Sizes (must be 5MiB+ for AWS minimum chunk size)
const CONCURRENT_BUFFER_SIZE_BYTES = 6 * 1024 * 1024

type DataSlice struct {
	DataReader         io.ReadCloser
	Start, Size, Avail int64
}

/* Create an empty Dataslice with a reader with no bytes.*/
func NewDataSlice() DataSlice {
	return DataSlice{
		DataReader: io.NopCloser(bytes.NewReader([]byte{})),
		Start:      0,
		Size:       0,
		Avail:      0,
	}
}

/* Provide file object storage. */
type FileStorage interface {
	Put(source, label, id string, filePath string, fileSize int64) error
	Fetch(source, label, id string, offset int64, size int64) (DataSlice, error)
	Exists(source, label, id string) (bool, error)
	// Delete deletes the specified key if older than supplied unix timestamp in seconds
	Delete(source, label, id string, ifOlderThan int64) (bool, error)
	Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error
}

type OffsetAfterEnd struct {
	msg string
}

func (r *OffsetAfterEnd) Error() string {
	return r.msg
}

type NotFoundError struct{}

func (e *NotFoundError) Error() string {
	return "not found"
}

type AccessError struct {
	msg string
}

func (e *AccessError) Error() string {
	return fmt.Sprintf("no access: %v", e.msg)
}

type ReadError struct {
	msg string
}

func (e *ReadError) Error() string {
	return fmt.Sprintf("read error: %v", e.msg)
}

// updateIdPath prefixes source and label path to id if source and label are non-empty
func updateIdPath(id, source, label string) string {
	return strings.Join([]string{source, label, id}, "/")
}

// reportStreamsOpMetric report a streams method duration for prometheus
func reportStreamsOpMetric(startTime int64, operationName string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	durationSeconds := float64(time.Now().UnixNano()-startTime) / 1e9
	prom.StreamsOperationDuration.WithLabelValues(operationName, result).Observe(durationSeconds)
}
