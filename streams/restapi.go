package streams

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client/poststreams"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi/restapi_handlers"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/identify"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
)

// get value from the query string, replace empty string with alt parameter
func getWithDefault(qv url.Values, key string, alt string) string {
	ret := qv.Get(key)
	if ret == "" {
		ret = alt
	}
	return ret
}

// jsonResponse returns a json response to client
func jsonResponse(c *gin.Context, response any) {
	c.Writer.Header().Set("Content-Type", "application/json")
	out, err := json.Marshal(response)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "json error", err)
		return
	}
	_, err = c.Writer.Write(out)
	if err != nil {
		log.Println(err)
	}
}

// GetData returns the content for the requested hash and supports http range headers.
func (s *Streams) GetData(c *gin.Context) {
	prom.DataDownloads.Inc()
	c.Writer.Header().Set("Content-Type", "application/octet-stream")
	ra := httpRange{0, -1}
	var err error
	if len(c.GetHeader("Range")) > 0 {
		ra, err = parseRange(c.GetHeader("Range"))
		if err != nil {
			restapi_handlers.JSONError(c, 400, "invalid range provided", err)
			st.Logger.Error().Err(err).Msgf("invalid range provided")
			return
		}
	}

	// Check in store with source and label prefix
	source := c.Params.ByName("source")
	label := c.Params.ByName("label")
	hash := c.Params.ByName("hash")
	content, err := s.Store.Fetch(source, label, hash, store.WithOffsetAndSize(ra.start, ra.length))

	var notFoundError *store.NotFoundError
	if errors.As(err, &notFoundError) {
		// Check in store without source and label prefix
		source = ""
		label = ""
		content, err = s.Store.Fetch(source, label, hash, store.WithOffsetAndSize(ra.start, ra.length))
	}

	if err != nil {
		// file was not found at root or under source and label prefix
		if errors.As(err, &notFoundError) {
			restapi_handlers.JSONError(c, 404, "not found", err)
			return
		}
		var offsetAfterEnd *store.OffsetAfterEnd
		if errors.As(err, &offsetAfterEnd) {
			// If the error is a range input error return 400
			restapi_handlers.JSONError(c, 400, "invalid range end", err)
			st.Logger.Error().Err(err).Msgf("invalid range end")
			return
		} else { // catch errors from original fetch and fallback fetch
			restapi_handlers.JSONError(c, 500, "store error in GetData", err)
			st.Logger.Error().Err(err).Msgf("store error in GetData %s/%s/%s", c.Params.ByName("source"), c.Params.ByName("label"), c.Params.ByName("hash"))
			return
		}
	}
	defer content.DataReader.Close()

	prom.DataDownloaded.Add(float64(content.Size))
	// return range info in response for partial
	if len(c.GetHeader("Range")) > 0 {
		c.Writer.Header().Set("Content-Range",

			fmt.Sprintf("bytes %d-%d/%d",
				content.Start,
				content.Start+content.Size-1,
				content.Avail))
		c.DataFromReader(http.StatusPartialContent, content.Size, "application/octet-stream", content.DataReader, map[string]string{})
	} else {
		c.Writer.Header().Set("Accept-Ranges", "bytes")
		c.DataFromReader(http.StatusOK, content.Size, "application/octet-stream", content.DataReader, map[string]string{})
	}
}

/*
HasData returns 200 if the requested hash exists in the store, 404 if not.
(It is expected to be called with a HEAD request.)
*/
func (s *Streams) HasData(c *gin.Context) {
	prom.DataExists.Inc()
	// Check in store with source and label prefix
	exist, err := s.Store.Exists(c.Params.ByName("source"), c.Params.ByName("label"), c.Params.ByName("hash"))
	if err != nil {
		st.Logger.Error().Err(err).Msgf("checking if hasData 500 error for file %s/%s/%s", c.Params.ByName("source"), c.Params.ByName("label"), c.Params.ByName("hash"))
		c.Writer.WriteHeader(500)
		c.Writer.Flush()
		return
	}
	if !exist {
		// Check in store without source and label prefix
		exist, err = s.Store.Exists("", "", c.Params.ByName("hash"))
		if err != nil {
			st.Logger.Error().Err(err).Msgf("checking if hasData 500 error for file %s/%s/%s", c.Params.ByName("source"), c.Params.ByName("label"), c.Params.ByName("hash"))
			c.Writer.WriteHeader(500)
			c.Writer.Flush()
			return
		}
		if !exist {
			c.Writer.WriteHeader(404)
			c.Writer.Flush()
			return
		}
	}
}

/*
PostStream allows a client to upload content and returns a basic set of its corresponding metadata.
*/
func (s *Streams) PostStream(c *gin.Context) {
	var err error
	qv := c.Request.URL.Query()
	// skip identify will speed up storage by not performing any file analysis
	// but requires trusting the supplied sha256 hash
	paramSkipIdentify, err := strconv.ParseBool(getWithDefault(qv, poststreams.SkipIdentify, "false"))
	if err != nil {
		restapi_handlers.JSONError(c, 400, "bad param skip identify", err)
		return
	}
	paramExpectedSha256 := qv.Get(poststreams.ExpectedSha256)
	if paramSkipIdentify && len(paramExpectedSha256) == 0 {
		restapi_handlers.JSONError(c, 400, "must supply sha256 when skipping identify", err)
		return
	}

	prom.DataUploads.Inc()
	c.Writer.Header().Set("Content-Type", "application/json")
	response := models.DataResponse{}
	file, err := os.CreateTemp(st.Streams.FCache.Path, "buffered-put")
	if err != nil {
		restapi_handlers.JSONError(c, 500, "filecaching error in PutData", err)
		return
	}
	defer file.Close()
	defer os.Remove(file.Name())
	var fileSize uint64
	// 10kB buffering
	maxBytesBufferBytes := 10240
	// Ensure request body closes.
	defer c.Request.Body.Close()
	bufferedReader := bufio.NewReaderSize(c.Request.Body, maxBytesBufferBytes)
	buf := make([]byte, maxBytesBufferBytes)
	var readBytes int
	var writtenBytes int
	hasher := identify.NewHasher(paramSkipIdentify)
	for {
		readBytes, err = bufferedReader.Read(buf)
		if err != nil && err != io.EOF {
			restapi_handlers.JSONError(c, 500, "filebuffering error, failed to read file in PutData", err)
			return
		}
		if readBytes == 0 {
			break
		}
		err = hasher.Write(buf[:readBytes])
		if err != nil {
			return
		}
		writtenBytes, err = file.Write(buf[:readBytes])
		if err != nil {
			restapi_handlers.JSONError(c, 500, "filebuffering error, failed to write file in PutData", err)
			return
		}
		fileSize += uint64(writtenBytes)
		if err == io.EOF {
			break
		}
	}

	// generate hashes for the stream
	metadata, err := hasher.Cook()
	if err != nil {
		restapi_handlers.JSONError(c, 500, "hasher error", err)
		return
	}

	if fileSize == 0 {
		restapi_handlers.JSONError(c, 400, "no content in request body", nil)
		return
	}
	if len(paramExpectedSha256) > 0 && paramExpectedSha256 != metadata.Sha256 {
		err = fmt.Errorf("sha256 mismatch - supplied: %v vs calc: %v", paramExpectedSha256, metadata.Sha256)
		restapi_handlers.JSONError(c, 400, "sha256 mismatch", err)
		return
	}
	if !paramSkipIdentify {
		// identify the file types
		err = s.identifier.Identify(file.Name(), metadata)
		if err != nil {
			restapi_handlers.JSONError(c, 500, "identify error", err)
			return
		}
	}

	// put binary into s3
	attempt := 0
	for attempt < 3 {
		_, err = file.Seek(0, 0)
		if err != nil {
			attempt += 1
			st.Logger.Warn().Err(err).Msgf("Retrying upload of file %s/%s/%s - due to error when seeking", c.Params.ByName("source"), c.Params.ByName("label"), metadata.Sha256)
			// Randomly sleep for up to 5seconds to deconflict with the colliding upload.
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			continue
		}
		err = s.Store.Put(c.Params.ByName("source"), c.Params.ByName("label"), metadata.Sha256, file, int64(fileSize))
		// Retry upload if an error occurred as it can be because of two concurrent uploads of the same file.
		if err != nil {
			attempt += 1
			st.Logger.Warn().Err(err).Msgf("Retrying upload of file %s/%s/%s", c.Params.ByName("source"), c.Params.ByName("label"), metadata.Sha256)
			// Randomly sleep for up to 5seconds to deconflict with the colliding upload.
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			continue
		}
		break
	}

	if err != nil {
		st.Logger.Err(err).Msgf("Unsuccessful after upload retry of file  %s/%s/%s", c.Params.ByName("source"), c.Params.ByName("label"), metadata.Sha256)
		restapi_handlers.JSONError(c, 500, "store error in PutData", err)
		return
	}
	prom.DataUploaded.Add(float64(fileSize))
	response.Data = *metadata
	jsonResponse(c, response)
}

// Copies data from one source to another
func (s *Streams) CopyData(c *gin.Context) {
	sourceOld := c.Params.ByName("sourceA")
	sourceNew := c.Params.ByName("sourceB")

	id := c.Params.ByName("hash")
	// By default labels shouldn't change between copies - this is unlikely to make sense in most cases
	// (we don't want an argumented stream to be copied as a regular binary and vice versa)
	labelOld := c.Params.ByName("label")
	labelNew := labelOld

	exist, err := s.Store.Exists(sourceOld, labelOld, id)
	if err != nil {
		st.Logger.Err(err).Msg("store error in CopyData")
		return
	}
	if !exist {
		// fall back to no source and label prefix
		sourceOld = ""
		labelOld = ""
		// additional exists check as copy function does not error when the source file does not exist
		exist, err = s.Store.Exists(sourceOld, labelOld, id)
		if err != nil {
			st.Logger.Err(err).Msg("store error in CopyData")
			return
		}
		if !exist {
			st.Logger.Err(err).Msg("source file not found for copy operation")
			return
		}
	}

	err = s.Store.Copy(sourceOld, labelOld, id, sourceNew, labelNew, id)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "copy error", err)
		return
	}
}

// DeleteData allows a client to delete data from storage.
func (s *Streams) DeleteData(c *gin.Context) {
	prom.DataDeletes.Inc()

	// read extra parameter
	qv := c.Request.URL.Query()
	rawOlderThan := qv.Get("ifOlderThan")
	if len(rawOlderThan) <= 0 {
		rawOlderThan = "0"
	}
	ifOlderThan, err := strconv.Atoi(rawOlderThan)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "ifOlderThan error", err)
		return
	}
	// Check in store with source and label prefix
	source := c.Params.ByName("source")
	label := c.Params.ByName("label")
	hash := c.Params.ByName("hash")
	ok, err := s.Store.Delete(source, label, hash, store.WithDeleteIfOlderThan(int64(ifOlderThan)))

	var notFoundError *store.NotFoundError
	if errors.As(err, &notFoundError) {
		// Check in store without source and label prefix
		source = ""
		label = ""
		ok, err = s.Store.Delete(source, label, hash, store.WithDeleteIfOlderThan(int64(ifOlderThan)))
	}

	if err != nil {
		// file was not found at root or under source and label prefix
		if errors.As(err, &notFoundError) {
			restapi_handlers.JSONError(c, 404, "not found", err)
			return
		} else { // catch errors from original fetch and fallback fetch
			restapi_handlers.JSONError(c, 500, "store error in DeleteData", err)
			return
		}
	}
	st.Logger.Info().Str("hash", hash).Msg("deleted")
	jsonResponse(c, map[string]bool{"deleted": ok})
}
