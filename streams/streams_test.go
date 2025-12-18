package streams

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/store"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/identify"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertPanic(t *testing.T, f func(), rx string) {
	// Ensures that the given function called with no arguments will panic with a string matching the regex
	defer func() {
		if r := recover(); r != nil {
			if m, _ := regexp.MatchString(rx, r.(string)); !m {
				t.Fatalf("panicked with wrong message ('%v' ~/~ '%v')", r, rx)
			}
		} else {
			t.Fatalf("Did not panic as expected - %v", r)
		}
	}()
	f()
}

var identifier identify.Identifier

func TestMain(m *testing.M) {
	var err error
	identifier, err = identify.NewIdentifier()
	if err != nil {
		panic(err.Error)
	}
	defer identifier.Close()

	bk := st.Streams.S3.Endpoint
	st.Streams.S3.Endpoint = ""

	// run tests
	exitVal := m.Run()

	st.Streams.S3.Endpoint = bk
	os.Exit(exitVal)
}

func TestInitLocalStore(t *testing.T) {
	// Tests store can be changed
	current := st.Streams.Backend
	st.Streams.Backend = "local"
	defer func() {
		st.Streams.Backend = current
	}()

	var apiStreams = NewStreams()
	defer apiStreams.Close()
	value, ok := apiStreams.Store.(*store.StoreXOR)
	if !ok {
		t.Fatalf("Default backend should be wrapped with a XORStore, but it is a %T", apiStreams.Store)
	}

	_, ok = value.Backend.(*store.StoreFilesystem)
	if !ok {
		t.Fatalf("Default store should be a LocalStore, but it is a %T", value.Backend)
	}
}

func TestInitDefaultStore(t *testing.T) {
	// Tests default setting for store is "s3"
	backend := st.Streams.Backend
	if backend != "" {
		t.Skip("Skipping default backend test as a backend has been specified.")
	}
	current := st.Streams.S3.AccessKey
	// CI pipeline sets keys so must overwrite
	st.Streams.S3.AccessKey = ""
	defer func() {
		st.Streams.S3.AccessKey = current
	}()
	defer func() {
		err := recover()
		assert.Contains(t, err, "S3 Store URL", "Default settings did not panic for s3 store with unset keys")
	}()
	var apiStreams = NewStreams()
	defer apiStreams.Close()
	_, _ = apiStreams.Store.(*store.StoreFilesystem)
}

func TestInitInvalidStore(t *testing.T) {
	// Tests an empty or unknown store panics
	current := st.Streams.Backend
	st.Streams.Backend = ""
	defer func() {
		st.Streams.Backend = current
	}()
	defer func() {
		err := recover()
		assert.Contains(t, err, "No STORE_BACKEND", "empty store did not panic")
	}()
	var apiStreams = NewStreams()
	defer apiStreams.Close()
	_, _ = apiStreams.Store.(*store.StoreFilesystem)
}

func tmpLocalStore(dirname string) *store.StoreFilesystem {
	// Creates a temporary directory and returns a LocalStore pointing to it
	dir, err := os.MkdirTemp("/tmp", dirname)
	if err != nil {
		panic("Error creating temp dir: " + err.Error())
	}
	ls, err := store.NewEmptyLocalStore(dir)
	if err != nil {
		panic("Error creating LocalStore: " + err.Error())
	}
	return ls.(*store.StoreFilesystem) // Assert type, since NewEmptyLocalStore returns a Store interface
}

// Sets up testing objects, calls the given method with the given parameters,
//
//	and returns the Response object from the ResponseRecorder.
func testEndpointWrapper(fn func(c *gin.Context), meth string, endpoint string, params gin.Params) *http.Response {
	rw := httptest.NewRecorder()
	req := httptest.NewRequest(meth, endpoint, bytes.NewReader([]byte{}))
	c, _ := gin.CreateTestContext(rw)
	c.Request = req
	c.Params = params
	fn(c)
	return rw.Result()
}

// Tests HasData, GetData, GetMeta
func TestGetMethods(t *testing.T) {
	current := st.Streams.Backend
	st.Streams.Backend = "local"
	defer func() {
		st.Streams.Backend = current
	}()
	var apiStreams = NewStreams()
	defer apiStreams.Close()
	// Set default store to a new empty store
	apiStreams.Store = tmpLocalStore("test-dispatcher-defaultstore")
	defer os.RemoveAll(apiStreams.Store.(*store.StoreFilesystem).GetRootPath())
	// Redirect logging so we can check for messages
	var logbuf bytes.Buffer
	log.SetOutput(&logbuf)
	defer func() {
		log.SetOutput(os.Stderr)
		if logs := logbuf.String(); logs != "" {
			t.Fatalf("Logs were:\n%v", logs)
		}
	}()

	// Set up some test data
	test_data := []byte("Test data content")
	test_hash := fmt.Sprintf("%x", sha256.Sum256(test_data))

	var res *http.Response
	// Try with no data in store at all
	res = testEndpointWrapper(apiStreams.HasData, "HEAD", "/api/v1/data"+test_hash, gin.Params{gin.Param{Key: "hash", Value: test_hash}})
	if res.Status != "404 Not Found" {
		t.Errorf("HasData returned %s with no data present (should be 404)", res.Status)
	}
	res = testEndpointWrapper(apiStreams.GetData, "GET", "/api/v1/data/"+test_hash, gin.Params{gin.Param{Key: "hash", Value: test_hash}})
	if res.Status != "404 Not Found" {
		t.Errorf("GetData returned %s with no data present (should be 404)", res.Status)
	}
	// res = testEndpointWrapper(apiStreams.GetMeta, "GET", "/api/v1/data/"+test_hash+"/meta", gin.Params{gin.Param{Key: "hash", Value: test_hash}})
	// if res.Status != "404 Not Found" {
	// 	t.Errorf("GetMeta returned %s with no data present (should be 404)", res.Status)
	// }

	f, err := os.CreateTemp("", "store-cache-test")
	if err != nil {
		t.Fatalf("Failed to create tempfile %s", err.Error())
	}
	defer f.Close()
	defer os.Remove(f.Name())
	_, err = f.Write(test_data)

	// Add data to default store
	m, err := identifier.HashAndIdentify(f.Name())
	require.Nil(t, err)
	test_source := "source"
	test_label := "stream"

	_, err = f.Seek(0, 0)
	require.Nil(t, err, "failed when attempting to seek file to put into store back to 0.")
	err = apiStreams.Store.Put(test_source, test_label, m.Sha256, f, int64(len(test_data)))
	if err != nil {
		panic("Error putting data to store: " + err.Error())
	}
	if test_hash != m.Sha256 {
		t.Errorf("Hash mismatch between test data and store return metadata; %s != %s", test_hash, m.Sha256)
	}

	// Check that it finds it
	// setup params
	testParams := gin.Params{gin.Param{
		Key:   "source",
		Value: test_source,
	}, gin.Param{
		Key:   "label",
		Value: test_label,
	}, gin.Param{
		Key:   "hash",
		Value: test_hash,
	}}
	res = testEndpointWrapper(apiStreams.HasData, "HEAD", "/api/v1/data"+test_hash, testParams)
	if res.Status != "200 OK" {
		t.Errorf("HasData returned %s with data in default store (should be 200)", res.Status)
	}
	res = testEndpointWrapper(apiStreams.GetData, "GET", "/api/v1/data/"+test_hash, testParams)
	if res.Status != "200 OK" {
		t.Errorf("GetData returned %s with data in default store (should be 200)", res.Status)
	}
	buf := make([]byte, len(test_data))
	if _, _ = res.Body.Read(buf); !reflect.DeepEqual(buf, test_data) {
		t.Errorf("GetData returned non-matching content (%v)", buf)
	}
	defer res.Body.Close()
	// res = testEndpointWrapper(apiStreams.GetMeta, "GET", "/api/v1/data/"+test_hash+"/meta", testParams)
	// if res.Status != "200 OK" {
	// 	t.Errorf("GetMeta returned %s with data in default store (should be 200)", res.Status)
	// }

}

func TestPutData(t *testing.T) {
	current := st.Streams.Backend
	st.Streams.Backend = "local"
	defer func() {
		st.Streams.Backend = current
	}()
	var apiStreams = NewStreams()
	defer apiStreams.Close()
	// Set default store to a new empty store
	apiStreams.Store = tmpLocalStore("test-dispatcher-defaultstore")
	defer os.RemoveAll(apiStreams.Store.(*store.StoreFilesystem).GetRootPath())

	// Redirect logging so we can check for messages
	var logbuf bytes.Buffer
	log.SetOutput(&logbuf)
	defer func() {
		log.SetOutput(os.Stderr)
		if logs := logbuf.String(); logs != "" {
			t.Fatalf("Logs were:\n%v", logs)
		}
	}()

	// Set up some test data
	test_data := []byte("Test data content")
	test_hash := fmt.Sprintf("%x", sha256.Sum256(test_data))

	var res *http.Response
	var rw *httptest.ResponseRecorder
	var req *http.Request

	test_source := "source"
	test_label := "stream"

	// Try to put data into default store and check that it's there
	rw = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/data", strings.NewReader(string(test_data)))
	c, _ := gin.CreateTestContext(rw)
	c.Request = req
	c.Params = gin.Params{gin.Param{
		Key:   "source",
		Value: test_source,
	}, gin.Param{
		Key:   "label",
		Value: test_label,
	}}

	apiStreams.PostStream(c)
	res = rw.Result()
	if res.Status != "200 OK" {
		t.Errorf("PutData error: %s", res.Status)
	}

	// Test data is in default store only
	if exist, err := apiStreams.Store.Exists(test_source, test_label, test_hash); err != nil {
		t.Error("Error calling store.Exists")
	} else if !exist {
		t.Error("Data is not present in default store")
	}

	// Set up some test data
	test_data2 := []byte("Test data content #2")
	test_hash2 := fmt.Sprintf("%x", sha256.Sum256(test_data2))

	test_source2 := "src3"
	test_label2 := "stream3"

	// Try a non-default store
	rw = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/data", strings.NewReader(string(test_data2)))
	c, _ = gin.CreateTestContext(rw)
	c.Request = req
	c.Params = gin.Params{gin.Param{
		Key:   "source",
		Value: test_source2,
	}, gin.Param{
		Key:   "label",
		Value: test_label2,
	}}
	apiStreams.PostStream(c)
	res = rw.Result()
	if res.Status != "200 OK" {
		t.Errorf("PutData error: %s", res.Status)
	}

	// Data #2 should now be in only b2 store
	if exist, err := apiStreams.Store.Exists(test_source2, test_label2, test_hash2); err != nil {
		t.Error("Error calling store.Exists")
	} else if !exist {
		t.Error("Data is not resent in default store when it should be")
	}
}

func TestCopyData(t *testing.T) {
	current := st.Streams.Backend
	st.Streams.Backend = "local"
	defer func() {
		st.Streams.Backend = current
	}()
	var apiStreams = NewStreams()
	defer apiStreams.Close()
	// Set default store to a new empty store
	apiStreams.Store = tmpLocalStore("test-dispatcher-defaultstore")
	defer os.RemoveAll(apiStreams.Store.(*store.StoreFilesystem).GetRootPath())

	// Set up some test data
	test_data := []byte("Test data content")
	test_hash := fmt.Sprintf("%x", sha256.Sum256(test_data))

	var res *http.Response
	var rw *httptest.ResponseRecorder
	var req *http.Request

	test_source := "source"
	test_source_b := "othersource"
	test_label := "stream"

	// Try to put data into default store
	rw = httptest.NewRecorder()
	req = httptest.NewRequest("PATCH", "/api/v1/fake-endpoint", strings.NewReader(string(test_data)))
	c, _ := gin.CreateTestContext(rw)
	c.Request = req
	c.Params = gin.Params{gin.Param{
		Key:   "source",
		Value: test_source,
	}, gin.Param{
		Key:   "label",
		Value: test_label,
	}}

	apiStreams.PostStream(c)
	res = rw.Result()
	if res.Status != "200 OK" {
		t.Errorf("PutData error: %s", res.Status)
	}

	// Copy
	rw = httptest.NewRecorder()
	req = httptest.NewRequest("PATCH", "/api/v1/fake-endpoint", strings.NewReader(string(test_data)))
	c, _ = gin.CreateTestContext(rw)
	c.Request = req
	c.Params = gin.Params{gin.Param{
		Key:   "sourceA",
		Value: test_source,
	}, gin.Param{
		Key:   "sourceB",
		Value: test_source_b,
	}, gin.Param{
		Key:   "label",
		Value: test_label,
	}, gin.Param{
		Key:   "hash",
		Value: test_hash,
	}}

	apiStreams.CopyData(c)
	res = rw.Result()
	if res.Status != "200 OK" {
		t.Errorf("CopyData error: %s", res.Status)
	}

	// Validate that data is stored in both sourceA and sourceB
	if exist, err := apiStreams.Store.Exists(test_source, test_label, test_hash); err != nil {
		t.Error("Error calling store.Exists")
	} else if !exist {
		t.Error("Data is not available in default store, source A when it should be")
	}

	if exist, err := apiStreams.Store.Exists(test_source_b, test_label, test_hash); err != nil {
		t.Error("Error calling store.Exists")
	} else if !exist {
		t.Error("Data is not available in default store, source B when it should be")
	}
}
