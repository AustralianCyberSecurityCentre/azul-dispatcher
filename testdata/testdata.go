package testdata

import (
	"embed"
	"log"
	"path"
	"runtime"
)

// dir of this go module, so tests can load files beneath it
var Dir string

//go:embed events
var Events embed.FS

func GetEvent(path string) string {
	ret, err := Events.ReadFile(path)
	if err != nil {
		log.Fatalf("could not load test file %v: %v", path, err)
	}
	return string(ret)
}

func GetEventBytes(path string) []byte {
	ret, err := Events.ReadFile(path)
	if err != nil {
		log.Fatalf("could not load test file %v: %v", path, err)
	}
	return ret
}

func GetEventInFlight(path string) []byte {
	ret, err := Events.ReadFile(path)
	if err != nil {
		log.Fatalf("could not load test file %v: %v", path, err)
	}
	return ret
}

func init() {
	_, filename, _, _ := runtime.Caller(0)
	Dir = path.Dir(filename)
}
