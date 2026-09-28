package kvprovider

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

/*Initialise all redis providers.*/
func NewMemoryProviders() (*KVMulti, error) {
	var err error
	ret := KVMulti{}
	ret.TrackPluginExecution, err = newMemoryProvider()
	if err != nil {
		return nil, err
	}
	ret.RegisteredPlugins, err = newMemoryProvider()
	if err != nil {
		return nil, err
	}
	ret.DeployedPlugins, err = newMemoryProvider()
	if err != nil {
		return nil, err
	}
	ret.PausePluginProcessingStartTime, err = newMemoryProvider()
	if err != nil {
		return nil, err
	}
	ret.Alerter, err = newMemoryProvider()
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

type MemoryProvider struct {
	Mem     map[string]string
	ListMem map[string][][]byte
	mu      sync.Mutex
}

func newMemoryProvider() (*MemoryProvider, error) {
	mem := MemoryProvider{
		Mem:     make(map[string]string),
		ListMem: make(map[string][][]byte),
	}
	return &mem, nil
}

func (prov *MemoryProvider) GetDBSize(ctx context.Context) int64 {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	return int64(len(prov.Mem))
}

func (prov *MemoryProvider) GetBytes(ctx context.Context, key string) ([]byte, error) {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	val, ok := prov.Mem[key]
	if !ok {
		return nil, redis.Nil
	}
	return []byte(val), nil
}

func (prov *MemoryProvider) GetTime(ctx context.Context, key string) (time.Time, error) {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	val, ok := prov.Mem[key]
	if !ok {
		return time.Time{}, redis.Nil
	}
	timeVal, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, fmt.Errorf("stored value %v was not a valid RFC3339 time with error %v", val, err)
	}
	return timeVal, nil
}

func (prov *MemoryProvider) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	encoded := ""
	switch value := value.(type) {
	case []byte:
		encoded = string(value)
	case time.Time:
		encoded = value.UTC().Format(time.RFC3339)
	default:
		return fmt.Errorf("bad type for in memory provider: %v", reflect.TypeOf(value))
	}
	prov.Mem[key] = encoded
	return nil
}

func (prov *MemoryProvider) PushToQueue(ctx context.Context, key string, value []byte) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	// Shouldn't have a key stored in memory or the key has been used with the wrong type.
	_, ok := prov.Mem[key]
	if ok {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := prov.ListMem[key]
	if ok {
		val = append(val, value)
		prov.ListMem[key] = val
	} else {
		prov.ListMem[key] = [][]byte{value}
	}
	return nil
}

func (prov *MemoryProvider) PopFromQueue(ctx context.Context, key string) ([]byte, error) {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	// Shouldn't have a key stored in memory or the key has been used with the wrong type.
	_, ok := prov.Mem[key]
	if ok {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := prov.ListMem[key]
	if ok {
		if len(val) == 0 {
			return []byte(nil), redis.Nil
		}
		// Pop first element off the list
		firstElem := val[0]
		val = val[1:]
		prov.ListMem[key] = val
		return firstElem, nil
	} else {
		return []byte(nil), redis.Nil
	}
}

func (prov *MemoryProvider) Del(ctx context.Context, keys ...string) (int64, error) {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	var deletedKeys int64
	for _, k := range keys {
		delete(prov.Mem, k)
		deletedKeys += 1
		delete(prov.ListMem, k)
		deletedKeys += 1
	}
	return deletedKeys, nil
}

func (prov *MemoryProvider) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	prov.mu.Lock()
	defer prov.mu.Unlock()
	// blindly assume globs are prefix or postfix wildcarded
	filter := strings.ReplaceAll(match, "*", "")
	keys := []string{}
	for k := range prov.Mem {
		if strings.Contains(k, filter) {
			keys = append(keys, k)
		}
	}
	for k := range prov.ListMem {
		if strings.Contains(k, filter) {
			keys = append(keys, k)
		}
	}
	return keys, 0, nil
}
