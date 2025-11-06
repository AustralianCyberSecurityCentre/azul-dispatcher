package dedupe

import (
	"math"
	"sync/atomic"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	"github.com/cespare/xxhash/v2"
)

// Lookup is the antibloom cache.
type Lookup struct {
	keys     []uint64
	sizeMask uint64
}

// New returns a Lookup with the specified capacity in bytes.
func New(size uint64) *Lookup {
	// round up
	size = uint64(math.Pow(2, math.Ceil(math.Log2(float64(size)))))
	if size < 8 {
		size = 8
	}
	// 8 bytes per entry (uint64)
	size = size / 8
	sizeMask := size - 1          // masked val = array index
	slice := make([]uint64, size) // prealloc to trigger any mem issues upfront
	return &Lookup{slice, sizeMask}
}

// CheckAndSet will return true if the lookup contains the specified value, and add the value as seen.
func (l *Lookup) CheckAndSet(val []byte) bool {
	prom.DedupeCacheLookups.Inc()
	h := xxhash.Sum64(val)
	index := h & l.sizeMask
	oldHash := getAndSet(l.keys, index, h)
	if oldHash == h {
		prom.DedupeCacheHits.Inc()
	} else if oldHash != 0 {
		prom.CacheCollisions.Inc()
	}
	return oldHash == h
}

// getAndSet will replace a value at specified index and return previous content.
func getAndSet(arr []uint64, index uint64, val uint64) uint64 {
	indexPtr := &arr[index]
	var oldVal uint64
	for {
		oldVal = atomic.LoadUint64(indexPtr)
		if atomic.CompareAndSwapUint64(indexPtr, oldVal, val) {
			break
		}
	}
	return oldVal
}
