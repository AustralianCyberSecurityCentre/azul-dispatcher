package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	DedupeCacheLookups = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_dedupe_lookups_total",
		Help: "The total number of dedupe lookups",
	})
	DedupeCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_dedupe_hits_total",
		Help: "The total number of dedupe cache hits",
	})
	CacheCollisions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_dedupe_collisions_total",
		Help: "The total number of dedupe cache collisions",
	})
)
