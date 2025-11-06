package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ReplayCacheHitCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_replay_cache_hit_count",
		Help: "Number of replayed events from cache",
	}, []string{"name"})
	ReplayCacheHitSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_replay_cache_hit_seconds",
		Help: "Seconds saved by replayed events from cache",
	}, []string{"name"})
	ProcessingTimes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "dispatcher_processing_time_seconds",
		Help: "Duration of consumer result processing as reported in status responses",
		// Unfortunately need a diverse range of values due to diff plugin but it also multiplies out the number of metrics
		Buckets: []float64{.005, .01, .025, .050, .1, .25, .5, 1, 2.5, 5, 30, 60, 120, 300, 600, 1200, 1800, 2400},
	}, []string{"publisher"})
	RestapiTimes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_restapi_time_seconds",
		Help:    "Duration of restapi processing",
		Buckets: []float64{.005, .01, .025, .050, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"method", "path"})
	RestapiCodes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_restapi_response_codes",
		Help: "The response codes for restapi endpoints",
	}, []string{"method", "path", "code"})
)
