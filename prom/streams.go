package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	DataExists = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_exist_requests_total",
		Help: "The total number of data existence checks performed",
	})
	DataUploads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_upload_requests_total",
		Help: "The total number of data uploads performed",
	})
	DataUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_uploaded_bytes_total",
		Help: "The total number of bytes uploaded",
	})
	DataDownloads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_download_requests_total",
		Help: "The total number of data fetches performed (including partial requests)",
	})
	DataDownloaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_downloaded_bytes_total",
		Help: "The total number of bytes downloaded",
	})
	DataDeletes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_delete_requests_total",
		Help: "The total number of data deletes performed",
	})
	MetadataDownloads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_metadata_requests_total",
		Help: "The total number metadata fetches performed",
	})
	CacheLookups = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_cache_lookups_total",
		Help: "The total number of data cache lookups performed",
	})
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatcher_data_cache_hits_total",
		Help: "The total number of data cache hits",
	})
	IdentifyFail = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_identify_fail",
		Help: "The total number of file identification failures",
	}, []string{"mime", "magic"})
	StreamsOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_streams_op_duration",
		Help:    "Duration of a Streams Operation, not including request time",
		Buckets: []float64{.005, .01, .025, .050, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"method", "result"})
)
