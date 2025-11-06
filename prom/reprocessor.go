package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Per topic signal for when EOF on all partitions has been reached
	ReprocessorEOF = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "reprocessor_eof",
		Help: "Signal for when workers have processed all partitions for a given topic",
	}, []string{"source_topic"})

	// Incrementing count of valid events that have been reprocessed
	ReprocessorEventCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reprocessor_event_count",
		Help: "Number of events reprocessed from each source topic",
	}, []string{"source_topic"})
)
