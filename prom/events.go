package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	//
	// consume
	//
	EventsConsumeRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_event_fetch_requests_total",
		Help: "The total number of event fetch requests serviced",
	}, []string{"plugin", "entity"})
	EventsConsumeFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_fetched_total",
		Help: "The total number of events fetched",
	}, []string{"plugin", "entity"})
	EventsConsumeFiltered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_filtered_total",
		Help: "The total number of events filtered from consumers",
	}, []string{"plugin", "entity"})
	EventsConsumeStagesDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_events_consume_duration",
		Help:    "Duration of stages when consuming events",
		Buckets: []float64{.00001, .0001, .001, .01, .1, 1.0, 5.0, 10.0},
	}, []string{"stage"})
	// consumer pipeline stage monitoring
	EventsConsumePipelineDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_events_consume_pipelines_duration",
		Help:    "Duration of piplines during pipeline stage",
		Buckets: []float64{.00001, .0001, .001, .01, .1, 1.0, 5.0, 10.0},
	}, []string{"pipeline"})
	EventsConsumePipelineRemoved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_consume_pipelines_removed",
		Help: "Events dropped by this pipeline",
	}, []string{"pipeline"})
	EventsConsumePipelineError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_consume_pipelines_error",
		Help: "Errors occurring during the pipeline.",
	}, []string{"pipeline"})
	// track not-ready consumers for the plugin
	EventsConsumeNotReady = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_events_consume_not_ready",
		Help: "For the plugin, number of consumers that are NOT ready",
	}, []string{"plugin", "version"})

	//
	// produce
	//
	EventsProducePublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_published_total",
		Help: "The total number of events published",
	}, []string{"plugin", "entity"})
	EventsProduceStatusPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_status_published",
		Help: "The total number of status events published",
	}, []string{"plugin", "status"})
	EventsProduceStagesDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_events_produce_duration",
		Help:    "Duration of stages when producing events",
		Buckets: []float64{.00001, .0001, .001, .01, .1, 1.0, 5.0, 10.0},
	}, []string{"stage"})
	// producer pipeline stage monitoring
	EventsProducePipelineDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatcher_events_produce_pipelines_duration",
		Help:    "Duration of piplines during pipeline stage",
		Buckets: []float64{.00001, .0001, .001, .01, .1, 1.0, 5.0, 10.0},
	}, []string{"pipeline"})
	EventsProducePipelineRemoved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_produce_pipelines_removed",
		Help: "Events dropped by this pipeline",
	}, []string{"pipeline"})
	EventsProducePipelineAdded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_produce_pipelines_added",
		Help: "Additional events produced by this pipeline",
	}, []string{"pipeline"})
	EventsProducePipelineError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_produce_pipelines_error",
		Help: "Errors occurring during the pipeline.",
	}, []string{"pipeline"})

	// dispatcher receiving wrong version of events
	EventsProduceBadVersion = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_produce_bad_version",
		Help: "Dispatcher received newly published events not up-to-date with latest event schema",
	}, []string{"entity", "version", "author"})

	// redis statistics
	EventsRedisTracking = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_events_redis_tracking",
		Help: "Number of events that redis is currently tracking.",
	}, []string{"origin"})
	EventsRedisTrackingControl = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_events_redis_tracking_control",
		Help: "Number of events that redis is currently tracking.",
	}, []string{"origin"})

	// Sarama statistics
	KafkaReceiveMessageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_kafka_consumed_message_bytes_total",
		Help: "Sarama total bytes received for a topic and group.",
	}, []string{"name", "group", "topic"})

	KafkaRebalanceCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_kafka_rebalance_count",
		Help: "Sarama rebalance count: Total number of rebalances (assign or revoke)",
	}, []string{"name", "group"})

	KafkaConsumerResetCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_kafka_consumer_restart_count",
		Help: "Sarama consumer reassign: Total number of times a Sarama consumer has had to reassign it's partitions (kerrorCode is the kafka error code 100 means not a kafka error).",
	}, []string{"name", "group", "kerrorCode"})

	// Producer stats.
	KafkaTransmitMessageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatcher_kafka_produced_message_bytes_total",
		Help: "Total bytes produced from dispatcher to Kafka.",
	}, []string{"topic"})
	KafkaTransmitErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_kafka_produced_message_error_total",
		Help: "Total number of produce events that have failed.",
	}, []string{"topic"})
)
