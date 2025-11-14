/*
Package settings controls reading configuration from environment and assigning defaults
*/
package settings

import (
	"log" // cannot use zerolog as log options not initialised
	"os"

	"github.com/go-viper/mapstructure/v2"

	bedsettings "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
)

var Settings *DPSettings
var Streams *DPStreams
var Events *DPEvents

type DPStreamsLocal struct {
	Path string `koanf:"path"`
}

type DPStreamsAzure struct {
	Endpoint       string `koanf:"endpoint"`
	StorageAccount string `koanf:"storage_account"`
	Container      string `koanf:"container"`
	AccessKey      string `koanf:"access_key"`
}

type DPStreamsS3 struct {
	// S3 server address or empty to use local storage instead
	Endpoint string `koanf:"endpoint"`
	// Access key to auth against S3 bucket
	AccessKey string `koanf:"access_key"`
	// Secret key to auth against S3 bucket
	SecretKey string `koanf:"secret_key"`
	// Whether to utilise HTTPS for S3 transport
	Secure bool `koanf:"secure"`
	// S3 region or empty if unsupported by server
	Region string `koanf:"region"`
	// S3 bucket name to store to (will attempt to create if not exists)
	Bucket string `koanf:"bucket"`
}

type DPStreamCache struct {
	/*
		NOTE when enabling the content cache it will consume 1.5 times the amount of
		RAM you allocate in the cache size bytes. e.g if you set it to 1GiB it will use 1.5GiB of RAM.
		This is because for every shard in bigcache the shard buffers every file you attempt to insert into it.
		Dispatcher limits this to half the total bytes a shard can hold.
		This means each shard can have a buffer up to half it's size and the total size of all the shards is the cache size.

		The recommendation for sizing is 32 shards with a 1GiB cache.
		This means shards are 32MB in size and can store files up to 16MB in size.
	*/
	// In memory content/data caching size
	SizeBytes bedsettings.HumanReadableBytes `koanf:"size_bytes"`
	// Number of cache shards, concurrency vs max object size
	Shards int64 `koanf:"shards"`
	// Max TimeToLive for cached data, in seconds.
	TTLSeconds int64 `koanf:"ttl_seconds"`
}

type DPStreamFileCache struct {
	/*Absolute path for where to cache files before they are forwarded to streams (e.g S3/Azure...)*/
	Path string `koanf:"path"`
}

type DPStreams struct {
	// valid backends: s3, azure, local
	Backend string            `koanf:"backend"`
	S3      DPStreamsS3       `koanf:"s3"`
	Azure   DPStreamsAzure    `koanf:"azure"`
	Local   DPStreamsLocal    `koanf:"local"`
	Cache   DPStreamCache     `koanf:"cache"`
	FCache  DPStreamFileCache `koanf:"fcache"`

	// simple settings
	APIAllowDelete bool `koanf:"api_allow_delete"`

	// Store files with a fixed XOR pattern to avoid AV detections
	XOREncoding bool `koanf:"xor_encoding"`
}

type DPKafka struct {
	// Kafka bootstrap server list
	Endpoint string `koanf:"endpoint"`
	// number of retries for kafka to be available before crashing
	ConnectRetries int64 `koanf:"connect_retries"`
	// prefixes to use when accesing topics in kafka
	TopicPrefix string `koanf:"topic_prefix"`
	// poll wait for standard consumer
	PollWaitEvents string `koanf:"poll_wait_events"`
	// poll wait for reprocessor
	PollWaitReprocessor string `koanf:"poll_wait_reprocessor"`
	// poll wait for any pipeline that reads direct from kafka
	PollWaitPipeline string `koanf:"poll_wait_pipeline"`
	// Max size of single message
	MessageMaxBytes bedsettings.HumanReadableBytes `koanf:"message_max_bytes"`
	// Fetch batch expected bytes (this is the minimum number of bytes backed up per consumer partition)
	// To save RAM make this number smaller e.g 500kB (NOTE - not too small or it causes re-negotiation issues for large messages)
	ConsumerFetchBatchBytes bedsettings.HumanReadableBytes `koanf:"consumer_fetch_batch_bytes"`
	// Number of network connection each consumer or producer communicating to kafka can open.
	NetworkConnectsPerAction int `koanf:"network_connections_per_action"`
	// Number of messages to be backed up in Sarama library, limiting this can save memory but reduce performance.
	ProducerInternalChannelSize int `koanf:"producer_queue_buffering_max_messages"`
	// Number of messages to be backed up in Sarama library, limiting this can save memory but reduce performance.
	ConsumerInternalChannelSize int `koanf:"consumer_queue_buffering_max_messages"`
	// Max messages before forcing producer to run
	ProducerMaxMessages int `koanf:"producer_max_messages"`
}

type DPEventReplayPluginCache struct {
	// seconds before produced events will be considered for caching
	MinRuntimeSeconds int `koanf:"min_runtime_seconds"`
	// Max cache size in bytes
	SizeBytes bedsettings.HumanReadableBytes `koanf:"size_bytes"`
	// Max TimeToLive for caches that support it -1 to disable
	TTLSeconds int `koanf:"ttl_seconds"`
}

type DPEventReprocess struct {
	// Specific topics to ignore. This generally shouldn't be used unless you have a specific use-case.
	DenyTopics string `koanf:"deny_topics"`
	// Kafka consumer group to use for reprocessing
	ConsumerGroup string `koanf:"consumer_group"`
	// Upgrade source topics between two prefixes
	UpgradeSources bool `koanf:"upgrade_sources"`
	// Upgrade system topics between two prefixes
	UpgradeSystem bool `koanf:"upgrade_system"`
	// Only include these system topics (comma separated)
	IncludeSystem string `koanf:"include_system"`
	// previous prefix to read from
	PreviousPrefix string `koanf:"previous_prefix"`
	// previos kafka topics stored in json format
	LegacyJson bool `koanf:"legacy_json"`
	// Maximum number of times to poll and get back no data before giving up on a topic (polling typically takes 1second)
	MaxNumberOfEmptyPolls int `koanf:"max_number_of_empty_polls"`
}

type DPRedis struct {
	Endpoint string `koanf:"endpoint"`
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}

type DPEvents struct {
	Reprocess                  DPEventReprocess         `koanf:"reprocess"`
	Kafka                      DPKafka                  `koanf:"kafka"`
	EnableExtendedKafkaMetrics bool                     `koanf:"enable_extended_kafka_metrics"`
	ReplayPluginCache          DPEventReplayPluginCache `koanf:"replay_plugin_cache"`
	// bytes to allocate for the dedupe cache
	DedupeCacheBytes bedsettings.HumanReadableBytes `koanf:"dedupe_cache_bytes"`
	// simple settings
	// If topic already exists with different replicas or partitions, raise error if this is false
	// as this requires manual action by a sysadmin to resolve.
	IgnoreTopicMismatch bool `koanf:"ignore_topic_mismatch"`
	// Default number of replicas (data redundancy) for topics if not overridden for a specific topic.
	GlobalReplicaCount int64 `koanf:"global_replica_count"`
	// Default number of partitions (max concurrency) for topics if not overridden for a specific topic.
	GlobalPartitionCount         int64  `koanf:"global_partition_count"`
	Sources                      string `koanf:"sources"`
	Topics                       string `koanf:"topics"`
	MaxConsumedEventsPerConsumer int64  `koanf:"max_consumed_events_per_consumer"`
	// APIEventFetchLimit is the max events a client can fetch in one call
	APIEventFetchLimit int `koanf:"api_event_fetch_limit"`
	// APIDefaultEventWait is the default seconds to wait to accumulate messages
	APIDefaultEventWait      int     `koanf:"api_default_event_wait"`
	Redis                    DPRedis `koanf:"redis"`
	LostTasksAfterMinutes    int64   `koanf:"lost_tasks_after_minutes"`
	LostTasksIntervalMinutes int64   `koanf:"lost_tasks_check_minutes"`
	LostTasksBulkCreateLimit int     `koanf:"lost_tasks_bulk_create_limit"`
}

type DPSettings struct {
	// restapi server will listen for connections from this address
	ListenAddr string `koanf:"listen_addr"`
	// for custom log files, the folder to place these file in
	LogPath string    `koanf:"log_path"`
	Streams DPStreams `koanf:"streams"`
	Events  DPEvents  `koanf:"events"`
}

var defaults DPSettings = DPSettings{
	ListenAddr: ":8111",
	LogPath:    "/tmp/logs/dispatcher/",
	Streams: DPStreams{
		APIAllowDelete: false,
		Backend:        "s3",
		S3: DPStreamsS3{
			Bucket: "azul",
		},
		Azure: DPStreamsAzure{
			Container: "azul",
		},
		Local: DPStreamsLocal{
			Path: "/tmp/store",
		},
		Cache: DPStreamCache{
			SizeBytes:  0,
			Shards:     32,
			TTLSeconds: 900,
		},
		FCache: DPStreamFileCache{
			Path: "/tmp/fcache",
		},
	},
	Events: DPEvents{
		Reprocess: DPEventReprocess{
			DenyTopics:            "",
			ConsumerGroup:         "azul-reprocessor",
			UpgradeSources:        false,
			UpgradeSystem:         false,
			IncludeSystem:         "delete,error,expedite,insert",
			PreviousPrefix:        "",
			LegacyJson:            false,
			MaxNumberOfEmptyPolls: 180, // 3minutes of failed polls before assuming topic is empty
		},
		ReplayPluginCache: DPEventReplayPluginCache{
			MinRuntimeSeconds: 10,
			SizeBytes:         0,
			TTLSeconds:        1800,
		},
		DedupeCacheBytes: 0, // cache is off by default
		Kafka: DPKafka{
			ConnectRetries:              10,
			Endpoint:                    "",
			TopicPrefix:                 "test01",
			PollWaitEvents:              "1ms",
			PollWaitPipeline:            "1s",
			PollWaitReprocessor:         "1s",
			MessageMaxBytes:             bedsettings.HumanToBytesFatal("3Mi"),
			ConsumerFetchBatchBytes:     bedsettings.HumanToBytesFatal("500Ki"),
			NetworkConnectsPerAction:    1,
			ProducerInternalChannelSize: 256,
			ConsumerInternalChannelSize: 256,
			ProducerMaxMessages:         100,
		},
		EnableExtendedKafkaMetrics: false,
		Redis: DPRedis{
			Endpoint: "",
			Username: "",
			Password: "",
		},
		IgnoreTopicMismatch:          true,
		GlobalReplicaCount:           1,
		GlobalPartitionCount:         1,
		Sources:                      "",
		Topics:                       "",
		MaxConsumedEventsPerConsumer: 10000,
		APIEventFetchLimit:           1000,
		APIDefaultEventWait:          5,
		LostTasksAfterMinutes:        30,
		LostTasksIntervalMinutes:     30,
		LostTasksBulkCreateLimit:     10,
	},
}

func setupLoggers(settings *DPSettings) {
	createFileLoggers(settings.LogPath)
	// Create File cache path if it doesn't exist
	if _, err := os.Stat(settings.Streams.FCache.Path); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(settings.Streams.FCache.Path, 0770)
			if os.IsExist(err) {
				log.Printf("Info Attempted to recreate path %s which already exists.", settings.Streams.FCache.Path)
			} else if err != nil {
				log.Fatalf("The file cache path '%s' could not be created with error: %s", settings.Streams.FCache.Path, err.Error())
			}
		} else {
			log.Fatalf("The file cache path '%s' exists but there is an error: %s", settings.Streams.FCache.Path, err.Error())
		}
	}
}

func ResetSettings() {
	Settings = bedsettings.ParseSettings(defaults, "DP", []mapstructure.DecodeHookFunc{bedsettings.HumanReadableBytesHookFunc()})
	setupLoggers(Settings)
	Streams = &Settings.Streams
	Events = &Settings.Events
}
