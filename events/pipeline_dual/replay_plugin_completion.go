package pipeline_dual

import (
	"context"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	fstore "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/allegro/bigcache/v3"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/metrics"
	"github.com/eko/gocache/lib/v4/store"
	bigcache_store "github.com/eko/gocache/store/bigcache/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const ERR_REPLAY_PLUGIN_COMPLETION = "error_replay_plugin_completion"

// GetCacheConfig returns a list of 0 or more configs from mapping cache environment vars.
func getCacheConfig() *CacheConfig {
	if st.Events.ReplayPluginCache.SizeBytes > 0 {
		return &CacheConfig{
			TTLSeconds: int(st.Events.ReplayPluginCache.TTLSeconds),
			SizeBytes:  int(st.Events.ReplayPluginCache.SizeBytes),
		}
	}
	return nil
}

var ctx = context.Background()

type CacheConfig struct {
	TTLSeconds int
	SizeBytes  int
}

type ReplayPluginCompletion struct {
	manager       cache.CacheInterface[[]byte]
	producer      pipeline.ProducerInterface
	store         fstore.FileStorage
	itemTTL       time.Duration
	produceParams pipeline.ProduceParams
	maxEventSize  int // maximum bytes for an event to be cached
}

// returns a new result cache object with the specified store configuration.
func NewReplayPluginCompletion(p pipeline.ProducerInterface, s fstore.FileStorage) (*ReplayPluginCompletion, error) {
	config := getCacheConfig()
	stores := []cache.SetterCacheInterface[[]byte]{}
	ttl := time.Second * time.Duration(-1)
	maxEventSize := -1

	if config != nil {
		ttl = time.Second * time.Duration(config.TTLSeconds)
		c := bigcache.DefaultConfig(ttl)
		c.HardMaxCacheSize = config.SizeBytes / 1048576 // in MB
		c.Verbose = false
		c.Shards = 64
		maxEventSize = (config.SizeBytes / c.Shards) - 1 // max size before will be rejected by bigcache
		ctx := context.Background()
		client, err := bigcache.New(ctx, c)
		if err != nil {
			return nil, err
		}
		cacheClient := bigcache_store.NewBigcache(client)
		stores = append(stores, cache.New[[]byte](cacheClient))
	}
	customRegistry := prometheus.NewRegistry()
	promMetrics := metrics.NewPrometheus("dispatcher", metrics.WithRegisterer(customRegistry))
	manager := cache.NewMetric(promMetrics, cache.NewChain(stores...))
	return &ReplayPluginCompletion{
		manager:       manager,
		producer:      p,
		store:         s,
		itemTTL:       ttl,
		produceParams: pipeline.ProduceParams{UserAgent: "replay_plugin_completion"},
		maxEventSize:  maxEventSize,
	}, nil
}

func (p *ReplayPluginCompletion) GetName() string { return "ReplayPluginCompletion" }

// conditionally adds a completed status event into the result cache.
func (p *ReplayPluginCompletion) ProduceMod(inFlight *msginflight.MsgInFlight, meta *pipeline.ProduceParams) (*msginflight.MsgInFlight, []*msginflight.MsgInFlight) {
	status, ok := inFlight.GetStatus()
	if !ok {
		return inFlight, nil
	}

	if !events.IsStatusTypeCompleted(status.Entity.Status) {
		return inFlight, nil
	}
	runtime := status.Entity.RunTime
	// cache result if over time cost threshold
	if runtime <= float64(st.Events.ReplayPluginCache.MinRuntimeSeconds) {
		return inFlight, nil
	}
	err := p.put(status)
	if err != nil {
		pipeline.HandleProducerError(meta.UserAgent, p.GetName(), inFlight, err, "error putting message")
	}
	return inFlight, nil
}

// publish a cached result if available, otherwise return the original event
func (p *ReplayPluginCompletion) ConsumeMod(message *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	binary, ok := message.GetBinary()
	// not binary or cache bypass
	if !ok || binary.Flags.BypassCache {
		return "", message
	}
	cached, err := p.fetch(binary, meta.Name, meta.Version)
	if err != nil {
		pipeline.HandleConsumerError(meta, p.GetName(), message, err, "fetch failed")
		return ERR_REPLAY_PLUGIN_COMPLETION, nil
	}
	if cached == nil {
		// cache miss
		return "", message
	}

	plugin_name := cached.Author.Name
	plugin_duration := cached.Entity.RunTime
	prom.ReplayCacheHitCount.WithLabelValues(plugin_name).Inc()
	prom.ReplayCacheHitSeconds.WithLabelValues(plugin_name).Add(plugin_duration)

	// found a cached result, a completed status message will be written to Kafka
	messages := formCacheCompletedEvents(binary, cached, time.Now().UTC())

	evs := []*msginflight.MsgInFlight{}
	for _, msg := range messages {
		// sources age data off differently so copy the artifact to the new source in file storage
		err = p.dupeDataSources(msg, cached)
		if err != nil {
			pipeline.HandleConsumerError(meta, p.GetName(), message, err, "copyCachedResults failed")
			return ERR_REPLAY_PLUGIN_COMPLETION, nil
		}

		ev, err := msginflight.NewMsgInFlightFromEvent(msg)
		if err != nil {
			pipeline.HandleConsumerError(meta, p.GetName(), message, err, "NewMsgInFlightFromEvent failed")
			return ERR_REPLAY_PLUGIN_COMPLETION, nil
		}
		evs = append(evs, ev)
	}

	// load similar to normal events
	to_publish, _ := p.producer.TransformMsgInFlights(evs, &p.produceParams)
	if err != nil {
		pipeline.HandleConsumerError(meta, p.GetName(), message, err, "TransformEvents failed")
		return ERR_REPLAY_PLUGIN_COMPLETION, nil
	}
	err = p.producer.ProduceAnyEvents(false, to_publish)
	if err != nil {
		pipeline.HandleConsumerError(meta, p.GetName(), message, err, "ProduceAnyEvents failed")
		return ERR_REPLAY_PLUGIN_COMPLETION, nil
	}
	// discard the message and the plugin won't reprocess the cached result
	return "cached", nil
}

func (c *ReplayPluginCompletion) put(ev *events.StatusEvent) error {
	input := ev.Entity.Input
	k := key(input.Entity.Sha256, ev.Author.Name, ev.Author.Version)
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if c.maxEventSize > 0 && len(raw) > c.maxEventSize {
		// too big for cache
		return nil
	}
	return c.manager.Set(ctx, k, raw, store.WithExpiration(c.itemTTL))
}

// attempts to retrieve a cached status result for the specified event and consumer.
func (c *ReplayPluginCompletion) fetch(ev *events.BinaryEvent, authorName, authorVersion string) (*events.StatusEvent, error) {
	k := key(ev.Entity.Sha256, authorName, authorVersion)
	val, err := c.manager.Get(ctx, k)
	if err != nil {
		// cache miss
		if strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var cached events.StatusEvent
	err = json.Unmarshal(val, &cached)
	if err != nil {
		return nil, err
	}
	return &cached, nil
}

// calculates a unique identifier for a given event and consumer.
func key(hash, plugin, version string) string {
	return strings.Join([]string{hash, plugin, version}, "-")
}

// dupeDataSources copies artifacts from a cached result into the new source's file store
func (c *ReplayPluginCompletion) dupeDataSources(message *events.BinaryEvent, cached *events.StatusEvent) error {
	// skip copy if the sources are the same
	if message.Source.Name == cached.Entity.Input.Source.Name {
		st.Logger.Debug().Msg("Skipping artifact copy as sources are the same")
		return nil
	}
	for _, r := range cached.Entity.Results {
		for _, data := range r.Entity.Datastreams {
			// perform an FileStorage artifact copy to the new source for the entity id
			err := c.store.Copy(cached.Entity.Input.Source.Name, data.Label.Str(), data.Sha256, message.Source.Name, data.Label.Str(), data.Sha256)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// takes an incoming 'trigger' event and merges with a 'cached' status event, replay events of same type as 'trigger'.
func formCacheCompletedEvents(trigger *events.BinaryEvent, cached *events.StatusEvent, now time.Time) []*events.BinaryEvent {
	results := []*events.BinaryEvent{}
	for i := range cached.Entity.Results {
		// make a copy of the struct for modification
		result := cached.Entity.Results[i]
		// set inherited flags
		// We don't care about inheriting bypass_cache=true as it is impossible to trigger a replay of a plugin
		// if the trigger event set bypass_cache=true
		result.Flags = events.BinaryFlags{Expedite: trigger.Flags.Expedite}
		// overwrite the entire cached source with source from incoming binary event
		result.Source = trigger.Source
		// append a copy of the cached last path
		last_cached_node := cached.Entity.Input.Source.Path[len(cached.Entity.Input.Source.Path)-1]
		// update timestamp on path
		last_cached_node.Timestamp = now
		result.Source.Path = append(result.Source.Path, last_cached_node)
		// update timestamps
		result.Timestamp = now
		// ensure the results are updated
		results = append(results, &result)
	}
	return results
}
