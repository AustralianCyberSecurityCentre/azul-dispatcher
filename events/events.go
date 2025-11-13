package events

import (
	"context"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/dedupe"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/manager"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_consume"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_dual"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_produce"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/tracking"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

var ctx = context.Background()

// wrap dispatcher resources in central location
type Events struct {
	prov    provider.ProviderInterface
	kvstore *kvprovider.KVMulti
	fstore  store.FileStorage
	// consumerManager for all Kafka consumers
	consumerManager *manager.ConsumerManager
	// producer is used to publish messages to Kafka
	producer *producer.Producer
	// injectChildEvents generates child events as needed
	injectChildEvents *pipeline_produce.InjectChildEvents
	// caches results published by active consumers
	replayPluginCompletion *pipeline_dual.ReplayPluginCompletion

	// filterDeleted tracks deleted messages
	filterDeleted *pipeline_dual.FilterPreviouslyDeleted

	simulateConsumerPipeline *pipeline.ConsumePipeline
}

func NewEvents(prov provider.ProviderInterface, kvstore *kvprovider.KVMulti, s store.FileStorage, ctx context.Context) *Events {
	filterMaxDepth := 10

	tracker, err := tracking.NewTaskTracker(kvstore.TrackPluginExecution, ctx)
	if err != nil {
		panic(err)
	}
	producer, err := producer.NewProducer(prov)
	if err != nil {
		panic(err)
	}

	filterDeleted, err := pipeline_dual.NewFilterDeleted(prov, producer)
	if err != nil {
		panic(err)
	}
	injectChildEvents, err := pipeline_produce.NewInjectChildEvents(prov, s)
	if err != nil {
		panic(err)
	}
	pipeAgeoff, err := pipeline_dual.NewPipelineAgeoff()
	if err != nil {
		panic(err)
	}

	// create simulation pipeline - should be same as passive pipe but without sideeffects pipes
	// this is used to provide feedback as to whether all consumers would process a specific event or not
	simulateConsumerPipe := pipeline.NewConsumePipeline([]pipeline.ConsumeAction{
		// Filters out messages that have a source path greater than the max depth.
		&pipeline_consume.FilterTooDeep{MaxDepth: filterMaxDepth + 1},
		// Uses a list of json filters provided through an API query to filter out un-desirable messages.
		&pipeline_consume.FilterConsumerRules{},
	}, s)

	// dedupe pipe should only be enabled if configured
	var filterDuplicatesPipe *pipeline_consume.FilterDuplicates = nil
	if st.Events.DedupeCacheBytes > 0 {
		dedupeCache := dedupe.New(uint64(st.Events.DedupeCacheBytes))
		filterDuplicatesPipe = &pipeline_consume.FilterDuplicates{Seen: dedupeCache}
	}
	// Filters out messages, sends notifications to Kafka about filtered messages and modifies the message format.
	passiveConsumerPipe := pipeline.NewConsumePipeline([]pipeline.ConsumeAction{
		// Filters out messages that are too old for the source
		pipeAgeoff,
		// Filters out messages that have a source path greater than the max depth.
		&pipeline_consume.FilterTooDeep{MaxDepth: filterMaxDepth + 1},
		// Uses a list of json filters provided through an API query to filter out un-desirable messages.
		&pipeline_consume.FilterConsumerRules{},
		// Filter out a message if it has already been deleted.
		filterDeleted,
		// Filter if this entity/src has already been consumed by the requesting plugin.
		// (useful if kafka consumer live == historic)
		filterDuplicatesPipe,
	}, s)

	var replayPluginCompletion *pipeline_dual.ReplayPluginCompletion = nil
	if st.Settings.Events.ReplayPluginCache.SizeBytes > 0 {
		replayPluginCompletion, err = pipeline_dual.NewReplayPluginCompletion(producer, s)
		if err != nil {
			panic(err)
		}
	}

	rds, err := pipeline_consume.NewRaiseDequeuedStatus(producer, tracker)
	if err != nil {
		panic(err)
	}

	// Filters out messages, sends notifications to Kafka about filtered messages and modifies the message format.
	activeConsumerPipe := pipeline.NewConsumePipeline([]pipeline.ConsumeAction{
		// Filters out messages that are too old for the source
		pipeAgeoff,
		// Filters out messages that have a source path greater than the max depth.
		&pipeline_consume.FilterTooDeep{MaxDepth: filterMaxDepth},
		// Uses a list of json filters provided through an API query to filter out un-desirable messages.
		&pipeline_consume.FilterConsumerRules{},
		// Filter out a message if it has already been deleted.
		filterDeleted,
		// Filter if this entity/src has already been consumed by the requesting plugin.
		// (useful if kafka consumer live == historic)
		filterDuplicatesPipe,
		// Publish a cached result if available, otherwise return the original event
		replayPluginCompletion,
		// Sends a status message to Kafka to notify that a task has been accepted by a plugin.
		// Used to confirm a corresponding completion of that task is received to determine if the plugin failed.
		rds,
	}, s)

	manager := manager.NewConsumerManager(prov, passiveConsumerPipe, activeConsumerPipe, kvstore)

	producePipe := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		// Filters out messages that are too old for the source
		pipeAgeoff,
		// Cache status events that took a long time to generate
		replayPluginCompletion,
		// Extract results from completed status
		pipeline_produce.NewProduceStatusResults(tracker),
		// Manual inserted children are injected here
		injectChildEvents,
		// set correct ids on events
		pipeline_produce.NewInjectId(),
		// Filter out a message if it has already been deleted.
		filterDeleted,
		// Remove the settings from the source if the depth of the model exceeds the depth limit.
		pipeline_produce.NewSourceSettingRemoval(),
	})
	producer.SetPipeline(producePipe)

	return &Events{
		prov:                     prov,
		kvstore:                  kvstore,
		fstore:                   s,
		consumerManager:          manager,
		producer:                 producer,
		injectChildEvents:        injectChildEvents,
		replayPluginCompletion:   replayPluginCompletion,
		filterDeleted:            filterDeleted,
		simulateConsumerPipeline: simulateConsumerPipe,
	}
}

func (ev *Events) InitialiseKafka() {
	var err error
	// if we can't connect to kafka, crash
	kc, err := topics.NewTopicControl(ev.prov)
	if err != nil {
		st.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}
	err = kc.EnsureAllTopics()
	if err != nil {
		st.Logger.Fatal().Err(err).Msg("could not contact kafka")
	}

	// some pipelines need to read existing events from kafka before we can start processing requests
	wg := sync.WaitGroup{}
	ev.filterDeleted.StartRoutine(&wg)
	ev.injectChildEvents.StartRoutine(&wg)
	wg.Wait()
}

// Stop various threads
func (ev *Events) Stop() {
	ev.filterDeleted.Stop()
	ev.injectChildEvents.Stop()
	ev.producer.Stop()
	ev.consumerManager.Stop()
}

// storeConsumerInRedis places the consumer in redis for a day
func (ev *Events) storeConsumerInRedis(p *consumer.ConsumeParams) error {
	// encode registration to json
	pluginOptions, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// only keep plugins that requested in last 24 hours
	ageoffAfter, err := time.ParseDuration("24h")
	if err != nil {
		return err
	}

	// update redis with parameters
	err = ev.kvstore.RegisteredPlugins.Set(ctx, p.GenerateKafkaPluginKey(), pluginOptions, ageoffAfter)
	if err != nil {
		return err
	}

	// inform workers that this specific plugin has been registered.
	if p.DeploymentKey != "" {
		deploymentConfig := models.DeployedPlugin{
			KafkaPrefix: p.GenerateKafkaPluginPrefix(),
		}
		encodedDeploymentConfig, err := json.Marshal(&deploymentConfig)
		if err != nil {
			return err
		}

		err = ev.kvstore.DeployedPlugins.Set(ctx, p.DeploymentKey, encodedDeploymentConfig, ageoffAfter)
		if err != nil {
			return err
		}
	}

	return nil
}

// getAllConsumersFromRedis returns all ConsumeParams stored in redis
func (ev *Events) getAllConsumersFromRedis() ([]consumer.ConsumeParams, error) {
	var err error
	var cursor uint64 = 0
	pluginKeys := []string{}
	for {
		var keys []string
		keys, cursor, err = ev.kvstore.RegisteredPlugins.Scan(ctx, cursor, "*", 1000)
		if err != nil {
			return nil, err
		}
		pluginKeys = append(pluginKeys, keys...)
		if cursor == 0 {
			break
		}
	}

	ret := []consumer.ConsumeParams{}
	for _, key := range pluginKeys {
		// fetch bytes and decode
		raw, err := ev.kvstore.RegisteredPlugins.GetBytes(ctx, key)
		if err != nil {
			return nil, err
		}
		consumerParam := consumer.ConsumeParams{}
		err = json.Unmarshal(raw, &consumerParam)
		if err != nil {
			return nil, err
		}
		ret = append(ret, consumerParam)
	}
	return ret, nil
}

// Perform simulation of binary event against all plugins registered in redis.
func (ev *Events) eventSimulate(event []byte) (*models.EventSimulate, error) {
	consumers, err := ev.getAllConsumersFromRedis()
	if err != nil {
		return nil, err
	}
	// call into pipeline with each redis plugin registered - if we get events out the other end, not filtered
	plugins := []models.EventSimulateConsumer{}
	for _, consumer := range consumers {
		inFlight, err := pipeline.NewMsgInFlightFromJson(event, events.ModelBinary)
		if err != nil {
			return nil, err
		}
		filters, events := ev.simulateConsumerPipeline.RunConsumeActions([]*msginflight.MsgInFlight{inFlight}, &consumer)
		filter_out := len(events) == 0
		filter_out_trigger := ""
		for k, v := range filters {
			if v > 0 {
				filter_out_trigger = k
			}
		}
		plugins = append(plugins, models.EventSimulateConsumer{
			Name:             consumer.Name,
			Version:          consumer.Version,
			FilterOut:        filter_out,
			FilterOutTrigger: filter_out_trigger,
		})
	}
	return &models.EventSimulate{Consumers: plugins}, nil
}
