package manager

import (
	"context"
	"sync"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
)

var lastDeleteRequestTime time.Time
var deleteAllPluginsLock sync.Mutex

// ConsumerManager represents all Kafka interaction objects and supporting properties
type ConsumerManager struct {
	prov                   provider.ProviderInterface
	eventReaders           map[string]*eventReader
	consumersKeyCreateLock sync.RWMutex
	consumersDeleteLock    sync.RWMutex
	passiveMessagePipeline *pipeline.ConsumePipeline
	activeMessagePipeline  *pipeline.ConsumePipeline
	kvstore                *kvprovider.KVMulti
}

type FetchEvents func(query *consumer.ConsumeParams) ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error)

// NewConsumerManager returns a new instance of consumer manager that should be long-lived.
func NewConsumerManager(prov provider.ProviderInterface, passivePipeline *pipeline.ConsumePipeline, activePipeline *pipeline.ConsumePipeline, kvstore *kvprovider.KVMulti) *ConsumerManager {
	m := ConsumerManager{
		prov:                   prov,
		eventReaders:           make(map[string]*eventReader),
		passiveMessagePipeline: passivePipeline,
		activeMessagePipeline:  activePipeline,
		kvstore:                kvstore,
	}
	return &m
}

// Actively consuming events, means consumers may publish events derived from consumed events.
func (m *ConsumerManager) FetchEventsActive(p *consumer.ConsumeParams) ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error) {
	return m.fetchEvents(m.activeMessagePipeline, p)
}

// Passively consuming events, means consumers do not publish events derived from consumed events.
func (m *ConsumerManager) FetchEventsPassive(p *consumer.ConsumeParams) ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error) {
	return m.fetchEvents(m.passiveMessagePipeline, p)
}

// Only set pause time if it's a plugin and it's not paused because otherwise setting the time could cause
// new non-plugin consumers to skip topics ahead.
func (m *ConsumerManager) getLastPauseTimeAndDeletePluginConsumers(p *consumer.ConsumeParams) (bool, time.Time, error) {
	// If the process isn't a task it's not a plugin subscription so ignore pause time.
	if !p.IsTask {
		return false, time.Time{}, nil
	}
	isPluginProcessingPaused, err := pauser.IsPluginProcessingPaused(context.Background(), m.kvstore)
	if err != nil {
		bedSet.Logger.Err(err).Msg("Failed to check if plugin processing was paused when fetching events.")
		return false, time.Time{}, err
	}

	if !isPluginProcessingPaused {
		lastPauseTime, err := pauser.WhenWasLastPause(context.Background(), m.kvstore)
		return false, lastPauseTime, err
	}

	// Plugin processing is paused, now clear plugin consumers if there are still any remaining.
	if time.Since(lastDeleteRequestTime) > pauser.TIME_BETWEEN_REDIS_UPDATES {
		deleteAllPluginsLock.Lock()
		defer deleteAllPluginsLock.Unlock()
		// If condition still met after acquiring lock
		if time.Since(lastDeleteRequestTime) > pauser.TIME_BETWEEN_REDIS_UPDATES {
			lastDeleteRequestTime = time.Now()
			m.DeleteAllPluginEventReaders()
		}
	}
	return true, time.Time{}, err
}

// FetchEvents will return a set of events for the specified plugin, applying any supplied filters.
func (m *ConsumerManager) fetchEvents(messagePipeline *pipeline.ConsumePipeline, p *consumer.ConsumeParams) ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error) {
	bedSet.Logger.Trace().Str("name", p.Name).Str("version", p.Version).Bool("isTask", p.IsTask).Str("entityType", p.Model.Str()).
		Msg("fetch events")

	// Prevent plugin messages being read if a pause is in place.
	isPaused, lastPauseTime, err := m.getLastPauseTimeAndDeletePluginConsumers(p)
	if err != nil {
		return []*msginflight.MsgInFlight{}, nil, err
	} else if isPaused {
		// No messages available as plugin is paused.
		return []*msginflight.MsgInFlight{}, &models.EventResponseInfo{Ready: false, Paused: true}, nil
	}

	// Prevent the deletion of consumers until all event collection stops.
	m.consumersDeleteLock.RLock()
	defer m.consumersDeleteLock.RUnlock()

	prom.EventsConsumeRequests.WithLabelValues(p.Name, p.Model.Str()).Inc()

	c, err := m.getEventReader(p, lastPauseTime)

	if err != nil {
		return []*msginflight.MsgInFlight{}, nil, err
	}
	events, info := c.pull(messagePipeline, p)
	prom.EventsConsumeFetched.WithLabelValues(p.Name, p.Model.Str()).Add(float64(len(events)))
	return events, info, nil
}

// getEventReader will return the eventReader for the specified plugin, creating if it doesn't exist.
func (m *ConsumerManager) getEventReader(p *consumer.ConsumeParams, lastPauseTime time.Time) (*eventReader, error) {
	pluginKey := p.GenerateKafkaPluginKey()
	// obtain a read lock before obtaining an existing consumer
	m.consumersKeyCreateLock.RLock()
	c := m.eventReaders[pluginKey]
	m.consumersKeyCreateLock.RUnlock()
	if c != nil {
		bedSet.Logger.Trace().Str("name", p.Name).Str("version", p.Version).Str("entityType", p.Model.Str()).
			Msg("loaded existing event reader, first try")
		return c, nil
	}

	// consumer not exists so grab lock for creating the consumer
	m.consumersKeyCreateLock.Lock()
	defer m.consumersKeyCreateLock.Unlock()

	// check if the consumer was created before we got the lock
	c = m.eventReaders[pluginKey]
	if c != nil {
		bedSet.Logger.Trace().Str("name", p.Name).Str("version", p.Version).Str("entityType", p.Model.Str()).
			Msg("loaded existing event reader, second try")
		return c, nil
	}
	// consumer does not yet exist so we create it now
	// check if we can limit the topic subscription to just those entities with 'data'

	c, err := newEventReader(m.prov, pluginKey, p, lastPauseTime)
	if err != nil {
		return nil, err
	}
	m.eventReaders[pluginKey] = c
	bedSet.Logger.Trace().Str("name", p.Name).Str("version", p.Version).Str("entityType", p.Model.Str()).
		Msg("created new event reader")
	return c, nil
}

func (m *ConsumerManager) DeleteAllPluginEventReaders() {
	m.consumersDeleteLock.Lock()
	defer m.consumersDeleteLock.Unlock()
	survivingReaders := map[string]*eventReader{}
	for name, eventReader := range m.eventReaders {
		// If the eventreader has IsTask true it's a plugin and should be removed.
		if eventReader.FetchEventsParams.IsTask {
			eventReader.Stop()
		} else {
			survivingReaders[name] = eventReader
		}
	}
	m.eventReaders = survivingReaders
}

func (m *ConsumerManager) Stop() {
	for i := range m.eventReaders {
		bedSet.Logger.Info().Str("consumer", i).Msg("closing consumer")
		m.eventReaders[i].Stop()
	}
}
