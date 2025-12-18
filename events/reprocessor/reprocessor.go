package reprocessor

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_produce"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
)

// The reprocessor takes existing events in a set of Kafka topics and reprocesses them.

// Determines mappings of which topics to reprocess from Kafka metadata.
func getTopicsForReprocess(allTopics []*sarama.TopicMetadata) ([]string, error) {
	denyList := strings.Split(st.Events.Reprocess.DenyTopics, ",")

	// Determine the profile of topics to select
	var topicMatchRegex string

	if st.Events.Reprocess.UpgradeSources && st.Events.Reprocess.UpgradeSystem {
		return nil, errors.New("cant upgrade sources and systems at the same time")
	}
	if st.Events.Reprocess.UpgradeSources {
		// example matches:
		// azul.dev01.hunt.binary
		// azul.dev01.hunt.binary.data
		// azul.dev01.samples.binary
		// azul.dev01.hunt.binary.extracted
		// azul.dev01.hunt.binary.enriched
		topicMatchRegex = fmt.Sprintf(`^azul\.%s\.[^\\.]+\.binary.*`, st.Events.Reprocess.PreviousPrefix)
	} else if st.Events.Reprocess.UpgradeSystem {
		includes := strings.Join(strings.Split(st.Events.Reprocess.IncludeSystem, ","), "|")
		// example match:
		// azul.dev01.system.expedite - actually a binary model
		// azul.dev01.system.retrohunt
		// azul.dev01.system.status
		// azul.dev01.system.filescan
		topicMatchRegex = fmt.Sprintf(`^azul\.%s\.system\.(%s)`, st.Events.Reprocess.PreviousPrefix, includes)
	} else {
		return nil, fmt.Errorf("no configured profile for reprocessor")
	}

	// Select topics to filter
	re, err := regexp.Compile(topicMatchRegex)
	if err != nil {
		return nil, err
	}

	// Filter topics based on the ignore list
	// The maximum number of topics is the count of all topics
	topics := make([]string, len(allTopics))
	validTopics := 0

TOPIC_FILTER:
	for _, topic := range allTopics {
		// Check to see if this topic is in the deny list
		for _, deny := range denyList {
			if deny == topic.Name {
				continue TOPIC_FILTER
			}
		}

		// Validate that the topic matches the compiled regex
		if re.MatchString(topic.Name) {
			topics[validTopics] = topic.Name
			validTopics += 1
		}
	}

	topics = topics[0:validTopics]

	// Preamble with the range of topics permitted
	for _, desiredTopic := range topics {
		bedSet.Logger.Info().Str("source", desiredTopic).Msg("configuring reprocessing topic")
	}

	return topics, nil
}

// Watches a worker channel to determine when all topics have reached EOF.
func watchForEOF(channel chan int, topicCount int) {
	// Watch for results from the channels to determine when all channels have
	// likely reached EOF
	channelSummary := make([]bool, topicCount)

RESULT_LOOP:
	for {
		id := <-channel
		channelSummary[id] = true

		// Determine if all channels are done
		for _, value := range channelSummary {
			if !value {
				continue RESULT_LOOP
			}
		}

		bedSet.Logger.Info().Int("reprocessor", -1).Bool("EOF", true).Msg("it appears that all reprocessors have reached the ends of their queues")
		return
	}
}

// Given a set of topics, spawns workers to handling the reprocessing of individual topics.
func spawnReprocessors(prov provider.ProviderInterface, topics []string, context context.Context) ([]Worker, chan int, error) {
	// Create a consumer and producer for each topic and spawn a routine
	resultChannel := make(chan int)
	producePipe := pipeline.NewProducePipeline([]pipeline.ProduceAction{
		// set correct ids on events
		pipeline_produce.NewInjectId(),
	})

	workers := make([]Worker, len(topics))

	for i, topic := range topics {
		bedSet.Logger.Info().Int("reprocessor", i).Str("source", topic).Msg("launching reprocessor")

		pollWait, err := time.ParseDuration(st.Events.Kafka.PollWaitReprocessor)
		if err != nil {
			return nil, nil, err
		}
		// Closed after context is cancelled.
		consumer, err := prov.CreateConsumer(st.Events.Reprocess.ConsumerGroup, "reprocess-earliest", "earliest", topic, provider.NewConsumerOptions(pollWait))
		if err != nil {
			return nil, nil, err
		}
		// Closed after context is cancelled.
		// this producer must allow for json messages to be upgraded
		producer, err := producer.NewProducer(prov)
		if err != nil {
			return nil, nil, err
		}

		producer.SetPipeline(producePipe)

		workers[i] = NewWorker(topic, consumer, producer, i, resultChannel, prom.ReprocessorEventCount.WithLabelValues(topic), prom.ReprocessorEOF.WithLabelValues(topic))

		go workers[i].Run(context)
	}

	return workers, resultChannel, nil
}

// Runs the reprocessor. Configuration is provided by settings. Will run indefinitely unless
// shutdownOnEOF is specified.
func Start(prov provider.ProviderInterface, kvprov *kvprovider.KVMulti, skipSleep bool, shutdownOnEOF bool) error {
	ac, err := topics.NewTopicControl(prov)
	if err != nil {
		return err
	}
	err = ac.EnsureAllTopics()
	if err != nil {
		return err
	}

	allTopics, err := ac.GetAllTopics()
	if err != nil {
		return err
	}

	topics, err := getTopicsForReprocess(allTopics)
	if err != nil {
		return err
	}

	topicCount := len(topics)

	if topicCount == 0 {
		return fmt.Errorf("no topics found to reprocess")
	}

	// Sleep to allow the user to follow along. Reprocessing shouldn't be done without
	// a user watching the system given the risk of data loss/etc.
	if skipSleep {
		bedSet.Logger.Warn().Int("count", topicCount).Msg("starting reprocessing immediately")
	} else {
		bedSet.Logger.Info().Int("count", topicCount).Msg("starting reprocessing in 30 seconds")
		time.Sleep(30 * time.Second)
	}

	context, cancel := context.WithCancel(context.Background())
	// Pause all plugin processing until reprocessing is completed.
	pauseWaitGroup := pauser.PauseUntilContextDone(context, kvprov)
	workers, resultChannel, err := spawnReprocessors(prov, topics, context)
	if err != nil {
		cancel()
		return err
	}

	// Watch for EOF and print a message when all reprocessors have reached EOF.
	watchForEOF(resultChannel, topicCount)

	if shutdownOnEOF {
		// Sleep to give time for the last messages that need to be added to the channel for publishing.
		time.Sleep(5 * time.Second)

		// Cancel all workers
		cancel()
	} else {
		// We want to run infinitely as this might be run in parallel with a running instance
		// of Azul where events continue to stream in while reprocessing occurs.
		for {
			<-resultChannel
		}
	}

	pauseWaitGroup.Wait()

	totalProcessed := 0
	for _, worker := range workers {
		totalProcessed += worker.processed
	}
	bedSet.Logger.Info().Int("processed", totalProcessed).Msg("finished reprocess")

	return nil
}
