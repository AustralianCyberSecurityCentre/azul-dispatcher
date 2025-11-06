package saramago

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	stats "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
)

// Extension of the existing Sarama ConsumerGroupHandler to enable the creation of different types of Sarama consumers.
type customSaramaConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	GetName() string
	GetGroup() string
	GetBrokers() []string
	// Get a list of topics for a consumer (can query kafka so if topics aren't found immediately query should be retried.)
	GetTopics() []string
}

// Start a Sarama based consumer that implements the CustomSaramaConsumerGroupHandler.
// Should be part of the New/Create method of the implementation to ensure it is only started once
// (as multiple starts cause more and more go routines to be started).
func startSaramaConsumer(ctx context.Context, kafkaVersion sarama.KafkaVersion, offset string, consumer customSaramaConsumerGroupHandler) error {
	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.ChannelBufferSize = st.Events.Kafka.ConsumerInternalChannelSize
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	config.Metadata.Full = false                                          // Saves some memory
	config.Net.MaxOpenRequests = st.Events.Kafka.NetworkConnectsPerAction // Limit number of concurrent connections because there shouldn't need to be lots.
	config.MetricRegistry = metrics.DefaultRegistry
	// Provides a name for this kafka connection for logging debugging and auditing.
	config.ClientID = "saramaConsumer"
	config.Consumer.Fetch.Default = int32(st.Events.Kafka.ConsumerFetchBatchBytes)
	config.Consumer.Fetch.Max = int32(st.Events.Kafka.MessageMaxBytes)

	// Set the appropriate offset.
	offset = strings.ToLower(offset)
	switch offset {
	case "earliest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		st.Logger.Error().Str("offset", offset).Msg("Unexpected offset valid values are earliest and latest.")
	}
	client, err := sarama.NewConsumerGroup(consumer.GetBrokers(), consumer.GetGroup(), config)
	if err != nil {
		st.Logger.Error().Err(err).Msg("Error creating consumer group client.")
		return fmt.Errorf("error creating consumer group client: %v", err)
	}

	topicList := consumer.GetTopics()

	// Go routine that continually runs the sarama consumer until the context is cancelled.
	go func() {
		for {
			// Get a list of all topics that this consumer should consume, this can return an empty list in a couple
			// of circumstances (topic just created during testing and kafka cluster is unresponsive.)
			// This is why it is continually checked in a loop with a delay.
			// There must be more than one topic to begin consuming (i.e run the command sClient.Consume())
			for len(topicList) == 0 && ctx.Err() == nil {
				time.Sleep(10 * time.Second)
				topicList = consumer.GetTopics()
			}
			if ctx.Err() != nil {
				return
			}
			// `Consume` is called inside an infinite loop to allow for server-side rebalance.
			// This is what is required according to the Sarama documentation.
			// Disabled as too noisy.
			// st.Logger.Info().Msgf("Starting consume for %v and group %v", topicList, consumer.GetGroup())
			if err := client.Consume(ctx, topicList, consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				if errors.Is(err, sarama.ErrRebalanceInProgress) {
					stats.KafkaRebalanceCount.WithLabelValues(consumer.GetName(), consumer.GetGroup()).Inc()
				}
				st.Logger.Info().Err(err).Msgf("Consumer %s has errored and will retry.", consumer.GetGroup())

				// Capture kafka error codes in stats, to allow for the errors to be identified.
				err2, ok := err.(sarama.KError)
				// Not a kafka error code.
				kcode := int64(100)
				if ok {
					kcode = int64(err2)
				}
				stats.KafkaConsumerResetCount.WithLabelValues(consumer.GetName(), consumer.GetGroup(), strconv.FormatInt(kcode, 10)).Inc()
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}
