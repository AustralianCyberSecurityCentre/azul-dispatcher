package provider

import (
	"context"
	"fmt"
	"log"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	stats "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
)

type SaramaKafkaProvider struct {
	bootstrap    string
	kafkaVersion sarama.KafkaVersion
	ctx          *context.Context
}

func NewSaramaProvider(bootstrap string, ctx context.Context) (*SaramaKafkaProvider, error) {
	bedSet.Logger.Info().Str("bootstrap", bootstrap).Msg("new consumer manager")

	version := sarama.V3_0_0_0.String()
	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Fatalf("Error parsing Kafka version: %v", err)
	}

	skc := SaramaKafkaProvider{bootstrap: bootstrap, kafkaVersion: kafkaVersion, ctx: &ctx}
	return &skc, nil
}

func (kp *SaramaKafkaProvider) CreateConsumer(consumerName, group, offset, pattern string, consumerOptions CreateConsumerOptions) (ConsumerInterface, error) {
	// Prefix added to allow two dispatchers with different prefixes to not collide even on the same kafka.
	groupWithPrefix := fmt.Sprintf("%s-%s", st.Events.Kafka.TopicPrefix, group)
	// Context to allow consumer to be closed, by itself or if the parent context closes.
	if kp.ctx == nil {
		return nil, fmt.Errorf("the SaramaProvider context is unset and is needed to create a new consumer")
	}
	ctx, cancel := context.WithCancel(*kp.ctx)

	consumer, err := sarama_internals.NewTrackingSaramaConsumer(ctx, []string{kp.bootstrap}, kp.kafkaVersion, consumerName, groupWithPrefix, pattern, offset, consumerOptions.LastPauseTime)
	conn := SaramaKafkaTrackingConsumer{Consumer: consumer, ctx: &ctx, cancel: cancel, pollWait: consumerOptions.PollWait}

	if err != nil {
		return nil, err
	}

	bedSet.Logger.Debug().Str("group", group).
		Str("offset", offset).
		Str("pattern", pattern).
		Msg("consumer subscribed to topics")
	return &conn, nil
}

func repeatablePartitioner(topic string) sarama.Partitioner {
	if topic == "access_log" || topic == "error_log" {
		return sarama.NewRandomPartitioner(topic)
	}
	return sarama.NewHashPartitioner(topic)
}

func (sp *SaramaKafkaProvider) CreateProducer() (ProducerInterface, error) {
	syncConfig := sarama.NewConfig()
	asyncConfig := sarama.NewConfig()

	// ensure we can publish larger messages than 1MB default
	asyncConfig.Producer.MaxMessageBytes = int(st.Events.Kafka.MessageMaxBytes)
	syncConfig.Producer.MaxMessageBytes = int(st.Events.Kafka.MessageMaxBytes)

	asyncConfig.Producer.Flush.MaxMessages = st.Events.Kafka.ProducerMaxMessages
	syncConfig.Producer.Flush.MaxMessages = st.Events.Kafka.ProducerMaxMessages

	asyncConfig.MetricRegistry = metrics.DefaultRegistry
	syncConfig.MetricRegistry = metrics.DefaultRegistry

	syncConfig.Metadata.Full = false
	asyncConfig.Metadata.Full = false

	syncConfig.ChannelBufferSize = st.Events.Kafka.ProducerInternalChannelSize
	asyncConfig.ChannelBufferSize = st.Events.Kafka.ProducerInternalChannelSize

	syncConfig.Net.MaxOpenRequests = st.Events.Kafka.NetworkConnectsPerAction
	asyncConfig.Net.MaxOpenRequests = st.Events.Kafka.NetworkConnectsPerAction

	syncConfig.Version = sp.kafkaVersion
	asyncConfig.Version = sp.kafkaVersion

	syncConfig.Producer.Compression = sarama.CompressionLZ4
	asyncConfig.Producer.Compression = sarama.CompressionLZ4

	// Configure partitioner for consistency purposes
	/*
		NewHashPartitioner returns a Partitioner which behaves as follows.
		If the message's key is nil then a random partition is chosen.
		Otherwise the FNV-1a hash of the encoded bytes of the message key is used,
		modulus the number of partitions.
		This ensures that messages with the same key always end up on the same partition.
	*/
	syncConfig.Producer.Partitioner = repeatablePartitioner
	asyncConfig.Producer.Partitioner = repeatablePartitioner
	// Provides a name for this kafka connection for logging debugging and auditing.
	syncConfig.ClientID = "saramaSyncProducer"
	asyncConfig.ClientID = "saramaAsyncProducer"

	// Channel configuration
	syncConfig.Producer.Return.Successes = true
	syncConfig.Producer.Return.Errors = true
	// For the AsyncProducer if you set to true you must consume the corresponding channel.
	// If you don't this causes memory leaks.
	asyncConfig.Producer.Return.Successes = false
	asyncConfig.Producer.Return.Errors = true

	asyncProd, err := sarama.NewAsyncProducer([]string{sp.bootstrap}, asyncConfig)
	if err != nil {
		return nil, err
	}
	syncProd, err := sarama.NewSyncProducer([]string{sp.bootstrap}, syncConfig)
	if err != nil {
		return nil, err
	}

	// Consume producer errors from the Async channel.
	// NOTE - this must be done to avoid memory leaks!
	go func() {
		for err := range asyncProd.Errors() {
			bedSet.Logger.Warn().Err(err).Msg("Error occurred in producer.")
			// Get all the asynchronous errors.
			stats.KafkaTransmitErrorCount.WithLabelValues(err.Msg.Topic).Inc()
		}
	}()

	return &SaramaKafkaProducer{
		asyncProducer: asyncProd,
		syncProducer:  syncProd,
	}, nil
}

func (kp *SaramaKafkaProvider) CreateAdmin() (AdminInterface, error) {
	config := sarama.NewConfig()
	config.Admin.Timeout = time.Duration(30) * time.Second
	config.Version = kp.kafkaVersion
	// Provides a name for this kafka connection for logging debugging and auditing.
	config.ClientID = "saramaAdminClient"
	config.MetricRegistry = metrics.DefaultRegistry
	config.Net.MaxOpenRequests = 1

	broker := sarama.NewBroker(kp.bootstrap)
	err := broker.Open(config)
	if err != nil {
		return nil, err
	}
	adminClient := SaramaKafkaAdmin{broker: broker, timeout: time.Duration(30) * time.Second}
	return &adminClient, nil
}

type SaramaKafkaTrackingConsumer struct {
	Consumer *sarama_internals.TrackingConsumer
	Group    string
	Pattern  string
	ctx      *context.Context
	cancel   context.CancelFunc
	pollWait time.Duration
}

/*Get the topics and partitions that are currently assigned to the consumer.*/
func (kc *SaramaKafkaTrackingConsumer) Assignment() map[string][]int32 {
	return kc.Consumer.Claims
}

/*Indicate if a Sarama Kafka consumer is ready to consume.*/
func (kc *SaramaKafkaTrackingConsumer) Ready() bool {
	return len(kc.Consumer.Claims) > 0
}

/*Read the newest entry from the consumer channel and wait for timeoutMs time and if no message is found return nil.*/
func (kc *SaramaKafkaTrackingConsumer) Poll() *sarama_internals.Message {
	ticker := time.NewTicker(kc.pollWait)
	defer ticker.Stop()
	select {
	case kafkaMessage, ok := <-kc.Consumer.DataChan:
		if !ok {
			bedSet.Logger.Error().Msgf("Unexpected err")
		}
		stats.KafkaReceiveMessageBytes.WithLabelValues(kc.Consumer.Name, kc.Consumer.Group, kc.Consumer.TopicRegex.String()).Add(float64(len(kafkaMessage.Value)))
		return kafkaMessage
	case <-ticker.C:
		return nil
	}
}

func (kc *SaramaKafkaTrackingConsumer) Chan() chan *sarama_internals.Message {
	return kc.Consumer.DataChan
}

func (kc *SaramaKafkaTrackingConsumer) AreAllPartitionsCaughtUp() bool {
	return kc.Consumer.AreAllPartitionsCaughtUp()
}

/*Close the current consumer so it stops reading messages from kafka.*/
func (kc *SaramaKafkaTrackingConsumer) Close() error {
	kc.cancel()
	return nil
}

type SaramaKafkaProducer struct {
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
}

func (sp *SaramaKafkaProducer) Produce(msg *sarama.ProducerMessage, confirm bool) error {
	// tombstone messages don't have a length
	if msg.Value != nil {
		stats.KafkaTransmitMessageBytes.WithLabelValues(msg.Topic).Add(float64(msg.Value.Length()))
	}
	if confirm {
		_, _, err := sp.syncProducer.SendMessage(msg)
		// Synchronous error count
		stats.KafkaTransmitErrorCount.WithLabelValues(msg.Topic).Inc()
		return err
	}
	sp.asyncProducer.Input() <- msg
	return nil
}

func (sp *SaramaKafkaProducer) Close() error {
	errAsync := sp.asyncProducer.Close()
	errSync := sp.syncProducer.Close()
	if errAsync != nil {
		return errAsync
	}
	return errSync
}

// --- Admin Interface implementation ---
type SaramaKafkaAdmin struct {
	broker  *sarama.Broker
	timeout time.Duration
}

func (sa *SaramaKafkaAdmin) GetMetadata(topic *string, allTopics bool, timeoutMs int) ([]*sarama.TopicMetadata, error) {
	// Get all topic metadata.
	request := sarama.MetadataRequest{
		Version: 10,  // 2.8+ (max version at writing)
		Topics:  nil, // Should give all topics with metadata attached.
	}
	resp, err := sa.broker.GetMetadata(&request)
	if err != nil {
		return []*sarama.TopicMetadata{}, err
	}
	return resp.Topics, nil
}
func (sa *SaramaKafkaAdmin) CreateTopics(topics []st.GenericKafkaTopicSpecification) error {
	/* Create provided topics with replication/partition count and name and additional config*/
	// *Broker.CreateTopics
	transformedTopics := make(map[string]*sarama.TopicDetail)
	for _, t := range topics {
		// Transform the replica assignments to be int32's
		transformedReplicaAssignment := make(map[int32][]int32)
		for i, val := range t.ReplicaAssignment {
			transformedReplicaAssignment[int32(i)] = val
		}
		// Transform the config from map[string]string to map[string]*string
		transformedConfig := make(map[string]*string)
		for key, val := range t.Config {
			transformedConfig[key] = &val
		}

		transformedTopics[t.Topic] = &sarama.TopicDetail{NumPartitions: int32(t.NumPartitions), ReplicationFactor: int16(t.ReplicationFactor), ReplicaAssignment: transformedReplicaAssignment, ConfigEntries: transformedConfig}
	}
	// sa.adminClient.CreateTopic("abc", &sarama.TopicDetail{}) // Alternative which only allows one topic at a time.
	resp, err := sa.broker.CreateTopics(&sarama.CreateTopicsRequest{Version: 4, TopicDetails: transformedTopics, Timeout: sa.timeout})
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("Failed to create topics.")
	}

	saramaHandleCreateTopicResult(resp)
	return nil
}
func (sa *SaramaKafkaAdmin) UpdateTopicPartitions(topicToNewPartitionCount map[string]int) {
	/*Increase the number of partitions a topic has*/
	// Not 100% sure maybe AlterConfig as well?   - CreatePartitions seems like a better choice
	topicPartitions := make(map[string]*sarama.TopicPartition)
	for topicName, partitionCount := range topicToNewPartitionCount {
		topicPartitions[topicName] = &sarama.TopicPartition{Count: int32(partitionCount)}
	}
	request := sarama.CreatePartitionsRequest{
		Version:         1, // 2.0.0+ (max version at writing)
		TopicPartitions: topicPartitions,
		Timeout:         sa.timeout,
	}
	response, err := sa.broker.CreatePartitions(&request)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("Failed to increase topic partitions")
	}
	saramaHandlePartitionUpdateResult(response)
}
func (sa *SaramaKafkaAdmin) UpdateTopicsConfig(topics []st.GenericKafkaTopicSpecification) {
	/*Update the current topic configuration  with any potential changes because of things like retention.ms and cleanup method delete/compaction*/
	// AlterConfig?  IncrementalAlterConfig?
	var laterConfigResource []*sarama.AlterConfigsResource
	for _, t := range topics {
		// Transform the config from map[string]string to map[string]*string
		transformedConfig := make(map[string]*string)
		for key, val := range t.Config {
			transformedConfig[key] = &val
		}

		laterConfigResource = append(laterConfigResource, &sarama.AlterConfigsResource{
			Type:          sarama.TopicResource,
			Name:          t.Topic,
			ConfigEntries: transformedConfig})
	}
	request := sarama.AlterConfigsRequest{
		Version:   1, // 2.0.0+ (max version at writing)
		Resources: laterConfigResource,
	}
	resp, err := sa.broker.AlterConfigs(&request)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msgf("Failed to update topic configurations %v", resp)
	}
	saramaHandleUpdateTopicResult(resp)
}

func (sa *SaramaKafkaAdmin) DescribeTopicConfig(topics []st.GenericKafkaTopicSpecification) []*sarama.ResourceResponse {
	/*Get information about the topics, used for testing*/
	var describeTopicList []*sarama.ConfigResource
	for _, t := range topics {
		describeTopicList = append(describeTopicList, &sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        t.Topic,
			ConfigNames: nil,
		})
	}
	req := sarama.DescribeConfigsRequest{
		Version:         2,
		Resources:       describeTopicList,
		IncludeSynonyms: true,
	}
	resp, err := sa.broker.DescribeConfigs(&req)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("Failed to describe topic configurations")
	}
	return resp.Resources

}

func (sa *SaramaKafkaAdmin) DeleteTopics(topicNames []string) error {
	/*Delete the provided topics by name.*/
	deleteRequest := sarama.DeleteTopicsRequest{Version: 3, Topics: topicNames, Timeout: sa.timeout}
	_, err := sa.broker.DeleteTopics(&deleteRequest)
	return err
}

func (sa *SaramaKafkaAdmin) Close() {
	/*Close all remaining connections.*/
	err := sa.broker.Close()
	if err != nil {
		bedSet.Logger.Err(err).Msg("error occurred when shutting down a sarama admin client.")
	}
}

func saramaHandleCreateTopicResult(results *sarama.CreateTopicsResponse) {
	// Check individual results
	for topicName, createResult := range results.TopicErrors {
		// If there is an error and the error isn't just that the topic already exists
		if createResult.Err != sarama.ErrNoError && createResult.Err != sarama.ErrTopicAlreadyExists {
			bedSet.Logger.Fatal().Msgf("Failed to create topic %s with error message %s", topicName, createResult.Error())
		}
	}
}

func saramaHandlePartitionUpdateResult(results *sarama.CreatePartitionsResponse) {
	// Check individual results
	for topicName, createResult := range results.TopicPartitionErrors {
		// If there is an error and the error isn't just that the topic already exists
		if createResult.Err != sarama.ErrNoError && createResult.Err != sarama.ErrTopicAlreadyExists {
			bedSet.Logger.Fatal().Msgf("Failed to modify partitions for topic %s with error message %s", topicName, createResult.Error())
		}
	}
}

func saramaHandleUpdateTopicResult(results *sarama.AlterConfigsResponse) {
	// Check individual results
	for _, rresult := range results.Resources {
		// If there is an error and the error isn't just that the topic already exists
		if sarama.KError(rresult.ErrorCode) != sarama.ErrNoError && sarama.KError(rresult.ErrorCode) != sarama.ErrTopicAlreadyExists {
			bedSet.Logger.Fatal().Msgf("Failed to update topic config %s with error message %s", rresult.Name, rresult.ErrorMsg)
		}
	}
}
