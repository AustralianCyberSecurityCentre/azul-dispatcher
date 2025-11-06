package provider

import (
	"time"

	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
)

type CreateConsumerOptions struct {
	PollWait      time.Duration
	LastPauseTime time.Time
}

func NewConsumerOptions(pollWait time.Duration) CreateConsumerOptions {
	return CreateConsumerOptions{
		LastPauseTime: time.Time{},
		PollWait:      pollWait,
	}
}

type ProviderInterface interface {
	CreateConsumer(consumerName, group, offset, pattern string, additionalOptions CreateConsumerOptions) (ConsumerInterface, error)
	CreateProducer() (ProducerInterface, error)
	CreateAdmin() (AdminInterface, error)
}

type ConsumerInterface interface {
	Assignment() map[string][]int32
	Ready() bool
	// Return a message if one is ready and if it isn't returns a nil value after the timeout provided.
	Poll() *sarama_internals.Message
	Chan() chan *sarama_internals.Message
	Close() error
	// Return true if all partitions are caught up to live. (Note - will always return false if assignment hasn't finished yet)
	AreAllPartitionsCaughtUp() bool
}

type ProducerInterface interface {
	Produce(msg *sarama.ProducerMessage, confirm bool) error
	Close() error
}

type AdminInterface interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) ([]*sarama.TopicMetadata, error)
	CreateTopics(topics []st.GenericKafkaTopicSpecification) error
	UpdateTopicPartitions(topicToNewPartitionCount map[string]int)
	UpdateTopicsConfig(topics []st.GenericKafkaTopicSpecification)
	DescribeTopicConfig(topics []st.GenericKafkaTopicSpecification) []*sarama.ResourceResponse
	DeleteTopics(topicNames []string) error
	Close()
}
