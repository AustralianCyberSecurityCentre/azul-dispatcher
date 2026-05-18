package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

var topic = ""

func main() {
	flag.StringVar(&topic, "topic", "", "topic for which events will be queried")
	flag.Parse()
	fmt.Println(topic)
	brokers := []string{"localhost:9092"}

	topicNames := []string{topic}
	var err error = nil
	if topic == "" {
		topicNames, err = ListTopics(brokers)
	}

	config := sarama.NewConfig()
	config.Net.DialTimeout = 10 * time.Second
	config.Metadata.Full = false
	config.Metadata.AllowAutoTopicCreation = false
	config.Consumer.Offsets.AutoCommit.Enable = false // does this work? seems like changing consumer group is the only way to re-consume events
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Second
	config.Metadata.RefreshFrequency = time.Minute
	config.Version, err = sarama.ParseKafkaVersion("4.2.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, _ := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, "asdf", config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	fmt.Println(topicNames)
	fmt.Println(err)

	consumer := Consumer{
		ready: make(chan bool),
	}

	err = client.Consume(ctx, topicNames, &consumer)
}

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func ListTopics(brokers []string) ([]string, error) {
	config := sarama.NewConfig()
	config.Admin.Timeout = time.Duration(30) * time.Second
	config.Net.MaxOpenRequests = 1
	// Provides a name for this kafka connection for logging debugging and auditing.
	config.ClientID = "saramaListAdmin"
	adminClient, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return []string{}, err
	}
	defer adminClient.Close()
	topicMap, err := adminClient.ListTopics()
	if err != nil {
		return []string{}, err
	}
	topicNames := make([]string, len(topicMap))
	i := 0
	for k := range topicMap {
		topicNames[i] = k
		i++
	}

	return topicNames, nil
}
