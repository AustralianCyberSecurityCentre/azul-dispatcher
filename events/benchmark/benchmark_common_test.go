package events_benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

type Benchmarker struct {
	prov          provider.ProviderInterface
	cancel        context.CancelFunc
	testTopicName string
}

const CONSUMER_SETUP_TIMEOUT = time.Duration(20) * time.Second
const CONSUMER_POLL_DURATION = time.Duration(100) * time.Millisecond
const MAX_POLL_ATTEMPTS = 50 // 50 * 100 milliseconds is 5000milliseconds (5 seconds)

// Produce the required number of messages for benchmarker.
func produceNMessages(t *testing.B, s *Benchmarker) {
	producer, err := s.prov.CreateProducer()
	defer producer.Close()
	require.Nil(t, err)

	t.StopTimer()

	// Generate bulk events prior to sending them to kafka.
	eventsToPublish := []*sarama.ProducerMessage{}
	for n := range t.N + 1 {
		binaryEvent := testdata.GenEventBinary(&testdata.BC{ID: fmt.Sprintf("sha256ofmessage%d", n), Action: events.ActionSourced, PresetFeatures: 40})
		binaryBytes, err := binaryEvent.ToAvro()
		require.Nil(t, err)
		randomMessage := &sarama.ProducerMessage{
			Topic: s.testTopicName,
			Key:   sarama.StringEncoder(binaryEvent.Entity.Sha256),
			Value: sarama.ByteEncoder(binaryBytes),
		}
		eventsToPublish = append(eventsToPublish, randomMessage)
	}

	// Produce the first random message to avoid publisher setup during timing.
	producer.Produce(eventsToPublish[0], true)

	t.ResetTimer()
	t.StartTimer()
	// Produce all of the generated events.
	for _, randomMessage := range eventsToPublish {
		err := producer.Produce(randomMessage, true)
		require.Nil(t, err)
	}
	t.StopTimer()
}

// Setup the channel and polling consumer by first establishing latest then producing and consuming a single message.
func setupConsumers(t *testing.B, s *Benchmarker, c provider.ConsumerInterface) {
	producer, err := s.prov.CreateProducer()
	require.Nil(t, err)
	defer producer.Close()

	start := time.Now()
	for {
		c.Poll()
		if c.Ready() {
			c.Poll()
			break
		}
		if time.Since(start) > CONSUMER_SETUP_TIMEOUT {
			// This typically occurs when the consumer couldn't connect to it's consumer group fast enough or can't get any partitions assigned.
			require.True(t, false, "Assignment took too long, during consumer setup %v", c.Assignment())
		}
	}
}

// Repetitively poll until you get a message or hit the max attempts
// Note this was added as it makes test times 40times faster in some cases.
func repetitivePollForMsg(c provider.ConsumerInterface) *sarama_internals.Message {
	var msg *sarama_internals.Message
	for _ = range MAX_POLL_ATTEMPTS {
		msg = c.Poll()
		if msg == (*sarama_internals.Message)(nil) {
			continue
		}
		break
	}
	return msg
}

// Consume benchmark number of messages using the consumer Poll interface
func benchmarkConsumerPoll(t *testing.B, s *Benchmarker) {
	t.StopTimer()
	consumer, err := s.prov.CreateConsumer(
		"poll-consumer",
		"test-benchmark-consume-poll",
		"latest",
		s.testTopicName,
		provider.CreateConsumerOptions{PollWait: CONSUMER_POLL_DURATION},
	)
	require.Nil(t, err)
	defer consumer.Close()
	// Ensure consumer is ready for messages
	setupConsumers(t, s, consumer)

	// Create all messages
	produceNMessages(t, s)
	produceNMessages(t, s)

	// untimed consumption of events to remove some of the caching inconsistencies.
	var msg *sarama_internals.Message
	for _ = range t.N {
		msg = repetitivePollForMsg(consumer)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}

	t.ResetTimer()
	t.StartTimer()
	start := time.Now()
	// Poll all the messages that producer put in.
	for _ = range t.N {
		msg = repetitivePollForMsg(consumer)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}
	duration := time.Since(start)
	t.StopTimer()
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(duration.Seconds()), "events/s")
}

// Consume benchmark number of messages using the consumer Channel interface
func benchmarkConsumerChannel(t *testing.B, s *Benchmarker) {
	t.StopTimer()
	consumer, err := s.prov.CreateConsumer(
		"channel-consumer",
		"test-benchmark-consume-channel",
		"latest",
		s.testTopicName,
		provider.CreateConsumerOptions{PollWait: CONSUMER_POLL_DURATION},
	)
	require.Nil(t, err)
	defer consumer.Close()
	// Establish latest, by polling multiple times and waiting for poll timeout
	setupConsumers(t, s, consumer)

	// Create all messages
	produceNMessages(t, s)
	produceNMessages(t, s)

	// untimed consumption of events to remove some of the caching inconsistencies.
	var msg *sarama_internals.Message
	for _ = range t.N {
		msg = repetitivePollForMsg(consumer)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}

	t.ResetTimer()
	t.StartTimer()
	start := time.Now()
	// Poll all the messages that producer put in.
	for _ = range t.N {
		msg := <-consumer.Chan()
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}
	t.StopTimer()
	duration := time.Since(start)
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(duration.Seconds()), "events/s")
}

func benchmarkMultiConsumer(t *testing.B, s *Benchmarker) {
	t.StopTimer()
	consumerA, err := s.prov.CreateConsumer(
		"channel-A",
		"test-multi",
		"latest",
		s.testTopicName,
		provider.CreateConsumerOptions{PollWait: CONSUMER_POLL_DURATION},
	)
	require.Nil(t, err)
	defer consumerA.Close()

	consumerB, err := s.prov.CreateConsumer(
		"channel-B",
		"test-multi",
		"latest",
		s.testTopicName,
		provider.CreateConsumerOptions{PollWait: CONSUMER_POLL_DURATION},
	)
	require.Nil(t, err)
	defer consumerB.Close()

	// Establish latest, by polling multiple times and waiting for poll timeout
	setupConsumers(t, s, consumerA)
	setupConsumers(t, s, consumerB)

	// Create all messages (produce double to make sure they spread out across partitions enough )
	produceNMessages(t, s)
	produceNMessages(t, s)
	produceNMessages(t, s)

	var msg *sarama_internals.Message
	// untimed consume run to remove caching inconsistencies
	for _ = range t.N / 2 {
		msg = repetitivePollForMsg(consumerA)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
		msg = repetitivePollForMsg(consumerB)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}

	t.ResetTimer()
	t.StartTimer()

	start := time.Now()

	// Poll all the messages (noting your polling two consumer so only need to do half the iterations)
	for _ = range t.N / 2 {
		msg = repetitivePollForMsg(consumerA)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
		msg = repetitivePollForMsg(consumerB)
		require.NotEqual(t, msg, (*sarama_internals.Message)(nil))
	}
	duration := time.Since(start)
	t.StopTimer()

	t.ReportMetric(float64(t.N)/float64(duration.Seconds()), "events/s")
}
