//go:build integration

package integration_tests

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	prov "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMessage(ctx context.Context, t *testing.T, expected string) {
	// create event
	provider, err := prov.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
	require.Nil(t, err)
	producer, err := provider.CreateProducer()
	require.Nil(t, err)

	if err != nil {
		t.Errorf("bad %v", err)
		return
	}
	defer producer.Close()

	bedSet.Logger.Printf("create message")
	topic := "testing-sarama"
	bedSet.Logger.Printf("expecting content: %v", expected)
	err = producer.Produce(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(expected),
		Key:   sarama.StringEncoder("somedata"),
	}, true)
	if err != nil {
		t.Errorf("bad %v", err)
		return
	}
	bedSet.Logger.Printf("finished create message")
}

func TestSimpleConsumer(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	seed := fmt.Sprint(rand.Intn(10000000))
	group := "test-consumer"
	pattern := "testing-sarama"

	CreateTopic(ctx, pattern)
	time.Sleep(1500 * time.Millisecond)

	version := sarama.V3_0_0_0.String()
	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	// Version should always parse.
	require.Nil(t, err)

	c, err := sarama_internals.NewTrackingSaramaConsumer(ctx, []string{st.Events.Kafka.Endpoint}, kafkaVersion, group, group, pattern, "earliest", time.Time{})
	if err != nil {
		t.Errorf("failed to create kafka consumer %v", err)
		return
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Greater(collect, len(c.Claims), 0)
	}, 10*time.Second, 50*time.Millisecond)

	// Clear any messages in the topic
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	stillMore := true
	for stillMore {
		select {
		case _ = <-ticker.C:
			stillMore = false
			break
		case _, ok := <-c.DataChan:

			if !ok {
				t.Errorf("Error when getting sarama message.")
			}
		}
	}

	expected := `{"blah": "` + seed + `"}`
	createMessage(ctx, t, expected)

	ticker = time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	var msg *sarama_internals.Message
	var ok bool
	select {
	case _ = <-ticker.C:
		t.Error("Took longer than 1seconds to consume kafka message.")
		DeleteTopic(ctx, pattern)
		return
	case msg, ok = <-c.DataChan:
		if !ok {
			t.Errorf("Error when getting sarama message.")
		}
	}
	require.Equal(t, expected, string(msg.Value[:]))

	DeleteTopic(ctx, pattern)
}

func TestSimpleTracker(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	seed := fmt.Sprint(rand.Intn(10000000))
	group := "test-consumer"
	pattern := "testing-sarama"

	CreateTopic(ctx, pattern)
	time.Sleep(1500 * time.Millisecond)

	version := sarama.V3_0_0_0.String()
	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	// Version should always parse.
	require.Nil(t, err)
	c, err := sarama_internals.NewTrackingSaramaConsumer(ctx, []string{st.Events.Kafka.Endpoint}, kafkaVersion, group, group, pattern, "earliest", time.Time{})
	if err != nil {
		t.Errorf("failed to create kafka consumer %v", err)
		return
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Greater(collect, len(c.Claims), 0)
	}, 10*time.Second, 50*time.Millisecond)

	// Clear any messages in the topic
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	stillMore := true
	for stillMore {
		select {
		case _ = <-ticker.C:
			stillMore = false
			break
		case _, ok := <-c.DataChan:

			if !ok {
				t.Errorf("Error when getting sarama message.")
			}
		}
	}

	expected := `{"blah": "` + seed + `"}`
	createMessage(ctx, t, expected)

	ticker = time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	var msg *sarama_internals.Message
	var ok bool
	select {
	case _ = <-ticker.C:
		t.Error("Took longer than 1seconds to consume kafka message.")
		DeleteTopic(ctx, pattern)
		return
	case msg, ok = <-c.DataChan:
		if !ok {
			t.Errorf("Error when getting sarama message.")
		}
	}
	require.Equal(t, expected, string(msg.Value[:]))
	require.True(t, c.AreAllPartitionsCaughtUp())

	DeleteTopic(ctx, pattern)
}
