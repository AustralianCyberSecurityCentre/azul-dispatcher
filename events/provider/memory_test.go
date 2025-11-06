package provider

import (
	"testing"
	"time"

	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"

	"github.com/stretchr/testify/require"
)

func TestMemoryProvider(t *testing.T) {
	topic1 := "topic1"
	topic2 := "topic2"
	prov, err := NewMemoryProvider()
	require.Nil(t, err)

	ac, err := prov.CreateAdmin()
	require.Nil(t, err)

	// create some topics
	err = ac.CreateTopics([]st.GenericKafkaTopicSpecification{{Topic: topic1}, {Topic: topic2}})
	require.Nil(t, err)

	// check topics exist
	meta, err := ac.GetMetadata(nil, true, 10)
	require.Nil(t, err)
	expected := []string{topic1, topic2}
	for _, topic := range meta {
		require.Contains(t, expected, topic.Name)
	}

	p, err := prov.CreateProducer()
	require.Nil(t, err)
	producerMeta, err := ac.GetMetadata(nil, true, 1000)
	require.Nil(t, err)
	expected = []string{topic1, topic2}
	for _, topic := range producerMeta {
		require.Contains(t, expected, topic.Name)
	}

	// publish a message
	msg := sarama.ProducerMessage{
		Topic:     topic1,
		Partition: 0,
		Value:     sarama.StringEncoder("test1"),
		Key:       sarama.StringEncoder("id"),
	}
	err = p.Produce(&msg, true)
	require.Nil(t, err)
	msg = sarama.ProducerMessage{
		Topic:     topic2,
		Partition: 0,
		Value:     sarama.StringEncoder("test2"),
		Key:       sarama.StringEncoder("id"),
	}
	err = p.Produce(&msg, true)
	require.Nil(t, err)

	// read a message
	c, err := prov.CreateConsumer("name1", "group1", "earliest", "topic.*", NewConsumerOptions(time.Second))
	require.Nil(t, err)

	require.True(t, c.Ready())

	topics := c.Assignment()
	expected = []string{topic1, topic2}
	for topic := range topics {
		require.Contains(t, expected, topic)
	}

	ev := c.Poll()
	require.NotNil(t, ev)
	expectedB := [][]byte{[]byte("test1"), []byte("test2")}
	require.NotNil(t, ev)
	require.Contains(t, expectedB, ev.Value)

	ev = c.Poll()
	require.NotNil(t, ev)
	expectedB = [][]byte{[]byte("test1"), []byte("test2")}
	require.NotNil(t, ev)
	require.Contains(t, expectedB, ev.Value)

	ev = c.Poll()
	require.Equal(t, (*sarama_internals.Message)(nil), ev)
}
