package in_memory

import (
	"regexp"
	"testing"

	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestInMemory(t *testing.T) {
	mem := NewInMemory()

	mem.CreateTopic("topic1", 1)
	mem.CreateTopic("topic2", 1)

	var data *sarama_internals.Message
	mem.Push(&sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("data"), Partition: 0})
	mem.Push(&sarama.ProducerMessage{Topic: "topic2", Value: nil, Partition: 0})
	mem.Push(&sarama.ProducerMessage{Topic: "topic2", Value: sarama.StringEncoder(""), Partition: 0})

	var err error

	data, err = mem.Pop("consumer1", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("data"), data.Value)

	data, err = mem.Pop("consumer1", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data)

	data, err = mem.Pop("consumer2", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("data"), data.Value)

	data, err = mem.Pop("consumer2", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data)

	// push another and try to read
	mem.Push(&sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("mydata"), Partition: 0})

	data, err = mem.Pop("consumer1", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("mydata"), data.Value)

	data, err = mem.Pop("consumer1", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data)

	data, err = mem.Pop("consumer2", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("mydata"), data.Value)

	data, err = mem.Pop("consumer2", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data)

	// new consumer, earliest
	data, err = mem.Pop("consumer3", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("data"), data.Value)

	data, err = mem.Pop("consumer3", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("mydata"), data.Value)

	data, err = mem.Pop("consumer3", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data)

	// new consumer, only latest
	data, err = mem.Pop("consumer4", "topic1", 0, "latest")
	require.Nil(t, err)
	require.Nil(t, data)

	data, err = mem.Pop("consumer4", "topic1", 0, "latest")
	require.Nil(t, err)
	require.Nil(t, data)

	data, err = mem.Pop("consumer4", "topic1", 0, "latest")
	require.Nil(t, err)
	require.Nil(t, data)

	// push another and should now get on consumer 3 & 4
	mem.Push(&sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("c4yeah"), Partition: 0})
	data, err = mem.Pop("consumer3", "topic1", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, []byte("c4yeah"), data.Value)
	data, err = mem.Pop("consumer4", "topic1", 0, "latest")
	require.Nil(t, err)
	require.Equal(t, []byte("c4yeah"), data.Value)

	re, err := regexp.Compile("topic.*")
	require.Nil(t, err)
	topics := mem.MatchPattern(re)
	require.ElementsMatch(t, []string{"topic1", "topic2"}, topics)

	// test empty messages (nil message from tombstone, empty bytes from unknown origin)
	data, err = mem.Pop("consumer5", "topic2", 0, "earliest")
	require.Nil(t, err)
	require.Nil(t, data.Value)
	data, err = mem.Pop("consumer5", "topic2", 0, "earliest")
	require.Nil(t, err)
	require.Equal(t, data.Value, []byte{})
}
