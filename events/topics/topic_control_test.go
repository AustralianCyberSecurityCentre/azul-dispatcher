package topics

import (
	"fmt"
	"slices"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"

	"github.com/stretchr/testify/require"
)

func TestInitialiseKafka(t *testing.T) {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	err = ac.EnsureAllTopics()
	require.Equal(t, nil, err)
	err = ac.DeleteAllTopics()
	require.Nil(t, err)
}

// Test topic created
func TestTopicCreated(t *testing.T) {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	// Create a testTopic
	topic := "testTopic"
	err = ac.createTopics(

		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}, st.Events.IgnoreTopicMismatch)
	if err != nil {
		t.Fatalf("Failed to create topic %v\n", err)
	}
	// Check topic is present
	meta, err := ac.Admin.GetMetadata(nil, true, 1000)
	if err != nil {
		t.Fatalf("Failed to fetch topics metadata %v\n", err)
	}
	var keys []string
	for _, currentTopic := range meta {
		keys = append(keys, currentTopic.Name)
	}
	require.True(t, slices.Contains(keys, topic))

	// Delete topic
	err = ac.Admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatalf("Failed to delete topic %v\n", err)
	}
}

// Test topic is only created once
func TestTopicCreatedOnce(t *testing.T) {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	// Create a testTopic
	topic := "azul.testTopic3"
	err = ac.createTopics(
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}, false)
	if err != nil {
		t.Fatalf("Failed to create topic %v\n", err)
	}

	// Try to create the topic a second time
	err = ac.createTopics(
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}, true)
	if err != nil {
		t.Fatalf("Failed to create topic the second time %v\n", err)
	}
	// Check topic is present
	meta, err := ac.Admin.GetMetadata(nil, true, 1000)
	if err != nil {
		t.Fatalf("Failed to fetch topics metadata %v\n", err)
	}
	var topicNames []string
	// Count using a map
	m := make(map[string]int)
	for _, currentTopic := range meta {
		topicNames = append(topicNames, currentTopic.Name)
		m[currentTopic.Name] = m[currentTopic.Name] + 1
	}
	fmt.Printf("%+v", topicNames)
	require.True(t, slices.Contains(topicNames, topic))
	require.Equal(t, 1, m[topic])

	// Delete topic
	err = ac.Admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatalf("Failed to delete topic %v\n", err)
	}
}

// Creating 2 topics with the same name, but different config should error.
func TestTopicErrorOnRecreate(t *testing.T) {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	// Create a testTopic
	topic := "testTopic2"
	err = ac.createTopics(
		// adminClient,
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}, st.Events.IgnoreTopicMismatch)
	if err != nil {
		t.Fatalf("Failed to create topic %v\n", err)
	}

	// Try to create the topic a second time with different config
	err = ac.createTopics(
		// adminClient,
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     2,
				ReplicationFactor: 2,
			},
		}, st.Events.IgnoreTopicMismatch)
	// Check an error is returned if getIgnoreTopicMismatch is not set
	if !st.Events.IgnoreTopicMismatch {
		require.NotEqual(t, nil, err)
	}

	// Check original topic is present
	meta, err := ac.Admin.GetMetadata(nil, true, 1000)
	if err != nil {
		t.Fatalf("Failed to fetch topics metadata %v\n", err)
	}
	var topicNames []string
	// Count using a map
	m := make(map[string]int)
	for _, currentTopic := range meta {
		topicNames = append(topicNames, currentTopic.Name)
		m[currentTopic.Name] = m[currentTopic.Name] + 1
	}
	require.True(t, slices.Contains(topicNames, topic))

	// Delete topic
	err = ac.Admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatalf("Failed to delete topic %v\n", err)
	}
}
