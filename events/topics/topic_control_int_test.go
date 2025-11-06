//go:build integration

package topics

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

func TestIntInitialiseKafka(t *testing.T) {
	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	require.Nil(t, err)
	ac, err := NewTopicControl(prov)
	require.Nil(t, err)
	err = ac.EnsureAllTopics()
	require.Nil(t, err)
	err = ac.DeleteAllTopics()
	require.Nil(t, err)
}

// Test topic created
func TestIntTopicCreated(t *testing.T) {
	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
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
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)
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
func TestIntTopicCreatedOnce(t *testing.T) {
	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
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
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(2000 * time.Millisecond)
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
func TestIntTopicErrorOnRecreate(t *testing.T) {
	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
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
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)

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
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1000 * time.Millisecond)
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

func TestIntTopicNoErorrOnPartitionRecreate(t *testing.T) {
	topic := "testTopic3"

	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()
	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}

	// Ensure topic doesn't exist by Deleting topic
	_ = ac.Admin.DeleteTopics([]string{topic})

	// Sleep as topic deletion is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)

	// Create a testTopic
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
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)

	// Try to create the topic a second time with different config
	err = ac.createTopics(
		// adminClient,
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     2,
				ReplicationFactor: 1,
			},
		}, st.Events.IgnoreTopicMismatch)
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1000 * time.Millisecond)

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
		// Ensure that the partition count increased.
		if topic == currentTopic.Name {
			require.Equal(t, 2, len(currentTopic.Partitions))
		}
	}
	require.True(t, slices.Contains(topicNames, topic))

	// Delete topic
	err = ac.Admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatalf("Failed to delete topic %v\n", err)
	}
}

func TestTopicModifyConifg(t *testing.T) {
	topic := "testTopic4"

	testdata.InitGlobalContext()
	defer testdata.CloseGlobalContext()

	prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, testdata.GetGlobalTestContext())
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	ac, err := NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}

	// Ensure topic doesn't exist by Deleting topic
	_ = ac.Admin.DeleteTopics([]string{topic})

	// Sleep as topic deletion is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)

	// Create a testTopic
	originalConfig := map[string]string{
		"retention.ms": "2419200000",
	}
	newConfig := map[string]string{
		"retention.ms": "-1",
	}
	err = ac.createTopics(
		// adminClient,
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
				Config:            originalConfig,
			},
		}, st.Events.IgnoreTopicMismatch)
	if err != nil {
		t.Fatalf("Failed to create topic %v\n", err)
	}
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1500 * time.Millisecond)

	// Try to create the topic a second time with different config
	err = ac.createTopics(
		// adminClient,
		[]st.GenericKafkaTopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     2,
				ReplicationFactor: 1,
				Config:            newConfig,
			},
		}, st.Events.IgnoreTopicMismatch)
	// Sleep as topic creation is async, wait before fetching topics list
	time.Sleep(1000 * time.Millisecond)

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
		// Ensure that the partition count increased.
		if topic == currentTopic.Name {
			require.Equal(t, 2, len(currentTopic.Partitions))
			description := ac.Admin.DescribeTopicConfig([]st.GenericKafkaTopicSpecification{{
				Topic:             topic,
				NumPartitions:     2,
				ReplicationFactor: 1,
			}})
			nameValue := make(map[string]string)
			for _, entry := range description[0].Configs {
				nameValue[entry.Name] = entry.Value
			}
			val, ok := nameValue["retention.ms"]
			// Verify value is in map.
			require.True(t, ok)
			require.Equal(t, "-1", val)
		}
	}
	require.True(t, slices.Contains(topicNames, topic))

	// Delete topic
	err = ac.Admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatalf("Failed to delete topic %v\n", err)
	}
}
