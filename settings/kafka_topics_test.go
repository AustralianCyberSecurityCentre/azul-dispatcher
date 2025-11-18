package settings

import (
	"testing"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/stretchr/testify/require"
)

func testKeyTopics(topics []GenericKafkaTopicSpecification) map[string]GenericKafkaTopicSpecification {
	keyed := map[string]GenericKafkaTopicSpecification{}
	for i := range topics {
		keyed[topics[i].Topic] = topics[i]
	}
	return keyed
}

// Tests reading of topics from a yaml file
func TestReadTopicsFromYAML(t *testing.T) {
	ac := Topics{}

	// Example YAML content for testing
	yamlContent := `
- topic: azul.test_topic1
  numpartitions: 3
  replicationfactor: 1
- topic: azul.test_topic2
`
	// FUTURE Set the environment variable for testing
	Events.Topics = yamlContent
	// Call the function to read topics from YAML
	topics, err := ac.readTopicsFromYAML(Events.Topics)
	if err != nil {
		t.Fatalf("Error reading topics from YAML: %v", err)
	}
	// Check if the correct number of topics is read
	require.Equal(t, len(topics), 2)

	// Check the values of the first topic
	keyed := testKeyTopics(topics)
	require.Equal(t, keyed["azul.test_topic1"], GenericKafkaTopicSpecification{
		Topic: "azul.test_topic1", NumPartitions: 3, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact"},
	})
	require.Equal(t, keyed["azul.test_topic2"], GenericKafkaTopicSpecification{
		Topic: "azul.test_topic2", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact"},
	})
}

// Test merging of topic lists
func TestMergeTopicLists(t *testing.T) {
	ac := Topics{}
	base := []GenericKafkaTopicSpecification{
		{Topic: "topic1", NumPartitions: 1},
		{Topic: "topic2", NumPartitions: 2},
	}
	newList := []GenericKafkaTopicSpecification{
		{Topic: "topic1", NumPartitions: 2, ReplicationFactor: 2},
		{Topic: "topic3", NumPartitions: 3},
	}

	err := ac.mergeTopicLists(&base, newList)
	require.NoError(t, err)

	expected := []GenericKafkaTopicSpecification{
		{Topic: "topic1", NumPartitions: 2, ReplicationFactor: 2},
		{Topic: "topic2", NumPartitions: 2},
		{Topic: "topic3", NumPartitions: 3},
	}
	require.Equal(t, expected, base)
}

// Test reading sources yaml
func TestReadSourcesYaml(t *testing.T) {

	ac := Topics{}
	yamlContent := `
sources:
  assemblyline:
    description: Samples forwarded to Azul by Assemblyline
    elastic:
      number_of_replicas: 2
      number_of_shards: 3
    kafka:
      numpartitions: 3
      replicationfactor: 1
      config:
        segment.bytes: 1073741824
    icon_class: fa-gears
    partition_unit: "year"
    references:
      - description: Priority of the submission
        name: priority
        required: true
      - description: Short description of what/why samples collected
        name: description
        required: true
      - description: User who submitted the sample
        name: user
        required: false
  incidents:
    description: Incident response and samples collected during investigations
    expire_events_after: 2 months
    elastic:
      number_of_replicas: 2
      number_of_shards: 3
    icon_class: fa-ambulance
    partition_unit: year
    references:
      - description: Task tracking identifier for incident or request
        name: task_id
        required: true
      - description: Short description of what/why samples collected
        name: description
        required: true
      - description: Investigation/operation name the samples were collected under
        name: operation
        required: false
      - description: Local submitter or point of contact for samples
        name: user
        required: false
`
	sourcesTopics := ac.readSourcesYaml(yamlContent)
	bedSet.Logger.Debug().Msgf("%+v", sourcesTopics)
	// Check if the correct number of topics is read
	require.Equal(t, len(sourcesTopics), 10)

	keyed := testKeyTopics(sourcesTopics)

	require.Equal(t, keyed["assemblyline.binary.sourced"], GenericKafkaTopicSpecification{
		Topic: "assemblyline.binary.sourced", NumPartitions: 3, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact"},
	})

	require.Equal(t, keyed["assemblyline.binary.extracted"], GenericKafkaTopicSpecification{
		Topic: "assemblyline.binary.extracted", NumPartitions: 3, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact"},
	})

	require.Equal(t, keyed["incidents.binary.augmented"], GenericKafkaTopicSpecification{
		Topic: "incidents.binary.augmented", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "5184000000",
			"cleanup.policy": "delete"},
	})

	require.Equal(t, keyed["incidents.binary.mapped"], GenericKafkaTopicSpecification{
		Topic: "incidents.binary.mapped", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "5184000000",
			"cleanup.policy": "delete"},
	})

	require.Equal(t, keyed["incidents.binary.enriched"], GenericKafkaTopicSpecification{
		Topic: "incidents.binary.enriched", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "5184000000",
			"cleanup.policy": "delete"},
	})
}

func TestGetTopicsFromConf(t *testing.T) {
	ac := Topics{}
	ktp := Settings.Events.Kafka.TopicPrefix
	shc := Events.Sources
	toc := Events.Topics
	Settings.Events.Kafka.TopicPrefix = "qa01"
	Events.Topics = `
- topic: system.testing
  numpartitions: 3
  replicationfactor: 1
`
	Events.Sources = `
sources:
  incidents:
    description: Incident response and samples collected during investigations
    elastic:
      number_of_replicas: 2
      number_of_shards: 3
    icon_class: fa-ambulance
    partition_unit: year
    references:
      - description: Task tracking identifier for incident or request
        name: task_id
        required: true
`
	defer ResetSettings()

	topics := ac.GetTopicsFromConf()
	keyed := testKeyTopics(topics)
	require.Contains(t, keyed, "azul.qa01.incidents.binary.sourced")
	require.Contains(t, keyed, "azul.qa01.incidents.binary.extracted")
	require.Contains(t, keyed, "azul.qa01.incidents.binary.mapped")
	require.Contains(t, keyed, "azul.qa01.incidents.binary.enriched")
	require.Contains(t, keyed, "azul.qa01.incidents.binary.augmented")
	require.Contains(t, keyed, "azul.qa01.system.testing")

	Events.Sources = shc
	Events.Topics = toc
	Settings.Events.Kafka.TopicPrefix = ktp
}

func TestGetTopicsFromConfPresets(t *testing.T) {
	defer ResetSettings()
	ac := Topics{}
	ktp := Settings.Events.Kafka.TopicPrefix
	shc := Events.Sources
	toc := Events.Topics
	Settings.Events.Kafka.TopicPrefix = "qa01"
	Events.Topics = ""
	Events.Sources = ""
	topics := ac.GetTopicsFromConf()
	keyed := testKeyTopics(topics)
	require.Equal(t, keyed["azul.qa01.system.insert"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.insert", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact",
		},
	})
	require.Equal(t, keyed["azul.qa01.system.delete"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.delete", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "-1",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "compact",
		},
	})
	require.Equal(t, keyed["azul.qa01.system.expedite"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.expedite", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "604800000",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "delete",
		},
	})
	require.Equal(t, keyed["azul.qa01.system.error"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.error", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "604800000",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "delete",
		},
	})
	require.Equal(t, keyed["azul.qa01.system.plugin"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.plugin", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "604800000",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "delete",
		},
	})
	require.Equal(t, keyed["azul.qa01.system.status"], GenericKafkaTopicSpecification{
		Topic: "azul.qa01.system.status", NumPartitions: 1, ReplicationFactor: 1, Config: map[string]string{
			"retention.ms":   "86400000",
			"segment.bytes":  "1073741824",
			"cleanup.policy": "delete",
		},
	})

	Events.Sources = shc
	Events.Topics = toc
	Settings.Events.Kafka.TopicPrefix = ktp
}
