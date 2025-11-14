package settings

import (
	"fmt"
	"slices"

	"dario.cat/mergo"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"gopkg.in/yaml.v3"
)

// This might make things easier to combine for when global settings in settings.go are scoped into a struct
type Topics struct {
}

// prefixTopics adds prefix to topic names as necessary
func (tc *Topics) prefixTopics(topics []GenericKafkaTopicSpecification) {
	for i := range topics {
		tp := topics[i].Topic
		topics[i].Topic = fmt.Sprintf("azul.%s.%s", Settings.Events.Kafka.TopicPrefix, tp)
	}
}

func (tc *Topics) mergeTopicLists(base *[]GenericKafkaTopicSpecification, newList []GenericKafkaTopicSpecification) error {
	var existingTopicsToIndex = make(map[string]int)
	var keys []string
	for i, i2 := range *base {
		existingTopicsToIndex[i2.Topic] = i
		keys = append(keys, i2.Topic)
	}
	for _, n := range newList {
		if !(slices.Contains(keys, n.Topic)) {
			*base = append(*base, n)
		} else {
			// Topics match so we merge onto the base topic
			targetIndex := existingTopicsToIndex[n.Topic]
			err := mergo.Merge(&n, (*base)[targetIndex])
			if err != nil {
				return err
			}
			(*base)[targetIndex] = n
		}
	}
	return nil
}

// readSourcesYaml parses yaml that defines sources for Metastore, and returns list of topic specifications if defined within
func (tc *Topics) readSourcesYaml(yamlContent string) []GenericKafkaTopicSpecification {
	sourcesConf, err := models.ParseSourcesYaml(yamlContent)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Str("yaml", yamlContent).Msg("Failed to unmarshal sourcesConf YAML")
	}
	var readTopics []GenericKafkaTopicSpecification
	for name, s := range sourcesConf.Sources {
		for _, binaryEventType := range events.ActionsBinary {
			newTopic := GenericKafkaTopicSpecification{
				Topic:             fmt.Sprintf("%s.binary.%s", name, binaryEventType.Str()),
				NumPartitions:     s.Kafka.NumPartitions,
				ReplicationFactor: s.Kafka.Replicationfactor,
				Config:            s.Kafka.Config,
			}
			// if unset, use defaults
			tc.setMissingTopicProperties(&newTopic)
			readTopics = append(readTopics, newTopic)
		}
	}
	return readTopics
}

func (tc *Topics) readTopicsFromYAML(yamlContent string) ([]GenericKafkaTopicSpecification, error) {
	var topics []GenericKafkaTopicSpecification
	if err := yaml.Unmarshal([]byte(yamlContent), &topics); err != nil {
		return nil, err
	}

	// if unset, use defaults
	for i := range topics {
		tc.setMissingTopicProperties(&topics[i])
	}
	return topics, nil
}

func (tc *Topics) setMissingTopicProperties(topic *GenericKafkaTopicSpecification) {
	if topic.ReplicationFactor == 0 {
		topic.ReplicationFactor = int(Events.GlobalReplicaCount)
	}
	if topic.NumPartitions == 0 {
		topic.NumPartitions = int(Events.GlobalPartitionCount)
	}
	if topic.Config == nil {
		topic.Config = getBaseTopicConfig()
	}
}

func (tc *Topics) GetTopicsFromConf() []GenericKafkaTopicSpecification {
	topics := getBaseTopicsList()
	for i := range topics {
		tc.setMissingTopicProperties(&topics[i])
	}

	// Overwrite using sources Helm config
	// Read from settings, this list of sources is also used to configure Metastore
	sourcesConf := Events.Sources
	if sourcesConf == "" {
		bedSet.Logger.Warn().Msg("Azul sources have not been provided")
	} else {
		topicsFromSources := tc.readSourcesYaml(sourcesConf)
		err := tc.mergeTopicLists(&topics, topicsFromSources)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("Failed to merge topics from Sources into Default topics")
		}
	}

	// Read the settings and merge with default topics
	topicsYAML := Events.Topics
	if topicsYAML == "" {
		bedSet.Logger.Debug().Msg("Azul topic overrides have not been provided")
	} else {
		configuredTopics, err := tc.readTopicsFromYAML(topicsYAML)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("Error reading topics from YAML")
		}
		// Overwrite default topics with topics.yaml config
		err = tc.mergeTopicLists(&topics, configuredTopics)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("Failed to merge topics from YAML into Default topics")
		}
	}
	tc.prefixTopics(topics)
	return topics
}

func GetTopicsFromConf() []GenericKafkaTopicSpecification {
	topics := Topics{}
	return topics.GetTopicsFromConf()
}
