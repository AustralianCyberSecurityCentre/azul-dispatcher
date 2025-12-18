package topics

import (
	"fmt"
	"slices"
	"strings"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
)

type TopicControl struct {
	Admin provider.AdminInterface
}

func NewTopicControl(prov provider.ProviderInterface) (*TopicControl, error) {
	var err error
	tc := TopicControl{}
	tc.Admin, err = prov.CreateAdmin()
	if err != nil {
		return nil, err
	}
	return &tc, nil
}

func (tc *TopicControl) createTopics(requiredTopics []st.GenericKafkaTopicSpecification, ignoreTopicMismatch bool) error {
	// Get list of existing topics
	allTopics, err := tc.GetAllTopics()
	if err != nil {
		return err
	}

	// Check if the desired topics exist
	var topicsSlice []string
	nameToTopicMap := make(map[string]*sarama.TopicMetadata)
	for _, topic := range allTopics {
		topicsSlice = append(topicsSlice, topic.Name)
		nameToTopicMap[topic.Name] = topic
	}

	var missingTopics []st.GenericKafkaTopicSpecification
	var configsToUpdate []st.GenericKafkaTopicSpecification
	partitionsToUpdate := make(map[string]int)
	for _, t := range requiredTopics {
		if slices.Contains(topicsSlice, t.Topic) {
			existingTopicPartitions := len(nameToTopicMap[t.Topic].Partitions)
			existingTopicReplicas := len(nameToTopicMap[t.Topic].Partitions[0].Replicas)
			if existingTopicPartitions <= t.NumPartitions && existingTopicReplicas == t.ReplicationFactor {
				configsToUpdate = append(configsToUpdate, t)
				if existingTopicPartitions < t.NumPartitions {
					partitionsToUpdate[t.Topic] = t.NumPartitions
				}
			} else {
				if ignoreTopicMismatch {
					bedSet.Logger.Warn().Msgf("%s exists with Partitions:%d Replicas:%d. New topic specifies:%+v but will be ignored.",
						t.Topic, existingTopicPartitions, existingTopicReplicas, t)
					continue
				}
				return fmt.Errorf("%s exists with Partitions:%d Replicas:%d. New topic specifies:%+v",
					t.Topic, existingTopicPartitions, existingTopicReplicas, t)
			}
		} else {
			missingTopics = append(missingTopics, t)
		}
	}

	// Create missing topics
	if len(missingTopics) > 0 {
		names := []string{}
		for _, v := range missingTopics {
			names = append(names, v.Topic)
		}
		bedSet.Logger.Debug().Msgf("Creating topics: %+v", names)
		err = tc.Admin.CreateTopics(missingTopics)
		if err != nil {
			bedSet.Logger.Err(err).Msg("Failed to create Topics")
			return err
		}
	}

	if len(partitionsToUpdate) > 0 {
		names := []string{}
		for k := range partitionsToUpdate {
			names = append(names, k)
		}
		// Increasing the partitions of these topics
		bedSet.Logger.Info().Msgf("Updating partitions for topics: %+v", names)
		tc.Admin.UpdateTopicPartitions(partitionsToUpdate)
	}

	// Update all existing topics config - This just updates them every time because there is no way to check existing config.
	if len(configsToUpdate) > 0 {
		names := []string{}
		for _, v := range configsToUpdate {
			names = append(names, v.Topic)
		}
		bedSet.Logger.Debug().Msgf("Updating config for topics: %+v", names)
		tc.Admin.UpdateTopicsConfig(configsToUpdate)
	}
	return nil
}

// makeHealthy creates azul kafka topics
func (tc *TopicControl) makeHealthy() error {
	allTopics, err := tc.GetAllTopics()
	if err != nil {
		return err
	}
	bedSet.Logger.Info().Int("topics", len(allTopics)).Msg("connected to kafka cluster")
	err = tc.CreateAllTopics()
	if err != nil {
		return err
	}
	return nil
}

// GetAllTopics fetches topic metadata from Kafka.
func (tc *TopicControl) GetAllTopics() ([]*sarama.TopicMetadata, error) {
	res, err := tc.Admin.GetMetadata(nil, true, 10000)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// CreateAllTopics creates all azul kafka topics defined in config
func (tc *TopicControl) CreateAllTopics() error {
	requiredTopics := st.GetTopicsFromConf()
	// Create the final list of topics
	err := tc.createTopics(requiredTopics, st.Events.IgnoreTopicMismatch)
	if err != nil {
		return err
	}
	return nil
}

// DeleteAllTopics deletes all azul kafka topics for the current topic prefix.
// VERY DANGEROUS.
func (tc *TopicControl) DeleteAllTopics() error {
	metadata, err := tc.Admin.GetMetadata(nil, true, 15000)
	if err != nil {
		return err
	}
	topics := []string{}
	for _, topic := range metadata {
		if strings.HasPrefix(topic.Name, fmt.Sprintf("azul.%s.", st.Settings.Events.Kafka.TopicPrefix)) {
			topics = append(topics, topic.Name)
		}
	}
	return tc.Admin.DeleteTopics(topics)
}

// EnsureAllTopics returns only when all Azul kafka topics are ready or an error occurs.
func (tc *TopicControl) EnsureAllTopics() error {
	var err error
	for i := 0; i < int(st.Events.Kafka.ConnectRetries); i++ {
		bedSet.Logger.Info().Msg("attempting kafka connection")
		err = tc.makeHealthy()
		if err != nil {
			time.Sleep(time.Second * 1)
		} else {
			break
		}
	}
	return err
}
