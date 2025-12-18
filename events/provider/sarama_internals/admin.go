package saramago

import (
	"regexp"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
)

// List all the topics on the kafka cluster and return them as a list of strings.
func ListTopics(brokers []string) ([]string, error) {
	config := sarama.NewConfig()
	config.Admin.Timeout = time.Duration(30) * time.Second
	config.Net.MaxOpenRequests = 1
	// Provides a name for this kafka connection for logging debugging and auditing.
	config.ClientID = "saramaListAdmin"
	config.MetricRegistry = metrics.DefaultRegistry
	adminClient, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		bedSet.Logger.Error().Err(err).Msg("Error occurred when attempting to set up the Sarama Admin Client.")
		return []string{}, err
	}
	defer adminClient.Close()
	topicMap, err := adminClient.ListTopics()
	if err != nil {
		bedSet.Logger.Warn().Err(err).Msg("Couldn't list kafka topics due to an error.")
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

// Get topics or return an empty list if there are no topics that match the query or kafka can't currently be contacted.
func GetTopics(brokers []string, regex *regexp.Regexp) []string {
	topics, err := ListTopics(brokers)
	if err != nil {
		return []string{}
	}
	selectedTopics := make([]string, 0)
	for _, topic := range topics {
		if regex.MatchString(topic) {
			selectedTopics = append(selectedTopics, topic)
		}
	}
	return selectedTopics
}
