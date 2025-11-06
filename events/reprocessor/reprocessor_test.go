package reprocessor

import (
	"fmt"
	"testing"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func setupEnvironment(upgradeSources bool, upgradeSystem bool, desiredTopics []string) {
	st.Events.Reprocess.LegacyJson = true
	st.Events.Reprocess.UpgradeSources = upgradeSources
	st.Events.Reprocess.UpgradeSystem = upgradeSystem
	st.Events.Topics = ""
	// Inject fake topics into the Kafka config
	for _, topic := range desiredTopics {
		st.Events.Topics += fmt.Sprintf(`- topic: %s
  numpartitions: 1
  replicationfactor: 1
  config:
    retention.ms: 43200000
    segment.bytes: 1073741824
`, topic)
	}
}

func assertTopicGeneration(t *testing.T, allTopics []*sarama.TopicMetadata, expected []string) {
	selectedTopics, err := getTopicsForReprocess(allTopics)
	if err != nil {
		t.Error("failed to discover topics:", err)
		return
	}

	require.ElementsMatch(t, selectedTopics, expected, "emitted topics do not match")
}

func TestTopicSelection(t *testing.T) {
	st.Events.Reprocess.PreviousPrefix = "qa01"
	st.Events.Kafka.TopicPrefix = "qa02"

	// Base case for a system topic
	allTopics := []*sarama.TopicMetadata{
		{
			Name: "azul.qa01.system.insert",
		},
	}
	expectedOutput := []string{
		"azul.qa01.system.insert",
	}

	setupEnvironment(false, true, []string{"azul.qa01.system.insert"})
	assertTopicGeneration(t, allTopics, expectedOutput)

	// Ignoring unrelated topics
	allTopics = []*sarama.TopicMetadata{
		{
			Name: "notazul.test",
		},
	}
	expectedOutput = []string{}

	setupEnvironment(false, true, []string{"azul.qa01.system.insert"})
	assertTopicGeneration(t, allTopics, expectedOutput)

	// Mapping two source topics, ignoring another that doesn't match the regex
	allTopics = []*sarama.TopicMetadata{
		{
			Name: "azul.qa01.cool.binary",
		},
		{
			Name: "azul.qa01.neat.binary.data",
		},
		{
			Name: "azul.qa01.neat.organic",
		},
		{
			Name: "azul.qa01.neat.organic.data",
		},
	}
	expectedOutput = []string{
		"azul.qa01.cool.binary",
		"azul.qa01.neat.binary.data",
	}

	setupEnvironment(true, false, []string{"azul.qa01.cool.binary", "azul.qa01.neat.binary.data"})
	assertTopicGeneration(t, allTopics, expectedOutput)
}

func TestWatchForEOF(t *testing.T) {
	testChannel := make(chan int)
	waitSync := make(chan bool)

	go func() {
		testChannel <- 0
		testChannel <- 2
		testChannel <- 1
		waitSync <- true
	}()

	watchForEOF(testChannel, 3)
	// Make sure both the routine and watcher terminate in sequence
	<-waitSync
}
