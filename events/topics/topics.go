package topics

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// basic topics
var ExpediteTopic string
var RetryTopic string
var RegexExpediteTopic string

var InsertTopic string
var DeleteTopic string

func RegenTopics() {
	// FUTURE this function being needed is horrible in itself - vars should be passed through somehow
	ExpediteTopic = GetSystemTopic("expedite")
	RetryTopic = GetSystemTopic("error")
	RegexExpediteTopic = RegexpTopic(GetSystemTopic("expedite"))

	InsertTopic = GetSystemTopic("insert")
	DeleteTopic = GetSystemTopic("delete")
}

func init() {
	RegenTopics()
}

// GetSystemTopic returns generic topic that don't need to be stored in unique ways
// i.e. non-binary eventType
func GetSystemTopic(model events.Model) string {
	// azul.qa01.system.status
	// azul.qa01.system.plugin
	return fmt.Sprintf("azul.%s.system.%s", st.Events.Kafka.TopicPrefix, model)
}

// GetBinaryTopic returns topic with certain source, usually for binary eventType
func GetBinaryTopic(source string, action events.BinaryAction) (string, error) {
	// azul.qa01.mysource.binary
	// azul.qa01.mysource.binary.data
	return fmt.Sprintf("azul.%s.%s.binary.%s", st.Events.Kafka.TopicPrefix, source, action), nil
}

// RegexpTopic returns an exact-match regex for the topic
func RegexpTopic(topic string) string {
	return fmt.Sprintf("^%s$", regexp.QuoteMeta(topic))
}

// MatchBinaryTopics returns a regex matching all possible 'sources' for the eventType
func MatchBinaryTopics(sources []string, eventTypes []events.BinaryAction) (string, error) {
	matchSource := `[^\.]+`
	if len(sources) > 0 {
		matchSource = fmt.Sprintf(`(%s)`, strings.Join(sources, "|"))
	}
	matchEvents := `[^\.]+`
	if len(eventTypes) > 0 {
		actions, err := events.StringsFromActions(eventTypes)
		if err != nil {
			return "", err
		}
		matchEvents = fmt.Sprintf(`(%s)`, strings.Join(actions, "|"))
	}
	// azul.qa01.<source>.binary.data
	// azul.qa01.system.status
	return fmt.Sprintf(`^azul\.%s\.%s\.binary\.%s$`, st.Events.Kafka.TopicPrefix, matchSource, matchEvents), nil
}
