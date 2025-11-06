package manager

import (
	"fmt"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// subscription contains all the data needed to manage a subscription to a pattern of kafka topics
type subscription struct {
	// friendly name of subscription
	name string
	// kafka subscription consumer name
	group string
	// default starting offset for new subscriptions
	offset string
	// kafka topics wildcard matcher
	pattern string

	// kafka consumer to actually pull events from
	consumer provider.ConsumerInterface
}

// newSub returns a new subscription.
func newSub(name, pluginKey, offset, pattern string) subscription {
	group := fmt.Sprintf("azul-%s-%s", pluginKey, name)
	return subscription{name, group, offset, pattern, nil}
}

// getSubscriptions returns all subscriptions required to satisfy the plugins event requests.
func getSubscriptions(pluginKey string, p *consumer.ConsumeParams) ([]subscription, error) {
	var err error
	var pattern string
	// the ordering is important for subscriptions, consume all events from first then move to second, etc.
	ret := []subscription{}
	if p.Model != events.ModelBinary {
		// simplest case, non-binary consumers cannot have events prioritised and only have one feed
		// this subscription can either reprocess all events or only receive events sent after registration
		pattern = topics.RegexpTopic(topics.GetSystemTopic(p.Model))
	} else {
		// subset only apply to the live/historical consumers and is primarily to split at the topic level
		// binaries that have associated data vs meta only, as the vast majority of plugins require data
		// and would otherwise spend a large amount of time just skipping/filtering events
		pattern, err = topics.MatchBinaryTopics(p.RequireSources, p.RequireEvents)
		if err != nil {
			return nil, err
		}
		// the order of topics in ret is important as in event_reader the first subscription in the list is read first.
		// This means that this topic needs to be exhausted before subsequent topics are read from.
		if p.RequireExpedite {
			// expedite - highest priority to process
			ret = append(ret, newSub("expedite", pluginKey, "latest", topics.RegexExpediteTopic))
		}
	}
	if p.RequireLive {
		// live - return events from this point onwards if this is a truly new pluginKey
		ret = append(ret, newSub("live", pluginKey, "latest", pattern))
	}
	if p.RequireHistoric {
		// historic - return events from start of queue if this is a truly new pluginKey
		ret = append(ret, newSub("historic", pluginKey, "earliest", pattern))
	}
	st.Logger.Debug().Str("pattern", pattern).Str("pluginKey", pluginKey).Msg("new kafka subscriptions")
	return ret, nil
}
