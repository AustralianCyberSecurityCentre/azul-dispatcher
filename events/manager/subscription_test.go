package manager

import (
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionNew(t *testing.T) {
	var fes consumer.ConsumeParams
	resetter := st.Events.Kafka.TopicPrefix
	st.Events.Kafka.TopicPrefix = "qa01"
	topics.RegenTopics()

	fes = consumer.ConsumeParams{
		Model:           "binary",
		RequireExpedite: true,
		RequireLive:     true,
		RequireHistoric: true,
	}
	subs, err := getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("expedite", "myplugin", "latest", "^azul\\.qa01\\.system\\.expedite$"),
		newSub("live", "myplugin", "latest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.[^\\.]+$"),
		newSub("historic", "myplugin", "earliest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.[^\\.]+$"),
	})
	fes = consumer.ConsumeParams{
		Model:           "binary",
		RequireExpedite: true,
		RequireLive:     true,
	}
	subs, err = getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("expedite", "myplugin", "latest", "^azul\\.qa01\\.system\\.expedite$"),
		newSub("live", "myplugin", "latest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.[^\\.]+$"),
	})

	// plugin requires binary events with data
	fes = consumer.ConsumeParams{
		Model:           "binary",
		RequireEvents:   events.ActionsBinaryDataOk,
		RequireExpedite: true,
		RequireLive:     true,
		RequireHistoric: true,
	}
	subs, err = getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("expedite", "myplugin", "latest", "^azul\\.qa01\\.system\\.expedite$"),
		newSub("live", "myplugin", "latest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.(sourced|extracted|augmented)$"),
		newSub("historic", "myplugin", "earliest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.(sourced|extracted|augmented)$"),
	})

	// backup tool back up only historical binary events with data
	fes = consumer.ConsumeParams{
		Model:           "binary",
		RequireEvents:   events.ActionsBinaryDataOk,
		RequireHistoric: true,
	}
	subs, err = getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("historic", "myplugin", "earliest", "^azul\\.qa01\\.[^\\.]+\\.binary\\.(sourced|extracted|augmented)$"),
	})

	// status event subscription
	fes = consumer.ConsumeParams{
		Model:           "status",
		RequireExpedite: true,
		RequireLive:     true,
	}
	subs, err = getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("live", "myplugin", "latest", "^azul\\.qa01\\.system\\.status$"),
	})
	fes = consumer.ConsumeParams{
		Model:           "status",
		RequireHistoric: true,
	}
	subs, err = getSubscriptions("myplugin", &fes)
	require.Nil(t, err)
	require.Equal(t, subs, []subscription{
		newSub("historic", "myplugin", "earliest", "^azul\\.qa01\\.system\\.status$"),
	})

	st.Events.Kafka.TopicPrefix = resetter
	topics.RegenTopics()
}
