package topics

import (
	"regexp"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/stretchr/testify/require"
)

func TestMatchAllTopics(t *testing.T) {
	st.Events.Kafka.TopicPrefix = "dev01"
	var pattern string
	var err error
	var r *regexp.Regexp

	pattern, err = MatchBinaryTopics([]string{}, []events.BinaryAction{})
	require.Nil(t, err)
	require.Equal(t, pattern, "^azul\\.dev01\\.[^\\.]+\\.binary\\.[^\\.]+$")
	r, err = regexp.Compile(pattern)
	require.Nil(t, err)
	require.True(t, r.MatchString("azul.dev01.hunt.binary.sourced"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.triage.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.mapped"))
	require.True(t, r.MatchString("azul.dev01.testing.binary.sourced"))

	pattern, err = MatchBinaryTopics([]string{"hunt"}, []events.BinaryAction{})
	require.Nil(t, err)
	require.Equal(t, pattern, "^azul\\.dev01\\.(hunt)\\.binary\\.[^\\.]+$")
	r, err = regexp.Compile(pattern)
	require.Nil(t, err)
	require.True(t, r.MatchString("azul.dev01.hunt.binary.sourced"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.extracted"))
	require.False(t, r.MatchString("azul.dev01.triage.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.mapped"))
	require.False(t, r.MatchString("azul.dev01.testing.binary.sourced"))

	pattern, err = MatchBinaryTopics([]string{"hunt", "triage"}, []events.BinaryAction{})
	require.Nil(t, err)
	require.Equal(t, pattern, "^azul\\.dev01\\.(hunt|triage)\\.binary\\.[^\\.]+$")
	r, err = regexp.Compile(pattern)
	require.Nil(t, err)
	require.True(t, r.MatchString("azul.dev01.hunt.binary.sourced"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.triage.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.mapped"))
	require.False(t, r.MatchString("azul.dev01.testing.binary.sourced"))

	pattern, err = MatchBinaryTopics([]string{"hunt", "triage"}, []events.BinaryAction{events.ActionSourced, events.ActionExtracted})
	require.Nil(t, err)
	require.Equal(t, pattern, "^azul\\.dev01\\.(hunt|triage)\\.binary\\.(sourced|extracted)$")
	r, err = regexp.Compile(pattern)
	require.Nil(t, err)
	require.True(t, r.MatchString("azul.dev01.hunt.binary.sourced"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.triage.binary.extracted"))
	require.False(t, r.MatchString("azul.dev01.hunt.binary.mapped"))
	require.False(t, r.MatchString("azul.dev01.testing.binary.sourced"))

	pattern, err = MatchBinaryTopics([]string{}, []events.BinaryAction{events.ActionSourced, events.ActionExtracted})
	require.Nil(t, err)
	require.Equal(t, pattern, "^azul\\.dev01\\.[^\\.]+\\.binary\\.(sourced|extracted)$")
	r, err = regexp.Compile(pattern)
	require.Nil(t, err)
	require.True(t, r.MatchString("azul.dev01.hunt.binary.sourced"))
	require.True(t, r.MatchString("azul.dev01.hunt.binary.extracted"))
	require.True(t, r.MatchString("azul.dev01.triage.binary.extracted"))
	require.False(t, r.MatchString("azul.dev01.hunt.binary.mapped"))
	require.True(t, r.MatchString("azul.dev01.testing.binary.sourced"))

	_, err = MatchBinaryTopics([]string{}, []events.BinaryAction{"confabulated"})
	require.NotNil(t, err)
}

func TestGetSystemTopic(t *testing.T) {
	st.Events.Kafka.TopicPrefix = "dev01"
	require.Equal(t, GetSystemTopic("status"), "azul.dev01.system.status")
	st.Events.Kafka.TopicPrefix = "dev08"
	require.Equal(t, GetSystemTopic("plugin"), "azul.dev08.system.plugin")
	require.Equal(t, GetSystemTopic("report"), "azul.dev08.system.report")
	require.Equal(t, GetSystemTopic("download"), "azul.dev08.system.download")
}

func TestGetSourceTopic(t *testing.T) {
	var err error
	var topic string
	st.Events.Kafka.TopicPrefix = "dev01"
	topic, err = GetBinaryTopic("triage", "sourced")
	require.Nil(t, err)
	require.Equal(t, topic, "azul.dev01.triage.binary.sourced")
	st.Events.Kafka.TopicPrefix = "dev08"
	topic, err = GetBinaryTopic("hunt", "extracted")
	require.Nil(t, err)
	require.Equal(t, topic, "azul.dev08.hunt.binary.extracted")
	topic, err = GetBinaryTopic("triage", "augmented")
	require.Nil(t, err)
	require.Equal(t, topic, "azul.dev08.triage.binary.augmented")
	topic, err = GetBinaryTopic("triage", "enriched")
	require.Nil(t, err)
	require.Equal(t, topic, "azul.dev08.triage.binary.enriched")
}
