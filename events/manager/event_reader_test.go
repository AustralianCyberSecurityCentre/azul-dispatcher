package manager

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/goccy/go-json"
	"github.com/tidwall/sjson"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_consume"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"

	"github.com/stretchr/testify/require"
)

var er_filter = testdata.GetEvent("events/manager/binary_filtered.json")
var er_no_filter = testdata.GetEvent("events/manager/binary_simple.json")

func testERAdd(t *testing.T, prov provider.ProviderInterface, uniq int) {
	msg2, err := sjson.Set(er_no_filter, "entity.sha256", fmt.Sprintf("plain-%d", uniq))
	require.Nil(t, err)
	flight, err := pipeline.NewMsgInFlightFromJson([]byte(msg2), events.ModelBinary)
	require.Nil(t, err)
	avro, err := flight.Event.ToAvro()
	require.Nil(t, err)

	p, err := prov.CreateProducer()
	require.Nil(t, err)
	topic := "azul.test01.testing.binary.enriched"
	err = p.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     sarama.StringEncoder(avro),
	}, true)
	require.Nil(t, err)
}

func testERAddEmpty(t *testing.T, prov provider.ProviderInterface) {
	msg := ``
	p, err := prov.CreateProducer()
	require.Nil(t, err)
	topic := "azul.test01.testing.binary.enriched"
	err = p.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     sarama.StringEncoder(msg),
	}, true)
	require.Nil(t, err)
}

func testERAddNil(t *testing.T, prov provider.ProviderInterface) {
	p, err := prov.CreateProducer()
	require.Nil(t, err)
	topic := "azul.test01.testing.binary.enriched"
	err = p.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     nil,
	}, true)
	require.Nil(t, err)
}

func testERAddBad(t *testing.T, prov provider.ProviderInterface, uniq int) {
	msg := `{"blah": 5}`
	p, err := prov.CreateProducer()
	require.Nil(t, err)
	topic := "azul.test01.testing.binary.enriched"
	err = p.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     sarama.StringEncoder(msg),
	}, true)
	require.Nil(t, err)
}

func testERAddFiltered(t *testing.T, prov provider.ProviderInterface, uniq int) {
	msg2, err := sjson.Set(er_filter, "entity.id", fmt.Sprintf("filtered-%d", uniq))
	require.Nil(t, err)
	flight, err := pipeline.NewMsgInFlightFromJson([]byte(msg2), events.ModelBinary)
	require.Nil(t, err)
	avro, err := flight.Event.ToAvro()
	require.Nil(t, err)

	p, err := prov.CreateProducer()
	require.Nil(t, err)
	topic := "azul.test01.testing.binary.enriched"
	err = p.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     sarama.StringEncoder(avro),
	}, true)
	require.Nil(t, err)
}

func TestEventReader(t *testing.T) {
	var fes consumer.ConsumeParams
	st.Events.Kafka.TopicPrefix = "test01"
	st.Events.Sources = `
sources:
  testing: {}
`
	topics.RegenTopics()
	defer st.ResetSettings()

	prov, err := provider.NewMemoryProvider()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("could not initialise kafka admin client")
	}

	// ac, err := prov.CreateAdmin()
	// require.Nil(t, err)
	ac, err := topics.NewTopicControl(prov)
	if err != nil {
		t.Fatalf("Error create client %v", err)
	}
	err = ac.EnsureAllTopics()
	require.Equal(t, nil, err)

	fes = consumer.ConsumeParams{
		Model:           "binary",
		RequireExpedite: true,
		RequireLive:     true,
	}
	er, err := newEventReader(prov, "testplugin", &fes, time.Time{})
	require.Nil(t, err)

	ready, nReady := er.checkReady()
	require.Equal(t, nReady, []string{})
	require.Equal(t, ready, []string{"expedite", "live"})

	cp := pipeline.NewConsumePipeline([]pipeline.ConsumeAction{&pipeline_consume.FilterFlagged{}}, nil)
	fes = consumer.ConsumeParams{
		Model:           "binary",
		Name:            "test",
		Version:         "5",
		RequireExpedite: true,
		RequireLive:     true,
		Count:           1,
		Deadline:        1,
		IsTask:          false,
	}
	evs, info := er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 0, Ready: true})
	require.Equal(t, evs, []*msginflight.MsgInFlight{})

	// add some events
	testERAddFiltered(t, prov, 66)
	testERAddFiltered(t, prov, 67)
	testERAdd(t, prov, 1)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{"FilterFlagged-filter_flagged": 2}, Fetched: 1, Filtered: 2, Ready: true})
	require.Equal(t, len(evs), 1)
	bin, ok := evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-1")

	// add some events
	// testERAdd(t, prov, 1)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 0, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 0)

	// add some events
	testERAdd(t, prov, 1)
	testERAdd(t, prov, 2)
	testERAdd(t, prov, 3)
	testERAdd(t, prov, 4)
	testERAdd(t, prov, 5)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 1, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 1)
	bin, ok = evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-1")

	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 1, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 1)
	bin, ok = evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-2")

	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 1, Filtered: 0, Ready: true})
	data, err := json.Marshal(info)
	require.Nil(t, err)
	require.JSONEq(t, string(data), `{"fetched":1, "filtered":0, "ready":true, "paused": false}`)
	require.Equal(t, len(evs), 1)
	bin, ok = evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-3")

	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 1, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 1)
	bin, ok = evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-4")

	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 1, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 1)
	bin, ok = evs[0].GetBinary()
	require.True(t, ok)
	require.Equal(t, bin.Entity.Sha256, "plain-5")

	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 0, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 0)
	data, err = json.Marshal(info)
	require.Nil(t, err)
	require.JSONEq(t, string(data), `{"fetched":0, "filtered":0, "ready":true, "paused": false}`)

	// test that json event that cannot decode to msginflight is filtered
	fes.Count = 3
	testERAdd(t, prov, 10)
	testERAddBad(t, prov, 11)
	testERAdd(t, prov, 12)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 2, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 2)

	log.Printf("test nil")
	testERAddNil(t, prov)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 0, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 0)

	log.Printf("test empty")
	testERAddEmpty(t, prov)
	evs, info = er.pull(cp, &fes)
	require.Equal(t, info, &models.EventResponseInfo{Filters: map[string]int{}, Fetched: 0, Filtered: 0, Ready: true})
	require.Equal(t, len(evs), 0)
}
