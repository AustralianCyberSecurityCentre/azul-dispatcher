package producer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/topics"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
)

type BadPartitionSelection struct {
	Msg string
}

func (r *BadPartitionSelection) Error() string {
	return r.Msg
}

// Producer contains data to control Kafka producer
type Producer struct {
	producer    provider.ProducerInterface
	adminClient provider.AdminInterface
	producePipe *pipeline.ProducePipeline
	checkSize   int
}

// NewProducer returns a new producer with specified options.
func NewProducer(prov provider.ProviderInterface) (*Producer, error) {
	st.Logger.Info().Msg("create a new Producer")
	p, err := prov.CreateProducer()
	if err != nil {
		return nil, err
	}
	adminClient, err := prov.CreateAdmin()
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer:    p,
		adminClient: adminClient,
		producePipe: &pipeline.ProducePipeline{},
		checkSize:   int(st.Events.Kafka.MessageMaxBytes),
	}, err
}

func (p *Producer) SetPipeline(producePipe *pipeline.ProducePipeline) {
	p.producePipe = producePipe
}

func (p *Producer) TransformEvents(evs []byte, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, *pipeline.ProduceActionInfo, error) {
	var err error
	var resp *models.ResponsePostEvent
	var msgInFlights []*msginflight.MsgInFlight
	if meta.AvroNonBulkFormat {
		// reprocess handling - individual event as they are read in non-bulk avro format from kafka
		msgInFlights, resp, err = pipeline.AvroToMsgInFlights(evs, meta.Model)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("ensure events were in json format: %w", err)
		}
	} else if meta.AvroFormat {
		msgInFlights, err = msginflight.AvroBulkToMsgInFlights(evs, meta.Model)
		// individual decodes can't fail so all are ok if this worked
		resp = &models.ResponsePostEvent{TotalOk: len(msgInFlights)}
		if err != nil {
			// check if mistakenly supplied json data
			var tmp any
			err2 := json.Unmarshal(evs, &tmp)
			if err2 == nil {
				return nil, nil, nil, fmt.Errorf("supplied events were in json format, not avro")
			}
			return nil, nil, nil, fmt.Errorf("avro event did not match expected bulk schema: %w", err)
		}
	} else {
		msgInFlights, resp, err = pipeline.JsonListsToMsgInFlights(evs, meta.Model)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("ensure events were in json format: %w", err)
		}
	}
	to_publish, produceActionInfo := p.TransformMsgInFlights(msgInFlights, meta)
	return to_publish, resp, produceActionInfo, nil
}

func (p *Producer) TransformMsgInFlights(msgInFlights []*msginflight.MsgInFlight, meta *pipeline.ProduceParams) ([]*msginflight.MsgInFlight, *pipeline.ProduceActionInfo) {
	to_publish := make([]*msginflight.MsgInFlight, 0, len(msgInFlights)*2)
	// run pipeline actions for each message
	additional, produceActionInfo := p.producePipe.RunProduceActions(msgInFlights, meta)
	to_publish = append(to_publish, additional...)
	return to_publish, produceActionInfo
}

func (p *Producer) ProduceAnyEvents(confirm bool, to_publish []*msginflight.MsgInFlight) error {
	return p.publish(confirm, to_publish...)
}

// Publish produces events to kafka, optionally blocking for confirm receipts.
func (p *Producer) publish(confirm bool, evs ...*msginflight.MsgInFlight) error {
	var err error
	selectedTopics := map[string]int{}
	createdEventAuthors := map[string]int{}
	// we iterate by index to stop unnecessary copies with range
	errorList := []error{}
	for i := range evs {
		// ensure binary tracking fields are finalised
		if binary, ok := evs[i].GetBinary(); ok {
			err = binary.UpdateTrackingFields()
			if err != nil {
				errorList = append(errorList, err)
				continue
			}
		}
		if insert, ok := evs[i].GetInsert(); ok {
			err = insert.UpdateTrackingFields()
			if err != nil {
				errorList = append(errorList, err)
				continue
			}
		}

		topic, err := chooseTopic(evs[i])
		if err != nil {
			errorList = append(errorList, err)
			continue
		}

		// bake event into avro bytes
		cooked, err := evs[i].Event.ToAvro()
		if err != nil {
			// failed to turn in to json, abort submission of this data
			err = fmt.Errorf("%w: unable to marshal produced event %v", err, evs[i])
			errorList = append(errorList, err)
			continue
		}

		// check event is not too big for kafka
		// This is done after transformations, as messages such as status can be vastly reduced in size
		// when we extract the 'results' field.
		trueSize := len(cooked)
		if trueSize > p.checkSize {
			errorList = append(errorList, fmt.Errorf(
				"message too large for kafka: %d vs %d bytes", trueSize, p.checkSize))
			continue
		}
		if trueSize == 0 {
			errorList = append(errorList, fmt.Errorf(
				"message had no length after avro conversion: %v", evs[i].Event))
			continue
		}
		key := *evs[i].Base.KafkaKey

		err = p.producer.Produce(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(cooked),
			Key:   sarama.StringEncoder(key),
		}, confirm)

		if err != nil {
			// Message couldn't be processed so don't include it in stats.
			errorList = append(errorList, err)
			continue
		}

		prom.EventsProducePublished.WithLabelValues(evs[i].Base.Author.Name, evs[i].Base.Model.Str()).Inc()
		if status, ok := evs[i].GetStatus(); ok {
			// Add to status if the author isn't a user type.
			if strings.ToLower(evs[i].Base.Author.Category) != "user" {
				prom.EventsProduceStatusPublished.WithLabelValues(evs[i].Base.Author.Name, status.Entity.Status).Inc()
			}
		}
		selectedTopics[topic]++
		createdEventAuthors[evs[i].Base.Author.Name]++
	}
	successfulEventCount := len(evs) - len(errorList)

	usedTopics := []string{}
	for k := range selectedTopics {
		usedTopics = append(usedTopics, k)
	}
	usedAuthors := []string{}
	for k := range createdEventAuthors {
		usedAuthors = append(usedAuthors, k)
	}
	st.Logger.Debug().Int("count", successfulEventCount).Int("failed", len(errorList)).Strs("topics", usedTopics).Strs("authors", usedAuthors).Msg("events produced")
	if len(errorList) > 0 {
		var errorSb strings.Builder
		errorSb.WriteString(fmt.Sprintf("successfully submitted %d/%d events with the failed events providing the following errors: ", successfulEventCount, len(evs)))
		for erIdx := range errorList {
			errorSb.WriteString(errorList[erIdx].Error())
			errorSb.WriteString(";\n")
		}
		return errors.New(errorSb.String())
	}

	return nil
}

// Tombstone marks a message key for deletion in kafka.
func (p *Producer) Tombstone(confirm bool, ev *msginflight.MsgInFlight) error {
	topic, err := chooseTopic(ev)
	if err != nil {
		return err
	}
	err = p.producer.Produce(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(*ev.Base.KafkaKey),
		Value: nil,
	}, confirm)
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) Stop() {
	err := p.producer.Close()
	if err != nil {
		st.Logger.Warn().Err(err).Msg("error when attempting to close producer provider in producer.")
	}
	p.adminClient.Close()
}

func chooseTopic(msg *msginflight.MsgInFlight) (string, error) {
	var topic string
	var err error
	if binary, ok := msg.GetBinary(); !ok {
		// status, plugin, report, scrape, etc
		// catch all to simplify management of topics for custom entity types
		topic = topics.GetSystemTopic(msg.Base.Model)
	} else {
		// handling binary docs is more complicated
		if binary.Flags.Expedite {
			// expedite events go to a fixed topic
			topic = topics.ExpediteTopic
		} else if binary.Flags.Retry {
			// retry events go to a fixed topic
			topic = topics.RetryTopic
		} else {
			if len(binary.Source.Name) == 0 || len(binary.Action) == 0 {
				return "", &BadPartitionSelection{Msg: "missing mandatory source or event fields"}
			}
			// split events by source and entity type
			topic, err = topics.GetBinaryTopic(binary.Source.Name, binary.Action)
			if err != nil {
				return "", err
			}
		}
		st.Logger.Trace().Str("topic", topic).Str("entityType", msg.Base.Model.Str()).Msg("selected topic for binary event")
	}
	return topic, nil
}
