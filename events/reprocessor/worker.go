package reprocessor

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/prometheus/client_golang/prometheus"
)

// some system topics hold different models to their name
var SystemModelRemap = map[string]string{
	"expedite": events.ModelBinary.Str(), // expedite topic holds only binary events
}

// Internal construct used to pass received and processed events to the sender.
type WorkerTransformedMessage struct {
	message   *msginflight.MsgInFlight
	offset    int64
	topic     string
	partition int32
}

// Reprocesses a specific topic given a consumer and producer.
// Emits its index when all partitions have been processed.
type Worker struct {
	produceParams           pipeline.ProduceParams
	topic                   string
	consumer                provider.ConsumerInterface
	producer                *producer.Producer
	index                   int      // Reprocessor ID
	eof                     chan int // EOF signal to the main thread
	signaledEof             bool
	transformed             chan WorkerTransformedMessage
	eventCount              prometheus.Counter // Number of events produced by a particular worker
	eofMetric               prometheus.Gauge   // Signal for metrics to determine when a topic has completed processing
	processed               int
	sequentialZeroDataPolls int // Number of times a partition
}

// Handles a single Kafka source event.
func (worker *Worker) handle(msg *sarama_internals.Message) {
	// Process the event using the produce pipeline
	toProcess, resp, _, err := worker.producer.TransformEvents(msg.Value, &worker.produceParams)

	if err != nil {
		st.Logger.Error().Int("reprocessor", worker.index).Bool("isfatal", false).Int64("offset", int64(msg.Offset)).
			Int32("partition", msg.Partition).Str("source", msg.Topic).Err(err).Msg("reprocessor encountered transformation error")
		return
	}
	if resp.TotalFailures > 0 {
		// should not abort
		st.Logger.Error().Int("reprocessor", worker.index).Bool("isfatal", false).Int64("offset", int64(msg.Offset)).
			Int32("partition", msg.Partition).Str("source", msg.Topic).Any("failures", resp.Failures).Msg("reprocessor encountered non-fatal transformation error")
	}
	// Send the processed events to the sender thread
	for _, event := range toProcess {
		worker.transformed <- WorkerTransformedMessage{
			message:   event,
			offset:    msg.Offset,
			topic:     msg.Topic,
			partition: msg.Partition,
		}
	}
}

func (worker *Worker) checkCaughtUp() {
	if worker.consumer.AreAllPartitionsCaughtUp() {
		worker.markCaughtUp()
	}
}

func (worker *Worker) markCaughtUp() {
	// If we are at this point, we have hopefully reached EOF on all partitions
	worker.eof <- worker.index
	if !worker.signaledEof {
		st.Logger.Info().Int("reprocessor", worker.index).Bool("EOF", true).Msg("reached EOF on assigned partitions")
		worker.eofMetric.Inc()
		worker.signaledEof = true
	}
}

// Infinitely receives and transforms messages.
func (worker *Worker) doRecv(context context.Context) {
	// Wait until assignment is 0 or context is cancelled before beginning
	// Needed to verify if the topic starts in a finished state.
	for !worker.consumer.Ready() && context.Err() == nil {
		time.Sleep(2 * time.Second)
	}

	var msg *sarama_internals.Message
	for {
		select {
		case <-context.Done():
			st.Logger.Info().Int("reprocessor", worker.index).Msg("Receiving thread shutting down")
			err := worker.consumer.Close()
			if err != nil {
				panic(err)
			}
			return
		default:
			msg = worker.consumer.Poll()
			if msg != (*sarama_internals.Message)(nil) {
				worker.sequentialZeroDataPolls = 0
				worker.handle(msg)
			} else {
				st.Logger.Info().Int("reprocessor", worker.index).Msg("reprocessor got no data before timeout")
				worker.sequentialZeroDataPolls += 1
				worker.checkCaughtUp()
				// If there are lots (180==~3minutes) of empty results the topic must be empty and is not be marked as such.
				// This can happen if a topic had messages that have all aged off and been deleted out of kafka.
				// As there were messages the highwatermark is greater than 0 but there are no messages because they all got deleted for being too old.
				// So the kafka consumer won't recognise that it's caught up to live but it effectively has.
				if worker.sequentialZeroDataPolls > st.Events.Reprocess.MaxNumberOfEmptyPolls {
					worker.markCaughtUp()
				}
			}
		}
	}
}

// Infinitely publish messages.
func (worker *Worker) doSend(context context.Context) {
	var event WorkerTransformedMessage

	for {
		select {
		case event = <-worker.transformed:
		case <-context.Done():
			// Only exit once we know all events have been processed.
			if len(worker.transformed) == 0 {
				st.Logger.Info().Int("reprocessor", worker.index).Msg("Submission thread shutting down")
				worker.producer.Stop()
				return
			}
		}
		err := worker.producer.ProduceAnyEvents(false, []*msginflight.MsgInFlight{event.message})
		worker.processed += 1

		if err != nil {
			st.Logger.Error().Int("reprocessor", worker.index).Bool("isfatal", false).Int64("offset", int64(event.offset)).
				Int32("partition", event.partition).Str("source", event.topic).
				Err(err).Msg("reprocessor encountered production error")
		} else {
			worker.eventCount.Inc()
		}
	}
}

// Runs a worker.
// Does not return and will watch topics indefinitely even once EOF has been reached.
func (worker *Worker) Run(context context.Context) {
	go worker.doRecv(context)
	go worker.doSend(context)
}

// Creates a new worker.
func NewWorker(topic string, consumer provider.ConsumerInterface, producer *producer.Producer, index int, eof chan int, eventCount prometheus.Counter, eofMetric prometheus.Gauge) Worker {
	model := events.ModelBinary.Str()
	// calculate model type from system topic name
	if st.Events.Reprocess.UpgradeSystem {
		re, err := regexp.Compile(`.*\.system\.(.*)`)
		if err != nil {
			panic("bad regex reprocess system")
		}
		matches := re.FindAllStringSubmatch(topic, 1)
		if matches == nil {
			panic(fmt.Errorf("could not find system topic in %s", topic))
		}
		// first capture group match
		model = matches[0][1]

		// some system topics hold different models to their name
		if remapped, ok := SystemModelRemap[model]; ok {
			model = remapped
		}
	}
	log.Printf("legacy json reprocessor %v", st.Events.Reprocess.LegacyJson)
	produceParams := pipeline.ProduceParams{
		UserAgent:         "reprocessor",
		Model:             events.Model(model),
		AvroNonBulkFormat: !st.Events.Reprocess.LegacyJson, // reading single events at a time from kafka
	}
	return Worker{
		produceParams: produceParams,
		topic:         topic,
		consumer:      consumer,
		producer:      producer,
		index:         index,
		eof:           eof,
		signaledEof:   false,
		transformed:   make(chan WorkerTransformedMessage, 64),
		eventCount:    eventCount,
		eofMetric:     eofMetric,
	}
}
