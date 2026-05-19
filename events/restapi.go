/*
Package events provides functionality for handling Azul events via Kafka.

Handles partitioning, producing and consuming events through Kafka topics.
*/
package events

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/client/getevents"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/client/postevents"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/manager"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline_dual"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	saramago "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi/restapi_handlers"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
)

// GetEventsPassive pulls messages from Kafka for the requested plugin name + version, and does not expect completion acknowledgements.
func (ev *Events) GetEventsPassive(c *gin.Context) {
	// When using this API you cannot do so as a task.
	qv := c.Request.URL.Query()
	qv.Del("is-task")

	c.Request.URL.RawQuery = qv.Encode()
	ev.getEvents(ev.consumerManager.FetchEventsPassive, c)
}

func (ev *Events) GetTopicEvents(c *gin.Context) {
	// When using this API you cannot do so as a task.
	qv := c.Request.URL.Query()
	c.Request.URL.RawQuery = qv.Encode()
	ev.getEvents(ev.consumerManager.FetchEventsPassive, c)
}

// GetEventsActiveImplicit pulls messages from Kafka for the requested plugin name + version.
func (ev *Events) GetEventsActiveImplicit(c *gin.Context) {
	// When using this API you must do so as a task
	qv := c.Request.URL.Query()
	qv.Set("is-task", "true")
	c.Request.URL.RawQuery = qv.Encode()
	ev.getEvents(ev.consumerManager.FetchEventsActive, c)
}

// helper for breaking up csv
func comma(c rune) bool {
	return c == ','
}

// get value from the query string, replace empty string with alt parameter
func getWithDefault(qv url.Values, key string, alt string) string {
	ret := qv.Get(key)
	if ret == "" {
		ret = alt
	}
	return ret
}

// format a message describing a bad parameter
func fmtBadParam(key string, err error) error {
	return fmt.Errorf("bad parameter for %s: %s", key, err.Error())
}

func parseParams(c *gin.Context) (*consumer.ConsumeParams, error) {
	var err error
	qv := c.Request.URL.Query()
	p := consumer.ConsumeParams{}

	p.UserAgent = c.GetHeader("user-agent")
	p.Model = events.Model(c.Params.ByName("model")) // set in route.go
	// ensure asking for a valid model type
	if !events.IsValidModel(p.Model) {
		return nil, fmtBadParam("model", fmt.Errorf("not a registered model type: %s", p.Model))
	}
	p.AvroFormat, err = strconv.ParseBool(getWithDefault(qv, getevents.AvroFormat, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.AvroFormat, err)
	}

	p.Name = qv.Get(getevents.Name)
	p.Version = qv.Get(getevents.Version)
	p.DeploymentKey = qv.Get(getevents.DeploymentKey)
	p.RequireExpedite, err = strconv.ParseBool(getWithDefault(qv, getevents.RequireExpedite, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireExpedite, err)
	}
	if p.Model != events.ModelBinary && p.RequireExpedite {
		return nil, fmt.Errorf("can only %s with binary events", getevents.RequireExpedite)
	}
	p.RequireLive, err = strconv.ParseBool(getWithDefault(qv, getevents.RequireLive, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireLive, err)
	}
	p.RequireHistoric, err = strconv.ParseBool(getWithDefault(qv, getevents.RequireHistoric, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireHistoric, err)
	}
	if !p.RequireExpedite && !p.RequireLive && !p.RequireHistoric {
		return nil, fmt.Errorf("requires at least one of %s, %s or %s",
			getevents.RequireExpedite, getevents.RequireLive, getevents.RequireHistoric,
		)
	}
	p.Count, err = strconv.Atoi(getWithDefault(qv, getevents.Count, "1"))
	if err != nil {
		return nil, fmtBadParam(getevents.Count, err)
	}
	p.Deadline, err = strconv.Atoi(getWithDefault(qv, getevents.Deadline, fmt.Sprintf("%d", st.Events.APIDefaultEventWait)))
	if err != nil {
		return nil, fmtBadParam(getevents.Deadline, err)
	}

	p.IsTask, err = strconv.ParseBool(getWithDefault(qv, getevents.IsTask, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.IsTask, err)
	}
	p.RequireSources = qv[getevents.RequireSources]
	if len(p.RequireSources) > 0 {
		// fast lookup
		p.RequireSourcesMap = map[string]bool{}
		for _, v := range p.RequireSources {
			p.RequireSourcesMap[v] = true
		}
	}

	// fixups
	if p.Count <= 0 {
		p.Count = 1
	} else if p.Count > st.Events.APIEventFetchLimit {
		p.Count = st.Events.APIEventFetchLimit
	}

	// additional filters
	p.RequireUnderContentSize, err = strconv.Atoi(getWithDefault(qv, getevents.RequireUnderContentSize, "0"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireUnderContentSize, err)
	}

	p.RequireOverContentSize, err = strconv.Atoi(getWithDefault(qv, getevents.RequireOverContentSize, "0"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireOverContentSize, err)
	}
	// Verify that if maxContentSize is set minContentSize is less than it's value.
	if p.RequireUnderContentSize > 0 && p.RequireOverContentSize > p.RequireUnderContentSize {
		return nil, fmtBadParam(getevents.RequireOverContentSize, fmt.Errorf(
			"minimum content size cannot be greater than max content size. min: '%d' max: '%d'",
			p.RequireOverContentSize, p.RequireUnderContentSize,
		))
	}

	// keep specified event types
	requireActions := qv[getevents.RequireActions]
	denyActions := qv[getevents.DenyActions]
	p.RequireContent, err = strconv.ParseBool(getWithDefault(qv, getevents.RequireContent, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.RequireContent, err)
	}
	if p.RequireContent && (len(requireActions) > 0 || len(denyActions) > 0) {
		return nil, fmt.Errorf("cannot combine %s with %s or %s", getevents.RequireActions, getevents.RequireContent, getevents.DenyActions)
	}
	if p.RequireContent {
		requireActions = []string{events.ActionSourced.Str(), events.ActionExtracted.Str()}
	}
	if len(requireActions) > 0 {
		// check eventTypes are valid
		p.RequireEvents, err = events.ActionsFromStrings(requireActions)
		if err != nil {
			return nil, fmtBadParam(getevents.RequireActions, err)
		}

		// fast lookup
		p.RequireEventsMap = map[events.BinaryAction]bool{}
		for _, v := range p.RequireEvents {
			p.RequireEventsMap[v] = true
		}
	}

	// remove specified event types
	if len(denyActions) > 0 {
		// check eventTypes are valid
		p.DenyEvents, err = events.ActionsFromStrings(denyActions)
		if err != nil {
			return nil, fmtBadParam(getevents.RequireActions, err)
		}

		// fast lookup
		p.DenyEventsMap = map[events.BinaryAction]bool{}
		for _, v := range p.DenyEvents {
			p.DenyEventsMap[v] = true
		}
	}

	p.DenySelf, err = strconv.ParseBool(getWithDefault(qv, getevents.DenySelf, "false"))
	if err != nil {
		return nil, fmtBadParam(getevents.DenySelf, err)
	}

	// list of label:ft1,ft2,ft3
	dataTypes := qv[getevents.RequireStreams]
	if len(dataTypes) > 0 {
		p.RequireStreams = map[events.DatastreamLabel]map[string]bool{}
		for _, dataType := range dataTypes {
			parsed := strings.FieldsFunc(dataType, comma)
			if len(parsed) <= 0 {
				continue
			}
			stream := events.DatastreamLabel(parsed[0])
			p.RequireStreams[stream] = map[string]bool{}
			for _, ft := range parsed[1:] {
				p.RequireStreams[stream][ft] = true
			}
		}
	}

	return &p, nil
}

func (ev *Events) getEvents(fetchEvents manager.FetchEvents, c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "application/json")

	// get info such as client plugin and how many events are needed
	p, err := parseParams(c)
	if err != nil {
		restapi_handlers.JSONError(c, 422, "bad parameters", err)
		return
	}

	err = ev.storeConsumerInRedis(p)
	if err != nil {
		err2 := fmt.Errorf("failed to store consumer in redis: %w", err)
		restapi_handlers.JSONError(c, 422, "bad parameters", err2)
		return
	}

	// fetch required events from kafka
	var evs []*msginflight.MsgInFlight
	evs, info, err := fetchEvents(p)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Fetch Failed", err)
		return
	}
	respInfo, err := json.Marshal(info)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "info marshal Failed", err)
		return
	}

	// prepare avro or json encoded events
	var respEvents []byte
	if p.AvroFormat {
		respEvents, err = msginflight.MsgInFlightsToAvroBulk(evs, p.Model)
		if err != nil {
			restapi_handlers.JSONError(c, 500, "events MsgInFlightsToAvroBulk Failed", err)
			return
		}
	} else {
		jsonEvents := make([][]byte, 0, len(evs))
		// try encoding individually and reporting a bad example
		var badEvent *msginflight.MsgInFlight
		countBad := 0
		countGood := 0
		for _, b := range evs {
			raw, err := b.MarshalJSON()
			if err != nil {
				countBad += 1
				badEvent = b
				continue
			}
			countGood += 1
			jsonEvents = append(jsonEvents, raw)
		}
		if countBad > 0 {
			err := fmt.Errorf("%w - total events good %d and bad %d. Example bad (if found): %v", err, countGood, countBad, badEvent)
			restapi_handlers.JSONError(c, 500, "Marshalling Failed", err)
			return
		}
		respEvents = []byte(`{"events":[` + string(bytes.Join(jsonEvents, []byte(","))) + `]}`)
	}

	// prepare multipart form response
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	var fw io.Writer
	// write info (filename doesn't matter)
	fw, err = w.CreateFormFile(getevents.RespInfo, "info.json")
	if err != nil {
		restapi_handlers.JSONError(c, 500, "CreateFormFile1 Failed", err)
		return
	}
	_, err = fw.Write(respInfo)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Write1 Failed", err)
		return
	}
	// write events (filename doesn't matter)
	fw, err = w.CreateFormFile(getevents.RespEvents, "events.json")
	if err != nil {
		restapi_handlers.JSONError(c, 500, "CreateFormFile2 Failed", err)
		return
	}
	_, err = fw.Write(respEvents)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Write2 Failed", err)
		return
	}
	// cook form into bytes with correct content-type boundary
	w.Close()
	c.Writer.Header().Set("Content-Type", w.FormDataContentType())
	respBody, err := io.ReadAll(&b)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "ReadAll Failed", err)
		return
	}

	// write data to response
	_, err = c.Writer.Write(respBody)
	if err != nil {
		bedSet.Logger.Err(err).Msg("failed Write")
	}
}

// PostEvent will publish events received over HTTP to Kafka.
func (ev *Events) PostEvent(c *gin.Context) {
	defer c.Request.Body.Close()
	qv := c.Request.URL.Query()
	sync := qv.Get(postevents.Sync) == "true" || qv.Get(postevents.Sync) == "1"
	includeOkInResp := qv.Get(postevents.IncludeOk) == "true" || qv.Get(postevents.IncludeOk) == "1"
	avroFormat := qv.Get(postevents.AvroFormat) == "true" || qv.Get(postevents.AvroFormat) == "1"
	isPausePluginProcessing := qv.Get(postevents.PausePlugins) == "true" || qv.Get(postevents.PausePlugins) == "1"
	if isPausePluginProcessing {
		// Trigger plugin pausing in a background task to prevent this task slowing down producing events.
		_ = pauser.BackgroundPauseProcessing(c.Request.Context(), ev.kvstore)
	}
	// ensure posting with a valid model type
	model := events.Model(qv.Get(postevents.Model))
	if !events.IsValidModel(model) {
		err := fmt.Errorf("not a registered model type: %s", model)
		restapi_handlers.JSONError(c, 400, "not a registered model type", err)
		return
	}

	promTimeStart := time.Now().UnixNano()
	c.Writer.Header().Set("Content-Type", "application/json")

	produceParams := pipeline.ProduceParams{
		UserAgent:  c.GetHeader("user-agent"),
		Model:      model,
		AvroFormat: avroFormat,
	}

	// still need to read entire body into mem but try to reuse buffer
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(c.Request.Body)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Couldn't read request body", err)
		return
	}
	b := buf.Bytes()
	if len(b) == 0 {
		restapi_handlers.JSONError(c, 400, "Empty Request", errors.New("no events were provided to be published"))
		return
	}

	// are we going to block for message confirmation/receipt?
	promTimeAfterBuffer := time.Now().UnixNano()
	prom.EventsProduceStagesDuration.WithLabelValues("buffer").Observe(float64(promTimeAfterBuffer-promTimeStart) / 1e9)

	to_publish, response, produceActionInfo, err := ev.producer.TransformEvents(b, &produceParams)
	if err != nil {
		restapi_handlers.JSONError(c, 422, "PostEvent TransformEvents invalid events", err)
		return
	}
	promTimeAfterTransform := time.Now().UnixNano()
	prom.EventsProduceStagesDuration.WithLabelValues("transform").Observe(float64(promTimeAfterTransform-promTimeAfterBuffer) / 1e9)

	if len(to_publish) > 0 {
		err = ev.producer.ProduceAnyEvents(sync, to_publish)
		if err != nil {
			bedSet.Logger.Err(err).Msg("failed ProduceAnyEvents")
			status := 500
			var badpart *producer.BadPartitionSelection
			if errors.As(err, &badpart) {
				status = 400
			}
			restapi_handlers.JSONError(c, status, "failed ProduceAnyEvents", err)
			return
		}
	} else {
		title := "All events to be published have been filtered out"
		eventsFilteredPerPipelineFilter := []string{}
		for k, v := range produceActionInfo.ProducersThatDroppedEvents {
			eventsFilteredPerPipelineFilter = append(eventsFilteredPerPipelineFilter, fmt.Sprintf("%s:%d", k, v))
		}
		respFailures := []string{}
		totalFailures := -1
		if response != nil {
			// Just take the error section of the response failure for the event.
			for fail_index := range response.Failures {
				respFailures = append(respFailures, response.Failures[fail_index].Error)
			}
			totalFailures = response.TotalFailures
		}

		usedFilters := strings.Join(eventsFilteredPerPipelineFilter, ",")
		baseError := fmt.Errorf("failed to produce any of the provided events as they were all filtered out by the following filters [%s], with %d response failures which are: %v", usedFilters, totalFailures, respFailures)
		errorEnum := models.ErrorStringEnumAllEventsFiltered
		errorParams := map[string]string{
			"filters":           usedFilters,
			"total_failures":    strconv.Itoa(totalFailures),
			"response_failures": fmt.Sprintf("%v", respFailures),
		}
		if _, ok := produceActionInfo.ProducersThatDroppedEvents[pipeline_dual.PipelineAgeOffName]; ok {
			baseError = errors.New("all submitted events aged off immediately, is the submission time older than ageoff?")
			errorParams = map[string]string{}
			errorEnum = models.ErrorStringEnumAllEventsAgedOffImmediately
		}
		restapi_handlers.JSONErrorWithEnum(
			c,
			425, // Too Early - Indicates that the server is unwilling to risk processing a request that might be replayed.
			title,
			baseError,
			errorEnum,
			errorParams,
		)
		return
	}

	if includeOkInResp {
		// Include in response the finalised messages sourced from client.
		// Used to inform metastore of the tracking information for the event,
		// so it can be posted immediately to opensearch
		response.Ok = []any{}
		for _, tp := range to_publish {
			if !tp.FromClient {
				continue
			}
			response.Ok = append(response.Ok, tp)
		}
	}

	out, err := json.Marshal(response)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Marshalling Failed", err)
		return
	}

	// write data to response
	_, err = c.Writer.Write(out)
	if err != nil {
		bedSet.Logger.Err(err).Msg("failed Write")
	}

	promTimeAfterProduce := time.Now().UnixNano()
	prom.EventsProduceStagesDuration.WithLabelValues("produce").Observe(float64(promTimeAfterProduce-promTimeAfterTransform) / 1e9)
}

// PostEventSimulate will simulate consumer filtering for all plugins..
func (ev *Events) PostEventSimulate(c *gin.Context) {
	defer c.Request.Body.Close()
	// promTimeStart := time.Now().UnixNano()
	c.Writer.Header().Set("Content-Type", "application/json")

	// produceParams := pipeline.ProduceParams{UserAgent: c.GetHeader("user-agent")}

	// still need to read entire body into mem but try to reuse buffer
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(c.Request.Body)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Couldn't read request body", err)
		return
	}
	b := buf.Bytes()

	simulation, err := ev.eventSimulate(b)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Failed to simulate event", err)
		return
	}

	out, err := json.Marshal(simulation)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "Failed to marshal response", err)
		return
	}

	// write data to response
	_, err = c.Writer.Write(out)
	if err != nil {
		bedSet.Logger.Err(err).Msg("failed Write")
	}
}

// GetDebugTopicEvents is a debugging endpoint that returns events from a topic starting at a specified offset.
// This endpoint does not consume events (no consumer group tracking) and is intended for debugging and inspection.
/*
TODO:
- get number of partitions based on topic if no partition specified (use admin client?)
- format events properly, including all info returned by kafka
- implement limit DONE
- fix offset specification to support earliest, latest and numeric offsets DONE
- figure out how to retrieve broker addresses from config rather than hardcoding DONE
   SaramaProvider is the provider interface stored by events. So we just need to add a method to the interface
   and implement the method for SaramaProvider to retrieve whatever config is needed for this endpoint.
   MemoryProvider is the only other provider, so it will need a method too.
*/
func (ev *Events) GetDebugTopicEvents(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "application/json")

	var err error = nil

	// In practice this will only ever be a SaramaProvider, not a MemoryProvider.
	prov := ev.prov.(*provider.SaramaKafkaProvider)

	qv := c.Request.URL.Query()

	// Parse required parameters
	topic := qv.Get("topic")
	if topic == "" {
		restapi_handlers.JSONError(c, 400, "missing required parameter", fmt.Errorf("topic is required"))
		return
	}

	offset := sarama.OffsetOldest
	offsetStr := qv.Get("offset")
	if offsetStr == "" {
		restapi_handlers.JSONError(c, 400, "missing required parameter", fmt.Errorf("offset is required (earliest, latest, or numeric value)"))
		return
	}

	// sarama.OffsetOldest is default (see above)
	if offsetStr == "newest" {
		offset = sarama.OffsetNewest
	}

	if offsetStr != "oldest" && offsetStr != "newest" {
		if offset, err = strconv.ParseInt(offsetStr, 10, 64); err != nil {
			restapi_handlers.JSONError(c, 400, "invalid offset format", fmt.Errorf("offset must be 'earliest', 'latest', or a numeric value"))
			return
		}
	}

	// Parse optional parameters
	partition := int32(0)
	partitionStr := qv.Get("partition")
	if partitionStr != "" {
		p, err := strconv.ParseInt(partitionStr, 10, 32)
		if err != nil {
			restapi_handlers.JSONError(c, 400, "invalid partition format", fmt.Errorf("partition must be a numeric value"))
			return
		}
		partition = int32(p)
	}

	limit := int(^uint(0) >> 1) // int_max determined at runtime based on system width
	limitStr := qv.Get("limit")
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil {
			restapi_handlers.JSONError(c, 400, "invalid limit format", fmt.Errorf("limit must be a numeric value"))
			return
		}
		limit = l
	}
	fmt.Println("debug get topic events with topic:", topic, "offset:", offset, "partition:", partition, "limit:", limit)

	broker := []string{prov.GetBootstrap()}
	var topicMap map[string]sarama.TopicDetail
	topicMap, err = saramago.GetTopicDetailsMap(broker)


    // either one topic was specified and we get info for that topic or we get info for all topics if none specified
	if topic != "" {
		if topicDetails, ok := topicMap[topic]; !ok {

			restapi_handlers.JSONError(c, 404, "topic not found", fmt.Errorf("topic must correspond to a topic known by the broker"))

		}
	}

	// What happens if we get no partition WITH a numeric offset?
	//  -> probably should just error on this
	// other permutations of offset, topic, and partition?
	// offset by itself: get all topics on all partitions from oldest/newest (error on numeric)
	// offset with topic: ok if not numeric, just get all events for that topic on all partitions
	// offset with topic and partition: get exactly this, numeric is ok
	// topic by itself: default to oldest offset and all partitions
	// topic and partition: default to oldest offset on single partition for specific topic
	// just partition: get events for all topics that have events on that partition(?) from oldest

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = prov.GetKafkaVersion()

	// Create consumer
	fmt.Println(prov.GetBootstrap())
	consumer, err := sarama.NewConsumer(broker, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// Consume from partition 0, starting at the oldest offset
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Fatalf("Error starting partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	latestOffset := partitionConsumer.HighWaterMarkOffset() - 1

	// Define event item structure for response
	type EventItem struct {
		Offset int64  `json:"offset"`
		Value  string `json:"value"`
	}

	response := map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
		"events":    []EventItem{},
		"info": map[string]interface{}{
			"count": 0,
		},
	}

	eventsList := make([]EventItem, 0, limit)
	fmt.Println("about to loop on messages")
	fmt.Println(latestOffset)
	if latestOffset > 0 { // Loop hangs if we don't first check this
		count := 0
		for msg := range partitionConsumer.Messages() {

			fmt.Println("loop")
			var value string
			if msg.Value != nil {
				value = string(msg.Value)
			}

			eventsList = append(eventsList, EventItem{
				Offset: msg.Offset,
				Value:  value,
			})
			count++

			if msg.Offset >= latestOffset || count == limit {
				fmt.Println("Reached latest offset, ending poll")
				break
			}
		}
	}

	response["events"] = eventsList
	response["info"].(map[string]interface{})["count"] = len(eventsList)

	// Marshal and write response
	out, err := json.Marshal(response)
	if err != nil {
		restapi_handlers.JSONError(c, 500, "failed to marshal response", err)
		return
	}

	_, err = c.Writer.Write(out)
	if err != nil {
		bedSet.Logger.Err(err).Msg("failed to write response")
	}
}
