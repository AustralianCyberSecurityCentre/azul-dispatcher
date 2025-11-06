package pipeline

import (
	"errors"
	"fmt"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
	"github.com/tidwall/gjson"
)

type ModelOutdatedError struct {
	msg string
}

func (e *ModelOutdatedError) Error() string { return e.msg }

// AvroToMsgInFlights turns avro formatted events into msgs in flight.
func AvroToMsgInFlights(event []byte, model events.Model) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, error) {
	resp := models.ResponsePostEvent{
		TotalOk:       0,
		TotalFailures: 0,
		Failures:      []models.ResponsePostEventFailure{},
	}
	msgInFlights := []*msginflight.MsgInFlight{}
	ev, err := msginflight.NewMsgInFlightFromAvro(event, model)
	if err != nil {
		resp.TotalFailures += 1
		resp.Failures = append(resp.Failures, models.ResponsePostEventFailure{
			Error: err.Error(),
			Event: "",
		})
		return nil, nil, err
	}
	msgInFlights = append(msgInFlights, ev)
	return msgInFlights, &resp, nil
}

// JsonListsToMsgInFlights turns json bytes of multiple events into a set of msgs in flight.
func JsonListsToMsgInFlights(event []byte, model events.Model) ([]*msginflight.MsgInFlight, *models.ResponsePostEvent, error) {
	resp := models.ResponsePostEvent{
		TotalOk:       0,
		TotalFailures: 0,
		Failures:      []models.ResponsePostEventFailure{},
	}
	msgInFlights := []*msginflight.MsgInFlight{}
	if len(event) <= 2 {
		return nil, nil, errors.New("event message too small")
	}
	if !gjson.ValidBytes(event) {
		return nil, nil, errors.New("event message invalid json")
	}
	// the http body will either contain a single event message dict or a
	// list of message dicts
	var raws []gjson.Result
	if event[0] == '[' {
		raws = gjson.ParseBytes(event).Array()
	} else {
		raws = []gjson.Result{gjson.ParseBytes(event)}
	}

	// track events that are (in)valid
	for _, parsed := range raws {
		inFlight, err := NewMsgInFlightFromJson([]byte(parsed.Raw), model)
		if err != nil {
			if serr, ok := err.(*ModelOutdatedError); ok {
				// outdated items need to return an error as client is acting badly
				return nil, nil, serr
			}

			resp.TotalFailures += 1
			resp.Failures = append(resp.Failures, models.ResponsePostEventFailure{
				Error: err.Error(),
				Event: parsed.Raw,
			})
			continue
		}
		// these events were sourced from a client (not a pipeline)
		inFlight.FromClient = true
		msgInFlights = append(msgInFlights, inFlight)
		resp.TotalOk += 1
	}
	return msgInFlights, &resp, nil
}

// NewMsgInFlightFromJson converts event json bytes into MsgInFlight.
// If store is not-nil, will ensure that upgrades to content labels are reflected in object store.
func NewMsgInFlightFromJson(message []byte, model events.Model) (*msginflight.MsgInFlight, error) {
	if len(model) == 0 {
		return nil, fmt.Errorf("no model provided")
	}

	// decode message into struct (assume already upgraded)
	mp, err := msginflight.NewMsgInFlightFromJson(message, model)
	if err != nil {
		// upgrade and decode
		return nil, &ModelOutdatedError{"event message version was too old or was corrupt"}
	}

	// check version and run upgrade only if needed
	if *mp.Base.ModelVersion < events.CurrentModelVersion {
		return nil, &ModelOutdatedError{"event message version was too old"}
	}

	// desirable to check that incoming events from clients are actually valid
	err = mp.Event.CheckValid()
	if err != nil {
		return nil, fmt.Errorf("innerNewMsgInFlight %w", err)
	}

	return mp, nil
}
