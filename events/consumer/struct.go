package consumer

import (
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
)

type ConsumeParams struct {
	// user agent of client
	UserAgent string `json:"user_agent"`
	// type of events to return
	Model      events.Model `json:"model"`
	AvroFormat bool         `json:"avro"` // returned events should be in avro format
	// each name+version of a plugin is a separate subscription
	Name    string `json:"name"`
	Version string `json:"version"`
	// the deployment key, an ID grouping together instances of a plugin
	// for the purposes of scaling
	DeploymentKey string `json:"deployment_key,omitempty"`
	// max events to return
	Count int `json:"count"`
	// if cannot reach event count, poll and wait this long before returning partial
	// timeout is measured in seconds.
	Deadline int `json:"deadline"`
	// raise a dequeued event, so if plugin doesn't publish a status event, the event gets reprocessed
	IsTask                  bool                                       `json:"is_task"`
	RequireSources          []string                                   `json:"require_sources"`            // only get events from a particular source
	RequireSourcesMap       map[string]bool                            `json:"require_sources_map"`        // optimisation of ^
	RequireEvents           []events.BinaryAction                      `json:"require_events"`             // only get specific types of events
	RequireEventsMap        map[events.BinaryAction]bool               `json:"require_events_map"`         // optimisation of ^
	RequireContent          bool                                       `json:"require_content"`            // only get events with a stream with label=content
	RequireStreams          map[events.DatastreamLabel]map[string]bool `json:"require_streams_map"`        // only get events with stream label and file type al
	RequireUnderContentSize int                                        `json:"require_under_content_size"` // only get events with label=content over this size
	RequireOverContentSize  int                                        `json:"require_over_content_size"`  // only get events with label=content under this size
	RequireExpedite         bool                                       `json:"require_expedite"`           // receive expedite events - for binary only
	RequireLive             bool                                       `json:"require_live"`               // receive live events
	RequireHistoric         bool                                       `json:"require_historic"`           // receive historic events
	DenyEvents              []events.BinaryAction                      `json:"deny_events"`                // event types for that entity
	DenyEventsMap           map[events.BinaryAction]bool               `json:"deny_events_map"`            // optimisation of ^
	DenySelf                bool                                       `json:"deny_self"`                  // do not receive events published by this same author
}

// Generate the kafka consumer group's prefix
func (p *ConsumeParams) GenerateKafkaPluginPrefix() string {
	return strings.Join([]string{p.Name, p.Version, p.Model.Str()}, "-")
}

// Generate the kafka consumer group key
func (p *ConsumeParams) GenerateKafkaPluginKey() string {
	pluginKey := p.GenerateKafkaPluginPrefix()
	flow := ""
	if p.Model == events.ModelBinary {
		// track by source
		if len(p.RequireSources) > 0 {
			pluginKey += "-" + strings.Join(p.RequireSources, ".")
		} else {
			pluginKey += "-allsrc"
		}

		// track by events
		if len(p.RequireEvents) > 0 {
			actions, err := events.StringsFromActions(p.RequireEvents)
			if err != nil {
				actions = []string{"invalid"}
			}
			pluginKey += "-" + strings.Join(actions, ".")
		} else {
			pluginKey += "-alltype"
		}

		// track by if history required
		if p.RequireExpedite {
			flow += "E"
		}
	}
	if p.RequireLive {
		flow += "L"
	}
	if p.RequireHistoric {
		flow += "H"
	}
	if len(flow) > 0 {
		pluginKey += "-" + flow
	}
	return pluginKey
}
