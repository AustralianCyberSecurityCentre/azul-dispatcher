package pipeline_consume

import (
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/consumer"
)

// rejection constants for FilterConsumerRules
const (
	REJECT_SOURCE            = "reject_source"
	REJECT_EVENT_TYPE        = "reject_event_type"
	REJECT_SELF              = "reject_self"
	REJECT_CONTENT_TOO_LARGE = "reject_content_too_large"
	REJECT_CONTENT_TOO_SMALL = "reject_content_too_small"
	REJECT_STREAM_LEGACY     = "reject_stream_legacy"
	REJECT_FILE_TYPE         = "reject_file_format"
	REJECT_STREAM            = "reject_stream"
	REJECT_WILDCARD_STREAM   = "reject_wildcard_stream"
	REJECT_HAS_NO_CONTENT    = "reject_no_content"
)

type FilterConsumerRules struct{}

func (p *FilterConsumerRules) GetName() string { return "FilterConsumerRules" }

// Filter if the event doesn't match the caller's requested query filter/s (gjson syntax).
func (f *FilterConsumerRules) ConsumeMod(msg *msginflight.MsgInFlight, meta *consumer.ConsumeParams) (string, *msginflight.MsgInFlight) {
	var ok bool
	var ev *events.BinaryEvent
	if binary, ok := msg.GetBinary(); ok {
		ev = binary
	} else if status, ok := msg.GetStatus(); ok {
		ev = &status.Entity.Input
	} else {
		// only binary or status messages are compatible
		return "", msg
	}

	if len(meta.RequireSourcesMap) > 0 {
		// filter based on source
		_, ok = meta.RequireSourcesMap[ev.Source.Name]
		if !ok {
			return REJECT_SOURCE, nil
		}
	}

	if len(meta.RequireEventsMap) > 0 {
		// filter based on event type
		_, ok = meta.RequireEventsMap[ev.Action]
		if !ok {
			return REJECT_EVENT_TYPE, nil
		}
	}

	if len(meta.DenyEventsMap) > 0 {
		// filter based on event type
		_, ok = meta.DenyEventsMap[ev.Action]
		if ok {
			return REJECT_EVENT_TYPE, nil
		}
	}

	// deny self published events
	if meta.DenySelf && ev.Author.Name == meta.Name {
		return REJECT_SELF, nil
	}

	// binary event filters only now
	binary, ok := msg.GetBinary()
	if !ok {
		return "", msg
	}

	dataStreamMapping := map[events.DatastreamLabel]events.BinaryEntityDatastream{}
	hasContent := false
	foundWildCardType := false
	wildCardTypeRequirements, checkWildcardType := meta.RequireStreams["*"]
	if checkWildcardType {
		// Only check the wildcard requirements if any file types are listed.
		checkWildcardType = len(wildCardTypeRequirements) > 0
	}

	for _, data := range binary.Entity.Datastreams {
		if data.Label == events.DataLabelContent {
			hasContent = true
			// filter based on data size
			if meta.RequireUnderContentSize > 0 {
				if data.Size > uint64(meta.RequireUnderContentSize) {
					return REJECT_CONTENT_TOO_LARGE, nil
				}
			}
			if meta.RequireOverContentSize > 0 {
				if data.Size < uint64(meta.RequireOverContentSize) {
					return REJECT_CONTENT_TOO_SMALL, nil
				}
			}
		}
		dataStreamMapping[data.Label] = data
		if checkWildcardType && !foundWildCardType {
			// Check if this stream matches any of the type requirements
			for permittedTypePrefix := range wildCardTypeRequirements {
				if strings.HasPrefix(data.FileFormat, permittedTypePrefix) {
					foundWildCardType = true
				}
			}
		}
	}
	// Has a wildcard stream with type requirements but none of the provided streams match the type requirement.
	if checkWildcardType && !foundWildCardType {
		return REJECT_WILDCARD_STREAM, nil
	}

	// Requires content, but has no content.
	if (meta.RequireContent || meta.RequireUnderContentSize > 0 || meta.RequireOverContentSize > 0) && !hasContent {
		return REJECT_HAS_NO_CONTENT, nil
	}

	// filter based on file type
	if len(meta.RequireStreams) > 0 {
		for requireStream, requireStreamTypeAllowedTypesMap := range meta.RequireStreams {
			// wildcard already handled
			if requireStream == "*" {
				continue
			}
			relevantStream, ok := dataStreamMapping[requireStream]
			if !ok {
				if !ok {
					return REJECT_STREAM, nil
				}
			}

			has_expected_prefix := false
			if len(requireStreamTypeAllowedTypesMap) > 0 {
				for permittedTypePrefix := range requireStreamTypeAllowedTypesMap {
					if strings.HasPrefix(relevantStream.FileFormat, permittedTypePrefix) {
						has_expected_prefix = true
					}
				}
				if !has_expected_prefix {
					return REJECT_FILE_TYPE, nil
				}
			}
		}

	}

	return "", msg
}
