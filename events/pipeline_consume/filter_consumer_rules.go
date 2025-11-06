package pipeline_consume

import (
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
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

	has_content := false
	has_expected_label := 0
	for _, data := range binary.Entity.Datastreams {
		if data.Label == events.DataLabelContent {
			has_content = true
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

		// filter based on file type
		if len(meta.RequireStreams) > 0 {
			fts, ok := meta.RequireStreams[data.Label]
			if !ok {
				// check if wildcarded
				fts, ok = meta.RequireStreams["*"]
				if !ok {
					return REJECT_STREAM, nil
				}
			}
			has_expected_label += 1
			has_expected_prefix := false
			if len(fts) > 0 {
				for k := range fts {
					if strings.HasPrefix(data.FileFormat, k) {
						has_expected_prefix = true
					}
				}
				if !has_expected_prefix {
					return REJECT_FILE_TYPE, nil
				}
			}
		}
	}
	// if multiple stream labels are expected, we must have them all
	if len(meta.RequireStreams) > 1 && has_expected_label < len(meta.RequireStreams) {
		return REJECT_STREAM, nil
	}

	if (meta.RequireContent || meta.RequireUnderContentSize > 0 || meta.RequireOverContentSize > 0) && !has_content {
		return REJECT_HAS_NO_CONTENT, nil
	}

	return "", msg
}
