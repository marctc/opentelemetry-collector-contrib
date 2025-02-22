// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package splunkhecexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
)

// hecEvent is a data structure holding a span event to export explicitly to Splunk HEC.
type hecEvent struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Name       string                 `json:"name"`
	Timestamp  pcommon.Timestamp      `json:"timestamp"`
}

// hecLink is a data structure holding a span link to export explicitly to Splunk HEC.
type hecLink struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	TraceID    string                 `json:"trace_id"`
	SpanID     string                 `json:"span_id"`
	TraceState string                 `json:"trace_state"`
}

// hecSpanStatus is a data structure holding the status of a span to export explicitly to Splunk HEC.
type hecSpanStatus struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

// hecSpan is a data structure used to export explicitly a span to Splunk HEC.
type hecSpan struct {
	TraceID    string                 `json:"trace_id"`
	SpanID     string                 `json:"span_id"`
	ParentSpan string                 `json:"parent_span_id"`
	Name       string                 `json:"name"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	EndTime    pcommon.Timestamp      `json:"end_time"`
	Kind       string                 `json:"kind"`
	Status     hecSpanStatus          `json:"status,omitempty"`
	StartTime  pcommon.Timestamp      `json:"start_time"`
	Events     []hecEvent             `json:"events,omitempty"`
	Links      []hecLink              `json:"links,omitempty"`
}

func mapSpanToSplunkEvent(resource pcommon.Resource, span ptrace.Span, config *Config, logger *zap.Logger) *splunk.Event {
	sourceKey := config.HecToOtelAttrs.Source
	sourceTypeKey := config.HecToOtelAttrs.SourceType
	indexKey := config.HecToOtelAttrs.Index
	hostKey := config.HecToOtelAttrs.Host

	host := unknownHostName
	source := config.Source
	sourceType := config.SourceType
	index := config.Index
	commonFields := map[string]interface{}{}
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case hostKey:
			host = v.Str()
		case sourceKey:
			source = v.Str()
		case sourceTypeKey:
			sourceType = v.Str()
		case indexKey:
			index = v.Str()
		case splunk.HecTokenLabel:
			// ignore
		default:
			commonFields[k] = v.AsString()
		}
		return true
	})

	se := &splunk.Event{
		Time:       timestampToSecondsWithMillisecondPrecision(span.StartTimestamp()),
		Host:       host,
		Source:     source,
		SourceType: sourceType,
		Index:      index,
		Event:      toHecSpan(logger, span),
		Fields:     commonFields,
	}

	return se
}

func toHecSpan(logger *zap.Logger, span ptrace.Span) hecSpan {
	attributes := map[string]interface{}{}
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		attributes[k] = convertAttributeValue(v, logger)
		return true
	})

	links := make([]hecLink, span.Links().Len())
	for i := 0; i < span.Links().Len(); i++ {
		link := span.Links().At(i)
		linkAttributes := map[string]interface{}{}
		link.Attributes().Range(func(k string, v pcommon.Value) bool {
			linkAttributes[k] = convertAttributeValue(v, logger)
			return true
		})
		links[i] = hecLink{
			Attributes: linkAttributes,
			TraceID:    traceutil.TraceIDToHexOrEmptyString(link.TraceID()),
			SpanID:     traceutil.SpanIDToHexOrEmptyString(link.SpanID()),
			TraceState: link.TraceState().AsRaw(),
		}
	}
	events := make([]hecEvent, span.Events().Len())
	for i := 0; i < span.Events().Len(); i++ {
		event := span.Events().At(i)
		eventAttributes := map[string]interface{}{}
		event.Attributes().Range(func(k string, v pcommon.Value) bool {
			eventAttributes[k] = convertAttributeValue(v, logger)
			return true
		})
		events[i] = hecEvent{
			Attributes: eventAttributes,
			Name:       event.Name(),
			Timestamp:  event.Timestamp(),
		}
	}
	status := hecSpanStatus{
		Message: span.Status().Message(),
		Code:    traceutil.StatusCodeStr(span.Status().Code()),
	}
	return hecSpan{
		TraceID:    traceutil.TraceIDToHexOrEmptyString(span.TraceID()),
		SpanID:     traceutil.SpanIDToHexOrEmptyString(span.SpanID()),
		ParentSpan: traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()),
		Name:       span.Name(),
		Attributes: attributes,
		StartTime:  span.StartTimestamp(),
		EndTime:    span.EndTimestamp(),
		Kind:       traceutil.SpanKindStr(span.Kind()),
		Status:     status,
		Links:      links,
		Events:     events,
	}
}
