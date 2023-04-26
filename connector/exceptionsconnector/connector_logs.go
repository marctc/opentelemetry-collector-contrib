// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exceptionsconnector

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/tilinna/clock"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"
)

type connectorLogs struct {
	lock   sync.Mutex
	logger *zap.Logger
	config Config

	logsConsumer consumer.Logs

	ld plog.Logs

	ticker  *clock.Ticker
	done    chan struct{}
	started bool

	shutdownOnce sync.Once
}

func newConnectorLogs(logger *zap.Logger, config component.Config, ticker *clock.Ticker) (*connectorLogs, error) {
	logger.Info("Building exceptionsconnector")
	cfg := config.(*Config)

	return &connectorLogs{
		logger: logger,
		config: *cfg,
		ld:     plog.NewLogs(),
		ticker: ticker,
		done:   make(chan struct{}),
	}, nil
}

// Start implements the component.Component interface.
func (c *connectorLogs) Start(ctx context.Context, _ component.Host) error {
	c.logger.Info("Starting exceptionsconnector")
	c.started = true
	go func() {
		for {
			select {
			case <-c.done:
				return
			case <-c.ticker.C:
				c.exportLogs(ctx)
			}
		}
	}()
	return nil
}

// Shutdown implements the component.Component interface.
func (c *connectorLogs) Shutdown(context.Context) error {
	c.shutdownOnce.Do(func() {
		c.logger.Info("Shutting down exceptions connector")
		if c.started {
			c.logger.Info("Stopping ticker")
			c.ticker.Stop()
			c.done <- struct{}{}
			c.started = false
		}
	})
	return nil
}

// Capabilities implements the consumer interface.
func (c *connectorLogs) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces implements the consumer.Traces interface.
// It aggregates the trace data to generate metrics.
func (c *connectorLogs) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	// Forward trace data unmodified and propagate both metrics and trace pipeline errors, if any.
	c.lock.Lock()
	c.aggregateExceptionMetrics(traces)
	c.lock.Unlock()
	return nil
}

func (c *connectorLogs) exportLogs(ctx context.Context) error {
	if err := c.logsConsumer.ConsumeLogs(ctx, c.ld); err != nil {
		c.logger.Error("Failed ConsumeLogs", zap.Error(err))
		return err
	}
	return nil
}

// aggregateExceptionMetrics aggregates data from spans to generate metrics.
func (c *connectorLogs) aggregateExceptionMetrics(traces ptrace.Traces) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		resourceAttr := rspans.Resource().Attributes()
		serviceAttr, ok := resourceAttr.Get(conventions.AttributeServiceName)
		if !ok {
			continue
		}
		serviceName := serviceAttr.Str()
		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			ils := ilsSlice.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				for l := 0; l < span.Events().Len(); l++ {
					event := span.Events().At(l)
					if event.Name() == "exception" {
						c.attrToLogRecord(serviceName, span.Attributes(), event.Attributes())
					}
				}
			}
		}
	}
}

func (c *connectorLogs) attrToLogRecord(serviceName string, spanAttr, eventAttr pcommon.Map) plog.LogRecord {
	rl := c.ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()

	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.SetSeverityNumber(plog.SeverityNumberError)
	logRecord.SetSeverityText("ERROR")

	logRecord.Attributes().PutStr("service.name", serviceName)
	logRecord.Attributes().PutStr("exception.stacktrace", getValue(eventAttr, "exception.stacktrace"))
	logRecord.Attributes().PutStr("exception.type", getValue(eventAttr, "exception.type"))
	logRecord.Attributes().PutStr("exception.message", getValue(eventAttr, "exception.message"))

	for k, v := range extractHTTP(spanAttr) {
		logRecord.Attributes().PutStr(k, v)
	}
	return logRecord
}

// extractHTTP extracts the HTTP context from span attributes.
func extractHTTP(attr pcommon.Map) map[string]string {
	http := make(map[string]string)
	attr.Range(func(k string, v pcommon.Value) bool {
		if strings.HasPrefix(k, "http.") {
			http[k] = v.Str()
		}
		return true
	})
	return http
}

// getValue returns the value of the attribute with the given key.
func getValue(attr pcommon.Map, key string) string {
	if attrVal, ok := attr.Get(key); ok {
		return attrVal.Str()
	}
	return ""
}
