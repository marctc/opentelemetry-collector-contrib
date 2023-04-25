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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tilinna/clock"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/exceptionsconnector/internal/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

const (
	serviceNameKey     = conventions.AttributeServiceName
	spanKindKey        = "span.kind"   // OpenTelemetry non-standard constant.
	statusCodeKey      = "status.code" // OpenTelemetry non-standard constant.
	metricKeySeparator = string(byte(0))

	defaultDimensionsCacheSize = 1000
)

type metricKey string

type connectorImp struct {
	lock   sync.Mutex
	logger *zap.Logger
	config Config

	metricsConsumer consumer.Metrics

	// Additional dimensions to add to metrics.
	dimensions []dimension

	// The starting time of the data points.
	startTimestamp pcommon.Timestamp

	keyBuf *bytes.Buffer

	exceptions map[metricKey]int
	// An LRU cache of dimension key-value maps keyed by a unique identifier formed by a concatenation of its values:
	// e.g. { "foo/barOK": { "serviceName": "foo", "status_code": "OK" }}
	metricKeyToDimensions *cache.Cache[metricKey, pcommon.Map]

	ticker  *clock.Ticker
	done    chan struct{}
	started bool

	shutdownOnce sync.Once
}

type dimension struct {
	name  string
	value *pcommon.Value
}

func newDimensions(cfgDims []Dimension) []dimension {
	if len(cfgDims) == 0 {
		return nil
	}
	dims := make([]dimension, len(cfgDims))
	for i := range cfgDims {
		dims[i].name = cfgDims[i].Name
		if cfgDims[i].Default != nil {
			val := pcommon.NewValueStr(*cfgDims[i].Default)
			dims[i].value = &val
		}
	}
	return dims
}

func newConnector(logger *zap.Logger, config component.Config, ticker *clock.Ticker) (*connectorImp, error) {
	logger.Info("Building exceptionsconnector")
	cfg := config.(*Config)

	metricKeyToDimensionsCache, err := cache.NewCache[metricKey, pcommon.Map](cfg.DimensionsCacheSize)
	if err != nil {
		return nil, err
	}

	return &connectorImp{
		logger:                logger,
		config:                *cfg,
		startTimestamp:        pcommon.NewTimestampFromTime(time.Now()),
		exceptions:            make(map[metricKey]int),
		dimensions:            newDimensions(cfg.Dimensions),
		keyBuf:                bytes.NewBuffer(make([]byte, 0, 1024)),
		metricKeyToDimensions: metricKeyToDimensionsCache,
		ticker:                ticker,
		done:                  make(chan struct{}),
	}, nil
}

// Start implements the component.Component interface.
func (c *connectorImp) Start(ctx context.Context, _ component.Host) error {
	c.logger.Info("Starting exceptionsconnector")
	c.started = true
	go func() {
		for {
			select {
			case <-c.done:
				return
			case <-c.ticker.C:
				c.exportMetrics(ctx)
			}
		}
	}()
	return nil
}

// Shutdown implements the component.Component interface.
func (c *connectorImp) Shutdown(context.Context) error {
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
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces implements the consumer.Traces interface.
// It aggregates the trace data to generate metrics.
func (c *connectorImp) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	// Forward trace data unmodified and propagate both metrics and trace pipeline errors, if any.
	c.lock.Lock()
	c.aggregateExceptionMetrics(traces)
	c.lock.Unlock()
	return nil
}

func (c *connectorImp) exportMetrics(ctx context.Context) error {
	c.lock.Lock()
	m, err := c.buildExceptionMetrics()
	c.resetAccumulatedMetrics()

	// This component no longer needs to read the metrics once built, so it is safe to unlock.
	c.lock.Unlock()

	if err != nil {
		return err
	}

	if err = c.metricsConsumer.ConsumeMetrics(ctx, m); err != nil {
		c.logger.Error("Failed ConsumeMetrics", zap.Error(err))
		return err
	}
	return nil
}

// getDimensionsByMetricKey gets dimensions from `metricKeyToDimensions` cache.
func (c *connectorImp) getDimensionsByMetricKey(k metricKey) (pcommon.Map, error) {
	if attributeMap, ok := c.metricKeyToDimensions.Get(k); ok {
		return attributeMap, nil
	}
	return pcommon.Map{}, fmt.Errorf("value not found in metricKeyToDimensions cache by key %q", k)
}

// aggregateExceptionMetrics aggregates data from spans to generate metrics.
func (c *connectorImp) aggregateExceptionMetrics(traces ptrace.Traces) {
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
						attr := event.Attributes()

						// Always reset the buffer before re-using.
						c.keyBuf.Reset()
						buildKey(c.keyBuf, serviceName, span, c.dimensions, attr)
						key := metricKey(c.keyBuf.String())
						c.cache(serviceName, span, key, attr)
						c.updateException(key, span.TraceID(), span.SpanID())
					}
				}
			}
		}
	}
}

// buildExceptionMetrics collects the computed raw metrics data, builds the metrics object and
// writes the raw metrics data into the metrics object.
func (c *connectorImp) buildExceptionMetrics() (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
	ilm := m.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ilm.Scope().SetName("exceptionsconnector")

	if err := c.collectExceptions(ilm); err != nil {
		return pmetric.Metrics{}, err
	}

	c.metricKeyToDimensions.RemoveEvictedItems()

	return m, nil
}

// collectExceptions collects the exception metrics data and writes it into the metrics object.
func (c *connectorImp) collectExceptions(ilm pmetric.ScopeMetrics) error {
	mCalls := ilm.Metrics().AppendEmpty()
	mCalls.SetName("exceptions_total")
	mCalls.SetEmptySum().SetIsMonotonic(true)
	mCalls.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dps := mCalls.Sum().DataPoints()
	dps.EnsureCapacity(len(c.exceptions))
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	for key, count := range c.exceptions {
		dpCalls := dps.AppendEmpty()
		dpCalls.SetStartTimestamp(c.startTimestamp)
		dpCalls.SetTimestamp(timestamp)
		dpCalls.SetIntValue(int64(count))

		dimensions, err := c.getDimensionsByMetricKey(key)
		if err != nil {
			return err
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}
	return nil
}

func (c *connectorImp) resetAccumulatedMetrics() {
	c.metricKeyToDimensions.Purge()
}

func (c *connectorImp) updateException(key metricKey, traceID pcommon.TraceID, spanID pcommon.SpanID) {
	_, ok := c.exceptions[key]
	if !ok {
		c.exceptions[key] = 0
	}

	c.exceptions[key]++
}

func (c *connectorImp) buildDimensionKVs(serviceName string, span ptrace.Span, resourceAttrs pcommon.Map) pcommon.Map {
	dims := pcommon.NewMap()
	dims.EnsureCapacity(3 + len(c.dimensions))
	dims.PutStr(serviceNameKey, serviceName)
	dims.PutStr(spanKindKey, traceutil.SpanKindStr(span.Kind()))
	dims.PutStr(statusCodeKey, traceutil.StatusCodeStr(span.Status().Code()))
	for _, d := range c.dimensions {
		if v, ok := getDimensionValue(d, span.Attributes(), resourceAttrs); ok {
			v.CopyTo(dims.PutEmpty(d.name))
		}
	}
	return dims
}

// cache the dimension key-value map for the metricKey if there is a cache miss.
// This enables a lookup of the dimension key-value map when constructing the metric like so:
//
//	LabelsMap().InitFromMap(p.metricKeyToDimensions[key])
func (c *connectorImp) cache(serviceName string, span ptrace.Span, k metricKey, resourceAttrs pcommon.Map) {
	// Use Get to ensure any existing key has its recent-ness updated.
	if _, has := c.metricKeyToDimensions.Get(k); !has {
		c.metricKeyToDimensions.Add(k, c.buildDimensionKVs(serviceName, span, resourceAttrs))
	}
}

func concatDimensionValue(dest *bytes.Buffer, value string, prefixSep bool) {
	if prefixSep {
		dest.WriteString(metricKeySeparator)
	}
	dest.WriteString(value)
}

// buildKey builds the metric key from the service name and span metadata such as kind, status_code and
// will attempt to add any additional dimensions the user has configured that match the span's attributes
// or resource attributes. If the dimension exists in both, the span's attributes, being the most specific, takes precedence.
//
// The metric key is a simple concatenation of dimension values, delimited by a null character.
func buildKey(dest *bytes.Buffer, serviceName string, span ptrace.Span, optionalDims []dimension, resourceAttrs pcommon.Map) {
	concatDimensionValue(dest, serviceName, false)
	concatDimensionValue(dest, span.Name(), true)
	concatDimensionValue(dest, traceutil.SpanKindStr(span.Kind()), true)
	concatDimensionValue(dest, traceutil.StatusCodeStr(span.Status().Code()), true)

	for _, d := range optionalDims {
		if v, ok := getDimensionValue(d, span.Attributes(), resourceAttrs); ok {
			concatDimensionValue(dest, v.AsString(), true)
		}
	}
}

// getDimensionValue gets the dimension value for the given configured dimension.
// It searches through the span's attributes first, being the more specific;
// falling back to searching in resource attributes if it can't be found in the span.
// Finally, falls back to the configured default value if provided.
//
// The ok flag indicates if a dimension value was fetched in order to differentiate
// an empty string value from a state where no value was found.
func getDimensionValue(d dimension, spanAttr pcommon.Map, resourceAttr pcommon.Map) (v pcommon.Value, ok bool) {
	// The more specific span attribute should take precedence.
	if attr, exists := spanAttr.Get(d.name); exists {
		return attr, true
	}
	if attr, exists := resourceAttr.Get(d.name); exists {
		return attr, true
	}
	// Set the default if configured, otherwise this metric will have no value set for the dimension.
	if d.value != nil {
		return *d.value, true
	}
	return v, ok
}
