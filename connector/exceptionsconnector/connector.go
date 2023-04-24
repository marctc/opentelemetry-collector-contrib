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
func (p *connectorImp) Start(ctx context.Context, _ component.Host) error {
	p.logger.Info("Starting exceptionsconnector")
	// exporters := host.GetExporters()

	// var availableMetricsExporters []string

	// // The available list of exporters come from any configured metrics pipelines' exporters.
	// for k, exp := range exporters[component.DataTypeMetrics] {
	// 	metricsExp, ok := exp.(exporter.Metrics)
	// 	if !ok {
	// 		return fmt.Errorf("the exporter %q isn't a metrics exporter", k.String())
	// 	}

	// 	availableMetricsExporters = append(availableMetricsExporters, k.String())

	// 	p.logger.Debug("Looking for exceptions exporter from available exporters",
	// 		zap.String("exceptions-exporter", p.config.MetricsExporter),
	// 		zap.Any("available-exporters", availableMetricsExporters),
	// 	)
	// 	if k.String() == p.config.MetricsExporter {
	// 		p.metricsConsumer = metricsExp
	// 		p.logger.Info("Found exporter", zap.String("exceptions-exporter", p.config.MetricsExporter))
	// 		break
	// 	}
	// }
	// if p.metricsConsumer == nil {
	// 	return fmt.Errorf("failed to find metrics exporter: '%s'; please configure metrics_exporter from one of: %+v",
	// 		p.config.MetricsExporter, availableMetricsExporters)
	// }
	// p.logger.Info("Started exceptionsconnector")
	p.started = true
	go func() {
		for {
			select {
			case <-p.done:
				return
			case <-p.ticker.C:
				p.exportMetrics(ctx)
			}
		}
	}()
	return nil
}

// Shutdown implements the component.Component interface.
func (p *connectorImp) Shutdown(context.Context) error {
	p.shutdownOnce.Do(func() {
		p.logger.Info("Shutting down exceptions connector")
		if p.started {
			p.logger.Info("Stopping ticker")
			p.ticker.Stop()
			p.done <- struct{}{}
			p.started = false
		}
	})
	return nil
}

// Capabilities implements the consumer interface.
func (p *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces implements the consumer.Traces interface.
// It aggregates the trace data to generate metrics.
func (p *connectorImp) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	// Forward trace data unmodified and propagate both metrics and trace pipeline errors, if any.
	p.lock.Lock()
	p.aggregateExceptionMetrics(traces)
	p.lock.Unlock()
	return nil
	//return multierr.Combine(p.tracesToMetrics(ctx, traces), p.nextConsumer.ConsumeTraces(ctx, traces))
}

func (p *connectorImp) exportMetrics(ctx context.Context) error {
	p.lock.Lock()

	//p.aggregateExceptionMetrics(traces)
	m, err := p.buildExceptionMetrics()
	p.resetAccumulatedMetrics()

	// This component no longer needs to read the metrics once built, so it is safe to unlock.
	p.lock.Unlock()

	if err != nil {
		return err
	}

	if err = p.metricsConsumer.ConsumeMetrics(ctx, m); err != nil {
		p.logger.Error("Failed ConsumeMetrics", zap.Error(err))
		return err
	}
	return nil
}

// getDimensionsByMetricKey gets dimensions from `metricKeyToDimensions` cache.
func (p *connectorImp) getDimensionsByMetricKey(k metricKey) (pcommon.Map, error) {
	if attributeMap, ok := p.metricKeyToDimensions.Get(k); ok {
		return attributeMap, nil
	}
	return pcommon.Map{}, fmt.Errorf("value not found in metricKeyToDimensions cache by key %q", k)
}

// aggregateExceptionMetrics aggregates data from spans to generate metrics.
func (p *connectorImp) aggregateExceptionMetrics(traces ptrace.Traces) {
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
						p.keyBuf.Reset()
						buildKey(p.keyBuf, serviceName, span, p.dimensions, attr)
						key := metricKey(p.keyBuf.String())
						p.cache(serviceName, span, key, attr)
						p.updateException(key, span.TraceID(), span.SpanID())
					}
				}
			}
		}
	}
}

// buildExceptionMetrics collects the computed raw metrics data, builds the metrics object and
// writes the raw metrics data into the metrics object.
func (p *connectorImp) buildExceptionMetrics() (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
	ilm := m.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ilm.Scope().SetName("exceptionsconnector")

	if err := p.collectExceptions(ilm); err != nil {
		return pmetric.Metrics{}, err
	}

	p.metricKeyToDimensions.RemoveEvictedItems()

	return m, nil
}

// collectExceptions collects the exception metrics data and writes it into the metrics object.
func (p *connectorImp) collectExceptions(ilm pmetric.ScopeMetrics) error {
	mCalls := ilm.Metrics().AppendEmpty()
	mCalls.SetName("exceptions_total")
	mCalls.SetEmptySum().SetIsMonotonic(true)
	mCalls.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dps := mCalls.Sum().DataPoints()
	dps.EnsureCapacity(len(p.exceptions))
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	for key, count := range p.exceptions {
		dpCalls := dps.AppendEmpty()
		dpCalls.SetStartTimestamp(p.startTimestamp)
		dpCalls.SetTimestamp(timestamp)
		dpCalls.SetIntValue(int64(count))

		dimensions, err := p.getDimensionsByMetricKey(key)
		if err != nil {
			return err
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}
	return nil
}

func (p *connectorImp) resetAccumulatedMetrics() {
	p.metricKeyToDimensions.Purge()
}

func (p *connectorImp) updateException(key metricKey, traceID pcommon.TraceID, spanID pcommon.SpanID) {
	_, ok := p.exceptions[key]
	if !ok {
		p.exceptions[key] = 0
	}

	p.exceptions[key]++
}

func (p *connectorImp) buildDimensionKVs(serviceName string, span ptrace.Span, resourceAttrs pcommon.Map) pcommon.Map {
	dims := pcommon.NewMap()
	dims.EnsureCapacity(3 + len(p.dimensions))
	dims.PutStr(serviceNameKey, serviceName)
	dims.PutStr(spanKindKey, traceutil.SpanKindStr(span.Kind()))
	dims.PutStr(statusCodeKey, traceutil.StatusCodeStr(span.Status().Code()))
	for _, d := range p.dimensions {
		if v, ok := getDimensionValue(d, span.Attributes(), resourceAttrs); ok {
			v.CopyTo(dims.PutEmpty(d.name))
		}
	}
	return dims
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

// cache the dimension key-value map for the metricKey if there is a cache miss.
// This enables a lookup of the dimension key-value map when constructing the metric like so:
//
//	LabelsMap().InitFromMap(p.metricKeyToDimensions[key])
func (p *connectorImp) cache(serviceName string, span ptrace.Span, k metricKey, resourceAttrs pcommon.Map) {
	// Use Get to ensure any existing key has its recent-ness updated.
	if _, has := p.metricKeyToDimensions.Get(k); !has {
		p.metricKeyToDimensions.Add(k, p.buildDimensionKVs(serviceName, span, resourceAttrs))
	}
}

// // copied from prometheus-go-metric-exporter
// // sanitize replaces non-alphanumeric characters with underscores in s.
// func sanitize(s string, skipSanitizeLabel bool) string {
// 	if len(s) == 0 {
// 		return s
// 	}

// 	// Note: No length limit for label keys because Prometheus doesn't
// 	// define a length limit, thus we should NOT be truncating label keys.
// 	// See https://github.com/orijtech/prometheus-go-metrics-exporter/issues/4.
// 	s = strings.Map(sanitizeRune, s)
// 	if unicode.IsDigit(rune(s[0])) {
// 		s = "key_" + s
// 	}
// 	// replace labels starting with _ only when skipSanitizeLabel is disabled
// 	if !skipSanitizeLabel && strings.HasPrefix(s, "_") {
// 		s = "key" + s
// 	}
// 	// labels starting with __ are reserved in prometheus
// 	if strings.HasPrefix(s, "__") {
// 		s = "key" + s
// 	}
// 	return s
// }

// // copied from prometheus-go-metric-exporter
// // sanitizeRune converts anything that is not a letter or digit to an underscore
// func sanitizeRune(r rune) rune {
// 	if unicode.IsLetter(r) || unicode.IsDigit(r) {
// 		return r
// 	}
// 	// Everything else turns into an underscore
// 	return '_'
// }
