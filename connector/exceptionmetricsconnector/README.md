# Exception Metrics Connector

| Status                   |               |
| ------------------------ |---------------|
| Stability                | [development] |
| Supported pipeline types | See [Supported Pipeline Types](#supported-pipeline-types)  |
| Distributions            | [contrib]     |

## Supported Pipeline Types

| [Exporter Pipeline Type] | [Receiver Pipeline Type] |
| ------------------------ | ------------------------ |
| traces                   | metrics                  |

Extract metrics from [recorded exceptions](https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/exceptions/) from spans.

## Overview

Each metric will have _at least_ the following dimensions because they are common across all spans:
- Service name
- Span kind
- Status code
- Exception message
- Exception type

This connector lets traces to continue through the pipeline unmodified.

## Configurations

If you are not already familiar with connectors, you may find it helpful to first visit the [Connectors README].

The following settings can be optionally configured:
- `dimensions`: the list of dimensions to add together with the default dimensions defined above.
  
  Each additional dimension is defined with a `name` which is looked up in the span's collection of attributes or resource attributes.

- `dimensions_cache_size`: the max items number of `metric_key_to_dimensions_cache`. If not provided, will use default value size `1000`.

## Examples

The following is a simple example usage of the `exceptionmetrics` connector.

For configuration examples on other use cases, please refer to [More Examples](#more-examples).

The full list of settings exposed for this connector are documented [here](../../connector/exceptionmetrics/config.go).


```yaml
receivers:
  nop:

exporters:
  nop:

connectors:
  exceptionmetrics:

service:
  pipelines:
    traces:
      receivers: [nop]
      exporters: [exceptionmetrics]
    metrics:
      receivers: [exceptionmetrics]
      exporters: [nop]
```

### More Examples

For more example configuration covering various other use cases, please visit the [testdata directory](../../connector/exceptionmetricsconnector/testdata).

[development]: https://github.com/open-telemetry/opentelemetry-collector#development
[contrib]:https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
[Connectors README]:https://github.com/open-telemetry/opentelemetry-collector/blob/main/connector/README.md