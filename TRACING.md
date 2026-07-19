# Tracing

Tracing is part of the standalone `reactive-pipeline-core` module and uses OpenTelemetry with OTLP/HTTP export to Tempo.

## Trace model

- Every Kafka poll creates a root `consume_kafka_batch` consumer span.
- Incoming message contexts are attached to that span as links, capped by `MAX_BATCH_SPAN_LINKS`.
- Item operations extract the originating Kafka context and create consumer spans with `pokemon.id` and correlation attributes.
- Outbound Kafka records receive W3C `traceparent` headers plus domain correlation headers.
- Errors are recorded on the active span and mark it as `ERROR`.

Correlation headers and attributes retained from the original implementation:

| Name | Meaning |
|---|---|
| `pokemon.id` | Item ID |
| `json.trace_id` / `jsonTrace` | Split/source trace correlation |
| `splitter.id` / `spillerid` | Split batch correlation |
| `split.ts.ms` | Split timestamp for latency analysis |
| `batch.trace_id` / `batch.span_id` | Batch relationship where applicable |

Configure tracing with:

```bash
TRACING_ENABLE=true
SERVICE_NAME=splitter
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://tempo:4318/v1/traces
MAX_BATCH_SPAN_LINKS=100
```

Tempo accepts OTLP HTTP on port `4318`. Grafana is provisioned with the Tempo data source and dashboards in `grafana/dashboards`.

The application trace browser is available at `/traces`; its JSON endpoint is `/api/traces/pokemon/{id}`.
