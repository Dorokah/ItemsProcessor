# Tracing Model

This project uses OpenTelemetry and exports traces to Tempo through OTLP HTTP.

The tracing model has two goals:

- Keep the original JSON trace readable.
- Create a separate trace for every Pokemon item after the splitter.

## What Was Implemented

### 1. Change OpenTracing to OpenTelemetry

The project no longer uses OpenTracing/Jaeger client code. Tracing is now implemented with OpenTelemetry in `src/utils/tracer.py`.

What changed:

- Services initialize an OpenTelemetry tracer provider.
- Traces are exported to Tempo through OTLP HTTP.
- Kafka and HTTP propagation use the W3C `traceparent` header.
- Docker Compose now uses:

```text
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://tempo:4318/v1/traces
```

Tempo now exposes OTLP:

```text
4318 - OTLP HTTP
4317 - OTLP gRPC
```

### 2. Use Span Links When Doing Batches

Kafka batch reads and HBase batch writes create batch spans. Those batch spans do not become the parent of every message trace. Instead, they use OpenTelemetry span links to point at the traces of the messages inside the batch.

Example:

```text
consume_kafka_batch
  messaging.batch.size = 2
  messaging.batch.linked_trace_count = 2
  links:
    trace of pokemon 605
    trace of pokemon 606
```

Item spans also keep a reference to the active batch span:

```text
batch.trace_id = <batch trace id>
batch.span_id = <batch span id>
```

This means the item trace remains readable by itself, while the batch span still shows which item traces participated in that batch.

### 3. Add A Dashboard To Search Split Trace Links

The dashboard file is:

```text
grafana/dashboards/pokemon_split_trace_links.json
```

Dashboard URL:

```text
http://localhost:3000/d/pokemon-split-trace-links/pokemon-split-trace-links
```

Use the `jsonTrace` variable to enter the original JSON trace id. The dashboard searches Tempo for spans and traces that carry that value.

Important detail: Tempo can search span attributes like `jsonTrace`, but it does not provide a simple native "search every linked trace from this trace id" query. So the reliable model here is:

- Put `jsonTrace` on every item span after the splitter.
- Use span links for batch causality.
- Search by `jsonTrace` when you want all split items from one original JSON.

### 4. Make Multithreading Safe For Span Creation And Context Propagation

The app processes Kafka work in worker threads. The tracing code avoids sharing one mutable active span across workers.

What makes it safe:

- Kafka headers are extracted inside the worker thread that handles the message or batch.
- `start_batch_span(...)` is called inside the worker thread.
- Item spans are started inside the same worker thread that does the actual handler work.
- Outbound Kafka/HTTP headers are injected while the intended span is active.
- OpenTelemetry context is context-local, so one worker thread's active span does not overwrite another worker thread's active span.

The practical effect: concurrent batches can run with `MAX_WORKER_THREADS > 1` without one batch accidentally injecting another batch's trace context.

### 5. Handle More Than The Maximum Possible Span Links

A batch of 10,000 messages should not create 10,000 span links. The current limit is:

```text
MAX_BATCH_SPAN_LINKS=100
```

When a batch has more messages than this limit, the span records:

```text
messaging.batch.linked_trace_count = <number of links actually added>
messaging.batch.dropped_link_count = <number of links not added>
messaging.batch.max_links = 100
```

Recommended approach for huge batches:

- Keep only the first N span links.
- Add a `batch.id` to every message/span/log.
- Write a compact batch manifest log to Elasticsearch with all Pokemon ids in the batch.
- Store complete batch membership in logs or storage, not inside trace links.

Best practice: traces should explain causality. Very large membership lists belong in logs, Elasticsearch, HBase, or another searchable store.

### 6. Measure Per-Item Split-To-Webhook Time Without Traces

Yes. The better way for this metric is logs, not traces.

The splitter writes this Kafka header:

```text
split.ts.ms
```

The webhook reads it and writes this field to Elasticsearch:

```text
splitToWebhookDurationMs
```

Use this field for dashboards and percentiles such as average, p95, and max latency. This is better than calculating it from traces because logs are cheaper to query, easier to aggregate, and still work if tracing is sampled.

## Trace Shape

### 1. Initial JSON trace

The splitter consumes the original JSON payload in its own trace.

Example:

```text
trace 4b64b642c3c9b404
  consume_kafka_batch
  split batch handling
```

This trace ends in the splitter. It is copied into every split item as span/header attributes:

```text
json.trace_id = 4b64b642c3c9b404
jsonTrace = 4b64b642c3c9b404
```

`jsonTrace` is intentionally duplicated as a simpler Grafana/Tempo search attribute.

### 2. Per-Pokemon item trace

After the splitter, every Pokemon starts a new trace.

Example:

```text
trace 9e9ec714b247cdda
  splitter          split_pokemon_item
  hbase-writer      queue_hbase_pokemon_put
  hbase-writer      publish_hbase_status
  webhook           handle_request
```

Every item span should have:

```text
pokemon.id = 1
jsonTrace = 4b64b642c3c9b404
splitter.id = splitter-2eadfe07edf4
```

This means:

- Search by `pokemon.id` to see one item flow.
- Search by `jsonTrace` to see all item traces created from one original JSON trace.

## Kafka Headers

Trace context and correlation values are propagated through Kafka headers.

The important headers are:

```text
traceparent
pokemon.id
json.trace_id
jsonTrace
splitter.id
split.ts.ms
```

`traceparent` is the W3C OpenTelemetry context. The other headers are searchable correlation fields.

## Splitter Behavior

The splitter does two things:

1. Processes the original JSON in the JSON trace.
2. Starts a new trace for each split Pokemon item.

Each `split_pokemon_item` span starts with `new_trace=True`. It is not a child of the JSON trace. Instead, it carries:

```text
jsonTrace = <original JSON trace id>
pokemon.id = <Pokemon id>
splitter.id = <splitter run id>
```

The splitter also writes:

```text
split.ts.ms = <epoch milliseconds>
```

That timestamp is used by the webhook service to calculate split-to-webhook latency without needing trace queries.

## Batch Spans And Span Links

Services may read Kafka in batches and write to HBase in batches.

Batch spans are created as independent spans and use OpenTelemetry span links to point to the item traces contained in the batch.

Example:

```text
consume_kafka_batch
  links:
    pokemon 1 split/item context
    pokemon 2 split/item context
  messaging.batch.size = 2
  messaging.batch.linked_trace_count = 2
```

Item spans also link back to the active batch span and keep tags:

```text
batch.trace_id
batch.span_id
```

This gives both directions:

- Batch span links to the messages it consumed.
- Item spans record which batch handled them.

## Known Limitation: Link Cardinality

OpenTelemetry span links are useful, but a batch can be very large. A batch of 10,000 messages should not create 10,000 span links.

The current limit is controlled by:

```text
MAX_BATCH_SPAN_LINKS=100
```

When a batch has more messages than the limit, the batch span records:

```text
messaging.batch.linked_trace_count = 100
messaging.batch.dropped_link_count = <remaining count>
messaging.batch.max_links = 100
```

Recommended options for very large batches:

- Keep only the first N span links and record the dropped count.
- Add `batch.id` to every message and span, then search by `batch.id`.
- Emit a compact batch manifest log to Elasticsearch with all item ids.
- Store full batch membership outside tracing, for example in Elasticsearch or HBase, and keep traces sampled/summarized.

Best practice here: traces should show causality and representative relationships; logs or storage should hold huge membership lists.

## Multithreading Safety

The app processes Kafka batches in worker threads.

The code avoids relying on context created in the main consumer thread. Instead:

- Kafka headers are extracted inside the worker thread.
- `start_batch_span(...)` runs inside the worker thread.
- Item spans are created inside the same worker thread as the handler work.
- Context is injected into outbound Kafka/HTTP headers while the intended item span is active.

OpenTelemetry context is context-local, so one worker thread's active span does not overwrite another worker thread's active span.

This is why span creation and propagation stay stable even when `MAX_WORKER_THREADS` allows concurrent batch handling.

## Split-To-Webhook Duration Without Traces

Yes, there is a better way than traces for this specific metric.

The splitter writes:

```text
split.ts.ms
```

Webhook reads it and logs:

```text
splitToWebhookDurationMs
```

This value goes to Elasticsearch/Logstash and can be graphed in Grafana as a normal log metric. That is better than using traces for latency dashboards because:

- it is cheaper to query
- it works even if traces are sampled
- it gives simple percentiles/averages over many Pokemon

Use traces to debug one item. Use `splitToWebhookDurationMs` logs to monitor the pipeline.

## Grafana Dashboards

Core pipeline dashboard:

```text
http://localhost:3000/d/pokemon-core-pipeline/pokemon-core-pipeline
```

Split trace links dashboard:

```text
http://localhost:3000/d/pokemon-split-trace-links/pokemon-split-trace-links
```

In the split trace links dashboard, enter the original JSON trace id as `jsonTrace`. It searches Tempo for all split item traces/spans that came from that JSON payload.

## How To Search In Tempo

Search one Pokemon:

```text
{ .pokemon.id = "1" }
```

Search all split items from one original JSON:

```text
{ .jsonTrace = "4b64b642c3c9b404" }
```

Search one splitter run:

```text
{ .splitter.id = "splitter-2eadfe07edf4" }
```
