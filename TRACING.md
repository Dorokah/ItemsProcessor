# Tracing Model

This project uses OpenTracing with Jaeger client export to Tempo. The tracing model is built around two needs:

- See the lifecycle of one initial JSON payload.
- See the full flow of one Pokemon after the JSON is split, even when later services read/write in batches.

## Trace Shape

There are two levels of trace.

### 1. Initial JSON trace

The first trace represents the original JSON payload being consumed by the splitter.

Example:

```text
trace 9af8b49e239c386c
  consume_kafka_batch
  split batch handling
```

This trace id is copied into every split Pokemon message as:

```text
json.trace_id = 9af8b49e239c386c
```

Use this when you want to know which initial JSON a later Pokemon item came from.

### 2. Per-Pokemon item trace

After the splitter, every Pokemon gets its own trace. This keeps item traces readable and avoids giant traces when one JSON splits into hundreds or thousands of messages.

Example:

```text
trace 2613b507a03f63ab
  splitter          split_pokemon_item
  hbase-writer      consume_kafka_batch
  hbase-writer      queue_hbase_pokemon_put
  hbase-writer      publish_hbase_status
  webhook           handle_request
```

Every item span should have:

```text
pokemon.id = 305
json.trace_id = 9af8b49e239c386c
splitter.id = splitter-f3108a039ca9
```

This lets you search Tempo by `pokemon.id` and still know the original JSON trace it came from.

## Kafka Headers

Trace context and correlation ids are propagated through Kafka headers.

The important headers are:

```text
pokemon.id
json.trace_id
splitter.id
```

The helper that writes these headers is:

```python
inject_trace_headers(
    item_id=pokemon_id,
    splitter_id=splitter_id,
    json_trace_id=json_trace_id,
)
```

Consumers read the headers back and set the same fields on spans.

## Splitter Behavior

The splitter does two things:

1. Handles the original JSON inside the initial JSON trace.
2. Creates one new item trace per Pokemon.

Each `split_pokemon_item` span starts a new trace and receives:

```text
pokemon.id
json.trace_id
splitter.id
```

The produced Kafka message gets the same values in its headers.

This is intentional. Keeping all Pokemon under the same JSON trace would create huge traces and make Tempo hard to use.

## Batch Spans

Services may read Kafka in batches and write to HBase in batches. A batch span is operational: it tells you that a batch was processed.

Batch spans use summary fields instead of storing every Pokemon id:

```text
messaging.batch.size = 2
pokemon.ids.count = 2
pokemon.ids.sample = 305,306
json.trace_id = 9af8b49e239c386c
```

For large batches, this avoids creating massive span attributes like:

```text
pokemon.ids = 1,2,3,...10000
```

## Linking Items To Batches

Each item span keeps the item trace as its main trace, but also records which batch span handled it.

Item spans inside batch processing get:

```text
batch.trace_id
batch.span_id
```

Example:

```text
queue_hbase_pokemon_put
  pokemon.id = 305
  json.trace_id = 9af8b49e239c386c
  batch.trace_id = 2613b507a03f63ab
  batch.span_id = 70dec2ef3e7cb627
```

This means:

- Search `pokemon.id=305` to see the item flow.
- Use `batch.trace_id` and `batch.span_id` to find the batch span that handled the item.

The batch is not used as the parent of item spans. The Kafka message context remains the parent, so the item stays in its own Pokemon trace.

## HBase Writer Behavior

The HBase writer reads Kafka messages in batches, but creates logical item spans for each Pokemon:

```text
consume_kafka_batch
queue_hbase_pokemon_put
publish_hbase_status
```

`queue_hbase_pokemon_put` does not mean there was a separate HBase network call per Pokemon. HBase still writes using a batch. The item span means this Pokemon was queued into that HBase batch.

The status Kafka message preserves:

```text
pokemon.id
json.trace_id
splitter.id
```

So webhook can continue the same item trace.

## Webhook Behavior

Webhook consumes HBase status messages, continues the item trace from Kafka headers, and tags its span with:

```text
pokemon.id
json.trace_id
splitter.id
batch.trace_id
batch.span_id
```

## How To Search In Tempo

Search one Pokemon:

```text
pokemon.id=305
```

Search everything related to one original JSON:

```text
json.trace_id=9af8b49e239c386c
```

Search one splitter run:

```text
splitter.id=splitter-f3108a039ca9
```

## Current Dashboard

Grafana includes a core pipeline dashboard:

```text
http://localhost:3000/d/pokemon-core-pipeline/pokemon-core-pipeline
```

It shows the splitter, HBase writer, webhook logs, and core pipeline traces.

## Why This Design

One initial JSON can split into many Pokemon. If all downstream work stayed in the original JSON trace, that trace could become enormous and hard to inspect.

The chosen design gives both views:

- `json.trace_id` gives the original JSON origin.
- `pokemon.id` gives the item flow.
- `batch.trace_id` / `batch.span_id` links an item back to the batch that handled it.

This keeps traces readable and makes Tempo searches practical even with large batches.
