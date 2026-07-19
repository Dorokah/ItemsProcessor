# Items Processor

Items Processor is a Java 21, Spring Boot, Project Reactor pipeline. Kafka stages run with Reactor Kafka, HBase access is exposed as reactive APIs with blocking client work isolated on Reactor's bounded-elastic scheduler, and OpenTelemetry carries trace context across both.

## Modules

| Module | Purpose |
|---|---|
| `reactive-pipeline-core` | Standalone package containing `Batch`, reactive Kafka and HBase wrappers, trace propagation, spans, and Spring Boot auto-configuration. |
| `items-processor-app` | Pipeline handlers, producer/generator jobs, surname API, HBase browser, and trace dashboard. |

The core module can be published and consumed independently:

```xml
<dependency>
  <groupId>com.example.itemsprocessor</groupId>
  <artifactId>reactive-pipeline-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Its auto-configuration provides `ReactiveKafkaClient`, `ReactiveHBaseClient`, and `PipelineTracing`. Implement `BatchHandler<ReceiverRecord<String,String>>` to process a Kafka poll as one explicit `Batch`:

```java
class ExampleHandler implements BatchHandler<ReceiverRecord<String, String>> {
    public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items())
                .flatMap(this::process)
                .then();
    }
}
```

Offsets are committed only after the batch handler completes successfully. Failed batches are retried. `BATCH_SIZE` maps to Kafka's `max.poll.records`, and `MAX_WORKER_THREADS` controls concurrent batch processing.

## Pipeline

The default topology is:

```text
pokedex-raw -> splitter -> pokemon-individual -> HBase writer -> hbase-status -> webhook
```

`REQUEST_HANDLER` remains compatible with the previous Docker configuration. Supported suffixes are:

- `SplitRequestsHandler`
- `HBaseRequestsHandler`
- `WebhookRequestsHandler`
- `SurnameEnrichmentRequestsHandler`
- `HBaseSurnameUpdateRequestsHandler`

## Build and test

With Java 21 and Maven 3.9+:

```bash
mvn test
mvn -pl items-processor-app -am package
```

Or build and run the complete environment:

```bash
docker compose build
docker compose up -d
./scripts/run_pipeline_test.sh
```

Useful endpoints:

- Grafana: <http://localhost:3000>
- HBase browser: <http://localhost:8081>
- HBase JSON API: <http://localhost:8081/api/hbase/pokemon>
- Trace dashboard: `/traces` on any application instance with HTTP exposed
- Surname API: `/translate?surname=Oak` and `/surname/Oak/french`
- Health: `/actuator/health`

## Configuration

| Environment variable | Default | Description |
|---|---:|---|
| `APP_MODE` | `pipeline` | `pipeline`, `producer`, `surname-generator`, or `web` |
| `REQUEST_HANDLER` | splitter handler | Handler selected for pipeline mode |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_GROUP_ID` | service name | Consumer group |
| `CONSUME_TOPIC` | `pokedex-raw` | Input topic |
| `PUBLISH_TOPIC` | `pokemon-individual` | Output topic |
| `BATCH_SIZE` | `100` | Maximum records per Kafka poll |
| `MAX_WORKER_THREADS` | `4` | Concurrent reactive batches |
| `HBASE_HOST` | `localhost` | HBase ZooKeeper quorum |
| `HBASE_ZOOKEEPER_PORT` | `2181` | ZooKeeper client port |
| `HBASE_TABLE_NAME` | `pokemon` | HBase table |
| `TRACING_ENABLE` | `true` | Enable OpenTelemetry |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | local Tempo | OTLP/HTTP trace endpoint |
| `MAX_BATCH_SPAN_LINKS` | `100` | Maximum input trace links on a batch span |

Spring properties under `pipeline.*` and `items.*` can be used instead of environment variables.

## Reactive boundaries

Kafka send and receive paths are non-blocking and backpressure-aware. HBase's standard Java client is synchronous; `ReactiveHBaseClient` wraps every connection, admin, put, and scan operation in `Mono`/`Flux` scheduled on `Schedulers.boundedElastic()`. Webhook, surname, and Tempo calls use Spring `WebClient`.
