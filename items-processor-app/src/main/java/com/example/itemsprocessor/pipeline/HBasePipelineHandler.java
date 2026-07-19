package com.example.itemsprocessor.pipeline;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.batch.Batch;
import com.example.pipeline.config.PipelineProperties;
import com.example.pipeline.hbase.HBaseMutation;
import com.example.pipeline.hbase.ReactiveHBaseClient;
import com.example.pipeline.kafka.OutboundMessage;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import com.example.pipeline.tracing.PipelineTracing;
import com.example.pipeline.tracing.TraceHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.List;

@Component
public final class HBasePipelineHandler implements PipelineHandler {
    private final ObjectMapper mapper;
    private final ReactiveHBaseClient hbase;
    private final ReactiveKafkaClient kafka;
    private final PipelineTracing tracing;
    private final AppProperties app;
    private final String table;

    public HBasePipelineHandler(ObjectMapper mapper, ReactiveHBaseClient hbase, ReactiveKafkaClient kafka,
                                PipelineTracing tracing, AppProperties app, PipelineProperties pipeline) {
        this.mapper = mapper; this.hbase = hbase; this.kafka = kafka; this.tracing = tracing; this.app = app;
        this.table = pipeline.getHbase().getTable();
    }

    @Override public boolean supports(String value) { return value != null && value.endsWith("HBaseRequestsHandler"); }

    @Override public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items()).concatMap(this::parse).collectList()
                .flatMap(records -> hbase.ensureTable(table, ReactiveHBaseClient.DEFAULT_FAMILIES)
                        .then(hbase.putBatch(table, records.stream().map(Parsed::mutation).toList()))
                        .thenMany(Flux.fromIterable(records))
                        .concatMap(record -> publishStatus(record.source(), record.mutation().rowKey(), "success",
                                "Successfully wrote Pokemon ID " + record.mutation().rowKey() + " to HBase"))
                        .then());
    }

    private Mono<Parsed> parse(ReceiverRecord<String, String> source) {
        return Mono.fromCallable(() -> {
            JsonNode pokemon = mapper.readTree(source.value());
            String id = pokemon.path("id").asText();
            if (id.isBlank()) throw new IllegalArgumentException("Missing 'id' in Pokemon record");
            return new Parsed(source, new HBaseMutation(id, PokemonColumns.from(pokemon)));
        });
    }

    private Mono<Void> publishStatus(ReceiverRecord<String, String> source, String id, String status, String message) {
        if (app.publishTopic() == null || app.publishTopic().isBlank()) return Mono.empty();
        return Mono.fromCallable(() -> mapper.writeValueAsString(new Status(id, status, message)))
                .flatMap(body -> kafka.send(new OutboundMessage<>(app.publishTopic(), id, body,
                        tracing.inject(source.headers(), id,
                                tracing.header(source.headers(), TraceHeaders.SPLITTER_ID).orElse(null),
                                tracing.header(source.headers(), TraceHeaders.JSON_TRACE_ID).orElse(null), null))));
    }

    private record Parsed(ReceiverRecord<String, String> source, HBaseMutation mutation) { }
    private record Status(String id, String status, String message) { }
}
