package com.example.itemsprocessor.pipeline;

import com.example.pipeline.batch.Batch;
import com.example.pipeline.config.PipelineProperties;
import com.example.pipeline.hbase.HBaseMutation;
import com.example.pipeline.hbase.ReactiveHBaseClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Map;

@Component
public final class HBaseSurnameUpdatePipelineHandler implements PipelineHandler {
    private final ObjectMapper mapper;
    private final ReactiveHBaseClient hbase;
    private final String table;

    public HBaseSurnameUpdatePipelineHandler(ObjectMapper mapper, ReactiveHBaseClient hbase, PipelineProperties properties) {
        this.mapper = mapper; this.hbase = hbase; this.table = properties.getHbase().getTable();
    }

    @Override public boolean supports(String value) { return value != null && value.endsWith("HBaseSurnameUpdateRequestsHandler"); }

    @Override public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items()).map(ReceiverRecord::value)
                .concatMap(json -> Mono.fromCallable(() -> mapper.readTree(json)))
                .map(this::mutation).collectList()
                .flatMap(mutations -> hbase.ensureTable(table, ReactiveHBaseClient.DEFAULT_FAMILIES)
                        .then(hbase.putBatch(table, mutations)));
    }

    private HBaseMutation mutation(JsonNode event) {
        String id = required(event, "id");
        return new HBaseMutation(id, Map.of(
                "name:surname", required(event, "surname"),
                "name:frenchSurname", required(event, "frenchName")));
    }

    private String required(JsonNode node, String field) {
        String value = node.path(field).asText().trim();
        if (value.isBlank()) throw new IllegalArgumentException("Missing '" + field + "' in enriched surname event");
        return value;
    }
}
