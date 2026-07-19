package com.example.itemsprocessor.pipeline;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.batch.Batch;
import com.example.pipeline.kafka.OutboundMessage;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import com.example.pipeline.tracing.PipelineTracing;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.UUID;

@Component
public final class SplitPipelineHandler implements PipelineHandler {
    private final ObjectMapper mapper;
    private final ReactiveKafkaClient kafka;
    private final PipelineTracing tracing;
    private final AppProperties properties;

    public SplitPipelineHandler(ObjectMapper mapper, ReactiveKafkaClient kafka, PipelineTracing tracing, AppProperties properties) {
        this.mapper = mapper; this.kafka = kafka; this.tracing = tracing; this.properties = properties;
    }

    @Override public boolean supports(String value) { return value != null && value.endsWith("SplitRequestsHandler"); }

    @Override public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        String splitterId = "splitter-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String jsonTraceId = tracing.currentTraceId();
        return kafka.sendAll(Flux.fromIterable(batch.items()).concatMap(record -> parse(record.value()))
                .map(item -> {
                    String id = item.path("id").asText();
                    long splitAt = System.currentTimeMillis();
                    return new OutboundMessage<>(properties.publishTopic(), id, item.toString(),
                            tracing.newRootItemHeaders("split_pokemon_item", id, splitterId, jsonTraceId, splitAt));
                }));
    }

    private Flux<JsonNode> parse(String json) {
        return Mono.fromCallable(() -> mapper.readTree(json))
                .flatMapMany(node -> node.isArray() ? Flux.fromIterable(node) : Flux.error(
                        new IllegalArgumentException("Incoming pokedex payload is not a JSON list")));
    }
}
