package com.example.itemsprocessor.pipeline;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.batch.Batch;
import com.example.pipeline.kafka.OutboundMessage;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import com.example.pipeline.tracing.PipelineTracing;
import com.example.pipeline.tracing.TraceHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Component
public final class SurnameEnrichmentPipelineHandler implements PipelineHandler {
    private final ObjectMapper mapper;
    private final WebClient webClient;
    private final ReactiveKafkaClient kafka;
    private final PipelineTracing tracing;
    private final AppProperties properties;

    public SurnameEnrichmentPipelineHandler(ObjectMapper mapper, WebClient webClient, ReactiveKafkaClient kafka,
                                             PipelineTracing tracing, AppProperties properties) {
        this.mapper = mapper; this.webClient = webClient; this.kafka = kafka; this.tracing = tracing; this.properties = properties;
    }

    @Override public boolean supports(String value) { return value != null && value.endsWith("SurnameEnrichmentRequestsHandler"); }

    @Override public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items()).flatMap(this::enrich).then();
    }

    private Mono<Void> enrich(ReceiverRecord<String, String> record) {
        return Mono.fromCallable(() -> mapper.readTree(record.value())).flatMap(event -> {
            String id = required(event, "id");
            String surname = required(event, "surname");
            String jsonTrace = tracing.header(record.headers(), TraceHeaders.JSON_TRACE_ID).orElse(null);
            return tracing.traceItem("enrich_pokemon_surname_item", record, id, () ->
                    webClient.get().uri(properties.surnameApiUrl() + "/translate?{query}", surname)
                            .headers(headers -> {
                                headers.set(TraceHeaders.HTTP_ITEM_ID, id);
                                if (jsonTrace != null) headers.set(TraceHeaders.JSON_TRACE_ID, jsonTrace);
                            })
                            .retrieve().bodyToMono(JsonNode.class)
                            .map(response -> {
                                ((ObjectNode) event).put("frenchName", response.path("frenchName").asText());
                                return new OutboundMessage<>(properties.publishTopic(), id, event.toString(),
                                        tracing.inject(record.headers(), id, null, jsonTrace, null));
                            }).flatMap(kafka::send));
        });
    }

    private String required(JsonNode node, String field) {
        String value = node.path(field).asText().trim();
        if (value.isBlank()) throw new IllegalArgumentException("Missing '" + field + "' in surname event");
        return value;
    }
}
