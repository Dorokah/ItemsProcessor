package com.example.itemsprocessor.pipeline;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.batch.Batch;
import com.example.pipeline.tracing.PipelineTracing;
import com.example.pipeline.tracing.TraceHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Component
public final class WebhookPipelineHandler implements PipelineHandler {
    private static final Logger log = LoggerFactory.getLogger(WebhookPipelineHandler.class);
    private final ObjectMapper mapper;
    private final WebClient webClient;
    private final PipelineTracing tracing;
    private final AppProperties properties;

    public WebhookPipelineHandler(ObjectMapper mapper, WebClient webClient, PipelineTracing tracing, AppProperties properties) {
        this.mapper = mapper; this.webClient = webClient; this.tracing = tracing; this.properties = properties;
    }

    @Override public boolean supports(String value) { return value != null && value.endsWith("WebhookRequestsHandler"); }

    @Override public Mono<Void> handle(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items()).flatMap(record -> Mono.fromCallable(() -> mapper.readTree(record.value()))
                .flatMap(payload -> tracing.traceItem("forward_webhook", record, payload.path("id").asText(), () ->
                        webClient.post().uri(properties.webhookUrl())
                                .headers(headers -> tracing.header(record.headers(), TraceHeaders.ITEM_ID)
                                        .ifPresent(id -> headers.set(TraceHeaders.HTTP_ITEM_ID, id)))
                                .bodyValue(payload).retrieve().toBodilessEntity().then()
                                .doOnSuccess(ignored -> log.info("Forwarded status for Pokemon {}", payload.path("id").asText())))))
                .then();
    }
}
