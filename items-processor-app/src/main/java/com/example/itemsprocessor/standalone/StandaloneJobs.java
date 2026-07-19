package com.example.itemsprocessor.standalone;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.kafka.OutboundMessage;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import com.example.pipeline.tracing.PipelineTracing;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
public final class StandaloneJobs implements ApplicationRunner, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StandaloneJobs.class);
    private static final List<String> SURNAMES = List.of("Oak", "Elm", "Birch", "Rowan", "Juniper", "Sycamore", "Kukui", "Magnolia", "Willow", "Maple");
    private final AppProperties properties;
    private final ReactiveKafkaClient kafka;
    private final PipelineTracing tracing;
    private final ObjectMapper mapper;
    private Disposable subscription;

    public StandaloneJobs(AppProperties properties, ReactiveKafkaClient kafka, PipelineTracing tracing, ObjectMapper mapper) {
        this.properties = properties; this.kafka = kafka; this.tracing = tracing; this.mapper = mapper;
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        if ("producer".equalsIgnoreCase(properties.mode())) {
            String json = Files.readString(Path.of(properties.pokedexFile()));
            mapper.readTree(json);
            kafka.send(new OutboundMessage<>(properties.consumeTopic(), null, json)).block();
            log.info("Published pokedex to {}", properties.consumeTopic());
        } else if ("surname-generator".equalsIgnoreCase(properties.mode())) {
            JsonNode pokedex = mapper.readTree(Path.of(properties.pokedexFile()).toFile());
            List<String> ids = Flux.fromIterable(pokedex).map(node -> node.path("id").asText()).collectList().block();
            subscription = Flux.interval(Duration.ZERO, Duration.ofSeconds(properties.surnameIntervalSeconds()))
                    .concatMap(tick -> generate(ids)).subscribe();
        }
    }

    private Mono<Void> generate(List<String> ids) {
        String id = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
        String surname = SURNAMES.get(ThreadLocalRandom.current().nextInt(SURNAMES.size()));
        Map<String, String> event = Map.of("eventType", "pokemon-surname-generated", "id", id,
                "surname", surname, "generatedAt", Instant.now().toString());
        return Mono.fromCallable(() -> mapper.writeValueAsString(event))
                .flatMap(json -> kafka.send(new OutboundMessage<>(properties.publishTopic(), id, json,
                        tracing.inject(new RecordHeaders(), id, null, null, null))));
    }

    @Override public void close() { if (subscription != null) subscription.dispose(); }
}
