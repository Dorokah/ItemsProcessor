package com.example.itemsprocessor.pipeline;

import com.example.itemsprocessor.config.AppProperties;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

import java.util.List;

@Component
public final class PipelineRunner implements ApplicationRunner, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);
    private final ReactiveKafkaClient kafka;
    private final AppProperties properties;
    private final List<PipelineHandler> handlers;
    private Disposable subscription;

    public PipelineRunner(ReactiveKafkaClient kafka, AppProperties properties, List<PipelineHandler> handlers) {
        this.kafka = kafka;
        this.properties = properties;
        this.handlers = handlers;
    }

    @Override public void run(ApplicationArguments args) {
        if (!"pipeline".equalsIgnoreCase(properties.mode())) return;
        PipelineHandler handler = handlers.stream()
                .filter(candidate -> candidate.supports(properties.requestHandler()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported REQUEST_HANDLER: " + properties.requestHandler()));
        log.info("Starting reactive pipeline handler={} topic={}", handler.getClass().getSimpleName(), properties.consumeTopic());
        subscription = kafka.consume(properties.consumeTopic(), handler);
    }

    @Override public void close() { if (subscription != null) subscription.dispose(); }
}
