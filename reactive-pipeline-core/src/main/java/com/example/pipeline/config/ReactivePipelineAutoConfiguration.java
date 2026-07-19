package com.example.pipeline.config;

import com.example.pipeline.hbase.ReactiveHBaseClient;
import com.example.pipeline.kafka.ReactiveKafkaClient;
import com.example.pipeline.tracing.PipelineTracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(PipelineProperties.class)
public class ReactivePipelineAutoConfiguration {
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    OpenTelemetry openTelemetry(PipelineProperties properties) {
        if (!properties.getTracing().isEnabled()) return OpenTelemetry.noop();
        OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(properties.getTracing().getEndpoint()).build();
        Resource resource = Resource.getDefault().merge(Resource.builder()
                .put(AttributeKey.stringKey("service.name"), properties.getTracing().getServiceName()).build());
        SdkTracerProvider provider = SdkTracerProvider.builder().setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build()).build();
        return OpenTelemetrySdk.builder().setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Bean @ConditionalOnMissingBean
    PipelineTracing pipelineTracing(OpenTelemetry telemetry, PipelineProperties properties) {
        return new PipelineTracing(telemetry, properties);
    }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean
    ReactiveKafkaClient reactiveKafkaClient(PipelineProperties properties, PipelineTracing tracing) {
        return new ReactiveKafkaClient(properties, tracing);
    }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean
    ReactiveHBaseClient reactiveHBaseClient(PipelineProperties properties) {
        return new ReactiveHBaseClient(properties);
    }
}
