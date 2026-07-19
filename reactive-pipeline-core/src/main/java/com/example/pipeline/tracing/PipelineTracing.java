package com.example.pipeline.tracing;

import com.example.pipeline.batch.Batch;
import com.example.pipeline.config.PipelineProperties;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public final class PipelineTracing {
    private static final TextMapGetter<Headers> GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Headers carrier) {
            List<String> keys = new ArrayList<>();
            if (carrier != null) carrier.forEach(header -> keys.add(header.key()));
            return keys;
        }
        @Override public String get(Headers carrier, String key) {
            if (carrier == null) return null;
            Header header = carrier.lastHeader(key);
            return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        }
    };
    private static final TextMapSetter<Headers> SETTER = (carrier, key, value) -> {
        if (carrier != null) carrier.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final PipelineProperties properties;

    public PipelineTracing(OpenTelemetry openTelemetry, PipelineProperties properties) {
        this.openTelemetry = openTelemetry;
        this.properties = properties;
        this.tracer = openTelemetry.getTracer("reactive-pipeline-core");
    }

    public <T> Mono<T> trace(String name, SpanKind kind, Context parent, Supplier<Mono<T>> operation) {
        if (!properties.getTracing().isEnabled()) return Mono.defer(operation);
        return Mono.defer(() -> {
            Span span = tracer.spanBuilder(name).setSpanKind(kind).setParent(parent).startSpan();
            try (Scope ignored = span.makeCurrent()) {
                return operation.get()
                        .doOnError(error -> span.recordException(error).setStatus(StatusCode.ERROR, error.getMessage()))
                        .doFinally(signal -> span.end());
            } catch (Throwable error) {
                span.recordException(error).setStatus(StatusCode.ERROR, error.getMessage()).end();
                return Mono.error(error);
            }
        });
    }

    public <T> Mono<T> traceItem(String name, ReceiverRecord<?, ?> record, String itemId, Supplier<Mono<T>> operation) {
        Context parent = extract(record.headers());
        return trace(name, SpanKind.CONSUMER, parent, () -> Mono.defer(() -> {
            tagCurrentItem(itemId);
            tagCurrentCorrelations(record.headers());
            return operation.get();
        }));
    }

    public Mono<Void> traceBatch(String name, Batch<? extends ReceiverRecord<?, ?>> batch, Supplier<Mono<Void>> operation) {
        if (!properties.getTracing().isEnabled()) return Mono.defer(operation);
        return Mono.defer(() -> {
            SpanBuilder builder = tracer.spanBuilder(name).setNoParent().setSpanKind(SpanKind.CONSUMER);
            int linked = 0;
            for (ReceiverRecord<?, ?> record : batch.items()) {
                Context context = extract(record.headers());
                if (Span.fromContext(context).getSpanContext().isValid()
                        && linked++ < properties.getTracing().getMaxBatchLinks()) {
                    builder.addLink(Span.fromContext(context).getSpanContext());
                }
            }
            Span span = builder.startSpan();
            span.setAttribute("messaging.batch.size", batch.size());
            span.setAttribute("messaging.batch.linked_trace_count", Math.min(linked, properties.getTracing().getMaxBatchLinks()));
            span.setAttribute("messaging.batch.dropped_link_count", Math.max(0, linked - properties.getTracing().getMaxBatchLinks()));
            Set<String> ids = new LinkedHashSet<>();
            batch.items().forEach(record -> header(record.headers(), TraceHeaders.ITEM_ID).ifPresent(ids::add));
            span.setAttribute("pokemon.ids.count", ids.size());
            span.setAttribute("pokemon.ids.sample", String.join(",", ids.stream().limit(20).toList()));
            try (Scope ignored = span.makeCurrent()) {
                return operation.get()
                        .doOnError(error -> span.recordException(error).setStatus(StatusCode.ERROR, error.getMessage()))
                        .doFinally(signal -> span.end());
            }
        });
    }

    public Headers inject(Headers source, String itemId, String splitterId, String jsonTraceId, Long splitTimestampMs) {
        RecordHeaders target = new RecordHeaders(source == null ? new Header[0] : source.toArray());
        put(target, TraceHeaders.ITEM_ID, itemId);
        put(target, TraceHeaders.SPLITTER_ID, splitterId);
        put(target, TraceHeaders.JSON_TRACE_ID, jsonTraceId);
        put(target, TraceHeaders.JSON_TRACE_ALIAS, jsonTraceId);
        put(target, TraceHeaders.SPLIT_TIMESTAMP_MS, splitTimestampMs == null ? null : splitTimestampMs.toString());
        if (properties.getTracing().isEnabled()) {
            openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), target, SETTER);
        }
        return target;
    }

    public Headers newRootItemHeaders(String operation, String itemId, String splitterId,
                                      String jsonTraceId, long splitTimestampMs) {
        if (!properties.getTracing().isEnabled()) {
            return inject(null, itemId, splitterId, jsonTraceId, splitTimestampMs);
        }
        Span span = tracer.spanBuilder(operation).setNoParent().startSpan();
        try (Scope ignored = span.makeCurrent()) {
            tagCurrentItem(itemId);
            if (splitterId != null) {
                span.setAttribute("splitter.id", splitterId);
                span.setAttribute("spillerid", splitterId);
            }
            if (jsonTraceId != null) {
                span.setAttribute("json.trace_id", jsonTraceId);
                span.setAttribute("jsonTrace", jsonTraceId);
            }
            span.setAttribute("split.ts.ms", Long.toString(splitTimestampMs));
            return inject(null, itemId, splitterId, jsonTraceId, splitTimestampMs);
        } catch (Throwable error) {
            span.recordException(error).setStatus(StatusCode.ERROR, error.getMessage());
            throw error;
        } finally {
            span.end();
        }
    }

    public Context extract(Headers headers) {
        return properties.getTracing().isEnabled()
                ? openTelemetry.getPropagators().getTextMapPropagator().extract(Context.root(), headers, GETTER)
                : Context.root();
    }

    public Optional<String> header(Headers headers, String key) {
        return Optional.ofNullable(GETTER.get(headers, key));
    }

    public void tagCurrentItem(String itemId) {
        if (itemId != null && !itemId.isBlank()) Span.current().setAttribute("pokemon.id", itemId);
    }

    public void tagCurrentCorrelations(Headers headers) {
        header(headers, TraceHeaders.SPLITTER_ID).ifPresent(value -> {
            Span.current().setAttribute("splitter.id", value);
            Span.current().setAttribute("spillerid", value);
        });
        header(headers, TraceHeaders.JSON_TRACE_ID).ifPresent(value -> {
            Span.current().setAttribute("json.trace_id", value);
            Span.current().setAttribute("jsonTrace", value);
        });
        header(headers, TraceHeaders.SPLIT_TIMESTAMP_MS)
                .ifPresent(value -> Span.current().setAttribute(AttributeKey.stringKey("split.ts.ms"), value));
    }

    public String currentTraceId() {
        return Span.current().getSpanContext().isValid() ? Span.current().getSpanContext().getTraceId() : null;
    }

    private static void put(Headers headers, String key, String value) {
        if (value != null && !value.isBlank()) headers.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
