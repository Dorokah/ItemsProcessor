package com.example.pipeline.kafka;

import com.example.pipeline.batch.Batch;
import com.example.pipeline.batch.BatchHandler;
import com.example.pipeline.config.PipelineProperties;
import com.example.pipeline.tracing.PipelineTracing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ReactiveKafkaClient implements AutoCloseable {
    private final PipelineProperties properties;
    private final PipelineTracing tracing;
    private final KafkaSender<String, String> sender;

    public ReactiveKafkaClient(PipelineProperties properties, PipelineTracing tracing) {
        this.properties = properties;
        this.tracing = tracing;
        Map<String, Object> producer = new HashMap<>();
        producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10_000_000);
        this.sender = KafkaSender.create(SenderOptions.create(producer));
    }

    public Flux<Batch<ReceiverRecord<String, String>>> receiveBatches(String topic) {
        Map<String, Object> consumer = new HashMap<>();
        consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        consumer.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getKafka().getGroupId());
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getKafka().getBatchSize());
        consumer.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Math.toIntExact(properties.getKafka().getBatchTimeout().toMillis()));
        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(consumer)
                .subscription(List.of(topic));
        return KafkaReceiver.create(options).receiveBatch()
                .concatMap(Flux::collectList)
                .filter(records -> !records.isEmpty())
                .map(Batch::of);
    }

    public Disposable consume(String topic, BatchHandler<ReceiverRecord<String, String>> handler) {
        return receiveBatches(topic)
                .flatMap(batch -> tracing.traceBatch("consume_kafka_batch", batch, () -> handler.handle(batch))
                                .then(commit(batch)),
                        properties.getKafka().getConcurrency())
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe();
    }

    public Mono<Void> send(OutboundMessage<String, String> message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                message.topic(), null, message.key(), message.value(), message.headers());
        return sender.send(Mono.just(SenderRecord.create(record, message.key())))
                .next()
                .flatMap(result -> result.exception() == null
                        ? Mono.empty()
                        : Mono.error(result.exception()));
    }

    public Mono<Void> sendAll(Flux<OutboundMessage<String, String>> messages) {
        return sender.send(messages.map(message -> SenderRecord.create(
                        new ProducerRecord<>(message.topic(), null, message.key(), message.value(), message.headers()),
                        message.key())))
                .flatMap(result -> result.exception() == null ? Mono.empty() : Mono.error(result.exception()))
                .then();
    }

    private Mono<Void> commit(Batch<ReceiverRecord<String, String>> batch) {
        return Flux.fromIterable(batch.items())
                .map(ReceiverRecord::receiverOffset)
                .doOnNext(offset -> offset.acknowledge())
                .then(batch.items().getLast().receiverOffset().commit());
    }

    @Override public void close() { sender.close(); }
}
