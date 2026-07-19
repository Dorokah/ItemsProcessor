package com.example.pipeline.kafka;

import org.apache.kafka.common.header.Headers;
import reactor.kafka.receiver.ReceiverRecord;

public record KafkaMessage<K, V>(K key, V value, String topic, int partition, long offset, Headers headers) {
    public static <K, V> KafkaMessage<K, V> from(ReceiverRecord<K, V> record) {
        return new KafkaMessage<>(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers());
    }
}
