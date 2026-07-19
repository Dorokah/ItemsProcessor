package com.example.pipeline.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public record OutboundMessage<K, V>(String topic, K key, V value, Headers headers) {
    public OutboundMessage(String topic, K key, V value) {
        this(topic, key, value, new RecordHeaders());
    }
}
