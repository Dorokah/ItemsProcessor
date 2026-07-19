package com.example.itemsprocessor.pipeline;

import com.example.pipeline.batch.BatchHandler;
import reactor.kafka.receiver.ReceiverRecord;

public interface PipelineHandler extends BatchHandler<ReceiverRecord<String, String>> {
    boolean supports(String configuredHandler);
}
