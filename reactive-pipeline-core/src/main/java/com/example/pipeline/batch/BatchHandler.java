package com.example.pipeline.batch;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface BatchHandler<T> {
    Mono<Void> handle(Batch<T> batch);
}
