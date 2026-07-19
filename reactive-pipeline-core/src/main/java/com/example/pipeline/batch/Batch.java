package com.example.pipeline.batch;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record Batch<T>(List<T> items, Instant receivedAt) {
    public Batch {
        items = List.copyOf(Objects.requireNonNull(items, "items"));
        receivedAt = Objects.requireNonNull(receivedAt, "receivedAt");
    }

    public static <T> Batch<T> of(List<T> items) {
        return new Batch<>(items, Instant.now());
    }

    public int size() { return items.size(); }
    public boolean isEmpty() { return items.isEmpty(); }
}
