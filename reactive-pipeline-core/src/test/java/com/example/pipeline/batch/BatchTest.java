package com.example.pipeline.batch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchTest {
    @Test void takesAnImmutableSnapshot() {
        List<String> source = new ArrayList<>(List.of("one"));
        Batch<String> batch = Batch.of(source);
        source.add("two");
        assertEquals(1, batch.size());
        assertThrows(UnsupportedOperationException.class, () -> batch.items().add("three"));
    }
}
