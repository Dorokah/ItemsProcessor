package com.example.pipeline.tracing;

public final class TraceHeaders {
    public static final String ITEM_ID = "pokemon.id";
    public static final String HTTP_ITEM_ID = "pokemon-id";
    public static final String JSON_TRACE_ID = "json.trace_id";
    public static final String JSON_TRACE_ALIAS = "jsonTrace";
    public static final String SPLITTER_ID = "splitter.id";
    public static final String SPLIT_TIMESTAMP_MS = "split.ts.ms";

    private TraceHeaders() { }
}
