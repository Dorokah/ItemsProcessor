package com.example.pipeline.hbase;

import java.util.Map;

public record HBaseMutation(String rowKey, Map<String, String> columns) {
    public HBaseMutation { columns = Map.copyOf(columns); }
}
