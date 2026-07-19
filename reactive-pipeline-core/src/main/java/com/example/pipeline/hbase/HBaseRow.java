package com.example.pipeline.hbase;

import java.util.Map;

public record HBaseRow(String rowKey, Map<String, String> columns) {
    public HBaseRow { columns = Map.copyOf(columns); }
}
