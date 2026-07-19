package com.example.itemsprocessor.web;

import com.example.pipeline.config.PipelineProperties;
import com.example.pipeline.hbase.HBaseRow;
import com.example.pipeline.hbase.ReactiveHBaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/hbase")
public class HBaseBrowserController {
    private final ReactiveHBaseClient hbase;
    private final String table;
    public HBaseBrowserController(ReactiveHBaseClient hbase, PipelineProperties properties) {
        this.hbase = hbase; this.table = properties.getHbase().getTable();
    }
    @GetMapping("/pokemon") public Flux<HBaseRow> pokemon() { return hbase.scan(table); }
}
