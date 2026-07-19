package com.example.pipeline.hbase;

import com.example.pipeline.config.PipelineProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ReactiveHBaseClient implements AutoCloseable {
    public static final List<String> DEFAULT_FAMILIES = List.of("info", "name", "type", "base", "profile", "evolution");
    private final Configuration configuration;
    private volatile Connection connection;

    public ReactiveHBaseClient(PipelineProperties properties) {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", properties.getHbase().getQuorum());
        configuration.setInt("hbase.zookeeper.property.clientPort", properties.getHbase().getClientPort());
    }

    public Mono<Void> ensureTable(String tableName, List<String> families) {
        return blocking(() -> {
            TableName name = TableName.valueOf(tableName);
            try (Admin admin = connection().getAdmin()) {
                if (!admin.tableExists(name)) {
                    TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(name);
                    families.forEach(family -> descriptor.setColumnFamily(
                            ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family))));
                    admin.createTable(descriptor.build());
                }
            }
            return null;
        }).then();
    }

    public Mono<Void> putBatch(String tableName, List<HBaseMutation> mutations) {
        if (mutations.isEmpty()) return Mono.empty();
        return blocking(() -> {
            try (Table table = connection().getTable(TableName.valueOf(tableName))) {
                List<Put> puts = mutations.stream().map(this::toPut).toList();
                table.put(puts);
            }
            return null;
        }).then();
    }

    public Flux<HBaseRow> scan(String tableName) {
        return blocking(() -> {
            try (Table table = connection().getTable(TableName.valueOf(tableName));
                 ResultScanner scanner = table.getScanner(new Scan())) {
                return Arrays.stream(scanner.next(Integer.MAX_VALUE)).map(this::toRow).toList();
            }
        }).flatMapMany(Flux::fromIterable);
    }

    private Put toPut(HBaseMutation mutation) {
        Put put = new Put(Bytes.toBytes(mutation.rowKey()));
        mutation.columns().forEach((column, value) -> {
            String[] parts = column.split(":", 2);
            if (parts.length != 2) throw new IllegalArgumentException("HBase column must be family:qualifier: " + column);
            put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]), Bytes.toBytes(value));
        });
        return put;
    }

    private HBaseRow toRow(Result result) {
        Map<String, String> columns = new LinkedHashMap<>();
        result.listCells().forEach(cell -> columns.put(
                Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + ":" +
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
        return new HBaseRow(Bytes.toString(result.getRow()), columns);
    }

    private synchronized Connection connection() throws IOException {
        if (connection == null || connection.isClosed()) connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    private <T> Mono<T> blocking(ThrowingSupplier<T> supplier) {
        return Mono.fromCallable(supplier::get).subscribeOn(Schedulers.boundedElastic());
    }

    @Override public void close() throws IOException { if (connection != null) connection.close(); }

    @FunctionalInterface private interface ThrowingSupplier<T> { T get() throws Exception; }
}
