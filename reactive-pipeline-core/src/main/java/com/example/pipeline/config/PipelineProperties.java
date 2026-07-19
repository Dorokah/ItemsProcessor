package com.example.pipeline.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("pipeline")
public class PipelineProperties {
    private final Kafka kafka = new Kafka();
    private final Hbase hbase = new Hbase();
    private final Tracing tracing = new Tracing();

    public Kafka getKafka() { return kafka; }
    public Hbase getHbase() { return hbase; }
    public Tracing getTracing() { return tracing; }

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private String groupId = "items-processor";
        private int batchSize = 100;
        private Duration batchTimeout = Duration.ofSeconds(1);
        private int concurrency = 4;
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String value) { bootstrapServers = value; }
        public String getGroupId() { return groupId; }
        public void setGroupId(String value) { groupId = value; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int value) { batchSize = value; }
        public Duration getBatchTimeout() { return batchTimeout; }
        public void setBatchTimeout(Duration value) { batchTimeout = value; }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int value) { concurrency = value; }
    }

    public static class Hbase {
        private String quorum = "localhost";
        private int clientPort = 2181;
        private String table = "pokemon";
        public String getQuorum() { return quorum; }
        public void setQuorum(String value) { quorum = value; }
        public int getClientPort() { return clientPort; }
        public void setClientPort(int value) { clientPort = value; }
        public String getTable() { return table; }
        public void setTable(String value) { table = value; }
    }

    public static class Tracing {
        private boolean enabled = true;
        private String serviceName = "items-processor";
        private String endpoint = "http://localhost:4318/v1/traces";
        private int maxBatchLinks = 100;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean value) { enabled = value; }
        public String getServiceName() { return serviceName; }
        public void setServiceName(String value) { serviceName = value; }
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String value) { endpoint = value; }
        public int getMaxBatchLinks() { return maxBatchLinks; }
        public void setMaxBatchLinks(int value) { maxBatchLinks = value; }
    }
}
