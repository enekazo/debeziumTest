package io.debezium.server.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.fabric.config.FabricSinkConfig;
import io.debezium.server.fabric.metadata.OracleMetadataLoader;
import io.debezium.server.fabric.metadata.TableMetadata;
import io.debezium.server.fabric.parquet.ParquetFileWriter;
import io.debezium.server.fabric.storage.AbfsStorageBackend;
import io.debezium.server.fabric.storage.LocalStorageBackend;
import io.debezium.server.fabric.storage.MetadataManager;
import io.debezium.server.fabric.storage.SequenceManager;
import io.debezium.server.fabric.storage.StorageBackend;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Debezium Server custom sink that writes Oracle CDC events to Microsoft Fabric
 * Open Mirroring landing zone as Parquet files.
 *
 * Discovered by Debezium Server via the {@code @Named("fabric")} annotation.
 * Configure with {@code debezium.sink.type=fabric} in application.properties.
 */
@Named("fabric")
@Dependent
public class FabricMirroringSink implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(FabricMirroringSink.class);

    // Row marker constants
    private static final int MARKER_INSERT = 0;
    private static final int MARKER_UPDATE = 1;
    private static final int MARKER_DELETE = 2;

    private FabricSinkConfig config;
    private StorageBackend storage;
    private OracleMetadataLoader metadataLoader;
    private SequenceManager sequenceManager;
    private MetadataManager metadataManager;
    private ParquetFileWriter parquetWriter;
    private ObjectMapper objectMapper;
    private Path tempDir;

    // Per-table row buffers (ConcurrentHashMap for thread safety between sink and flush threads)
    private final ConcurrentHashMap<String, List<Map<String, Object>>> tableBuffers = new ConcurrentHashMap<>();
    // Per-table metadata (loaded on first encounter)
    private final ConcurrentHashMap<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
    // Tracks which tables have been initialized (sequence, _metadata.json)
    private final ConcurrentHashMap<String, Boolean> initializedTables = new ConcurrentHashMap<>();

    private ScheduledExecutorService flushScheduler;

    @PostConstruct
    public void init() {
        LOG.info("Initializing FabricMirroringSink");
        config = FabricSinkConfig.load();

        // Create temp directory for local Parquet files before upload
        tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "fabric-sink");
        try {
            Files.createDirectories(tempDir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create temp directory: " + tempDir, e);
        }

        // Initialize storage backend
        if (config.baseUri.startsWith("abfss://")) {
            storage = new AbfsStorageBackend(config.baseUri, config.sasToken);
        } else {
            storage = new LocalStorageBackend(config.baseUri);
        }

        // Initialize Oracle metadata loader if JDBC config present.
        // Without this, the sink cannot write Parquet files since Avro schema
        // generation requires Oracle column and PK metadata.
        if (config.oracleJdbcUrl != null && !config.oracleJdbcUrl.isBlank()) {
            metadataLoader = new OracleMetadataLoader(
                    config.oracleJdbcUrl, config.oracleUsername, config.oraclePassword,
                    config.oracleMetaRefreshMs);
            LOG.info("Oracle metadata loader configured for {}", config.oracleJdbcUrl);
        }

        sequenceManager = new SequenceManager(storage, config.stateFileName);
        metadataManager = new MetadataManager(storage);
        parquetWriter = new ParquetFileWriter();
        objectMapper = new ObjectMapper();

        // Start background flush scheduler
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fabric-sink-flusher");
            t.setDaemon(true);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(
                this::backgroundFlush,
                config.flushIntervalMs,
                config.flushIntervalMs,
                TimeUnit.MILLISECONDS);

        LOG.info("FabricMirroringSink initialized: baseUri={}, flushIntervalMs={}",
                config.baseUri, config.flushIntervalMs);
    }

    @PreDestroy
    public void destroy() {
        LOG.info("Shutting down FabricMirroringSink");
        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                flushScheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Final flush
        try {
            flushAllTables();
        } catch (Exception e) {
            LOG.error("Error during final flush on shutdown", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
            throws InterruptedException {

        for (ChangeEvent<String, String> record : records) {
            try {
                processRecord(record);
            } catch (Exception e) {
                LOG.error("Error processing record key={}: {}", record.key(), e.getMessage(), e);
            }
            committer.markProcessed(record);
        }

        // Flush tables that have reached thresholds
        try {
            flushTablesIfNeeded();
        } catch (Exception e) {
            LOG.error("Error flushing tables after batch", e);
        }

        committer.markBatchFinished();
    }

    private void processRecord(ChangeEvent<String, String> record) throws Exception {
        String topic = record.destination();
        String tableFolder = topicToTableFolder(topic);

        // Tombstone: null value means delete
        if (record.value() == null) {
            String keyJson = record.key();
            Map<String, Object> row = new HashMap<>();
            if (keyJson != null) {
                JsonNode keyNode = objectMapper.readTree(keyJson);
                if (keyNode.has("payload")) {
                    keyNode = keyNode.get("payload");
                }
                keyNode.fields().forEachRemaining(e -> row.put(e.getKey(), jsonNodeToObject(e.getValue())));
            }
            row.put(config.rowMarkerColumn, MARKER_DELETE);
            bufferRow(tableFolder, row);
            return;
        }

        JsonNode root = objectMapper.readTree(record.value());

        // Unwrap Debezium envelope
        JsonNode payload = root.has("payload") ? root.get("payload") : root;
        String op = payload.has("op") ? payload.get("op").asText() : "c";

        JsonNode afterNode = payload.get("after");
        JsonNode beforeNode = payload.get("before");

        Map<String, Object> row;
        int marker;

        switch (op) {
            case "c", "r" -> {
                // "c" = create (INSERT), "r" = snapshot read — both produce INSERT rows in the landing zone
                row = jsonNodeToMap(afterNode);
                marker = MARKER_INSERT;
            }
            case "d" -> {
                // Delete
                row = jsonNodeToMap(beforeNode);
                marker = MARKER_DELETE;
            }
            case "u" -> {
                // Update
                row = jsonNodeToMap(afterNode);
                // If fetchRowOnUpdate enabled and "after" is null/empty, fetch from Oracle
                if (config.fetchRowOnUpdate && metadataLoader != null &&
                        (row == null || row.isEmpty()) && beforeNode != null) {
                    row = fetchUpdatedRow(tableFolder, beforeNode);
                }
                if (row == null) row = jsonNodeToMap(beforeNode);
                marker = MARKER_UPDATE;
            }
            default -> {
                LOG.warn("Unknown op '{}' for topic {}, treating as insert", op, topic);
                row = jsonNodeToMap(afterNode != null ? afterNode : beforeNode);
                marker = MARKER_INSERT;
            }
        }

        if (row == null) {
            LOG.warn("Null row for op={} on topic={}, skipping", op, topic);
            return;
        }

        row.put(config.rowMarkerColumn, marker);
        bufferRow(tableFolder, row);
    }

    private Map<String, Object> fetchUpdatedRow(String tableFolder, JsonNode beforeNode) {
        try {
            String[] parts = tableFolder.split("_", 2);
            if (parts.length < 2) return null;
            String schema = parts[0];
            String tableName = parts[1];
            TableMetadata meta = getOrLoadMetadata(tableFolder, schema, tableName);
            if (meta == null || meta.getPkColumns().isEmpty()) return null;

            Map<String, Object> pkValues = new HashMap<>();
            Map<String, Object> beforeMap = jsonNodeToMap(beforeNode);
            for (String pk : meta.getPkColumns()) {
                pkValues.put(pk, beforeMap.get(pk));
            }
            return metadataLoader.fetchRowByPk(schema, tableName, pkValues);
        } catch (Exception e) {
            LOG.warn("fetchRowByPk failed for {}: {}", tableFolder, e.getMessage());
            return null;
        }
    }

    private synchronized void bufferRow(String tableFolder, Map<String, Object> row) {
        tableBuffers.computeIfAbsent(tableFolder, k -> new ArrayList<>()).add(row);
    }

    private void flushTablesIfNeeded() throws Exception {
        for (Map.Entry<String, List<Map<String, Object>>> entry : tableBuffers.entrySet()) {
            String tableFolder = entry.getKey();
            List<Map<String, Object>> buffer = entry.getValue();
            if (buffer.size() >= config.flushMaxRecords) {
                flushTable(tableFolder);
            }
        }
    }

    private void backgroundFlush() {
        try {
            flushAllTables();
        } catch (Exception e) {
            LOG.error("Background flush error", e);
        }
    }

    private void flushAllTables() throws Exception {
        for (String tableFolder : tableBuffers.keySet()) {
            flushTable(tableFolder);
        }
    }

    private synchronized void flushTable(String tableFolder) throws Exception {
        List<Map<String, Object>> buffer = tableBuffers.get(tableFolder);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        // Snapshot and clear the buffer
        List<Map<String, Object>> rows = new ArrayList<>(buffer);
        buffer.clear();

        String[] parts = tableFolder.split("_", 2);
        String schema = parts.length >= 2 ? parts[0] : tableFolder;
        String tableName = parts.length >= 2 ? parts[1] : tableFolder;

        // Ensure table is initialized (metadata.json, sequence)
        ensureTableInitialized(tableFolder, schema, tableName);

        TableMetadata meta = tableMetadataMap.get(tableFolder);
        if (meta == null) {
            LOG.warn("No metadata for {}, cannot write Parquet", tableFolder);
            return;
        }

        long seq = sequenceManager.nextSequence(tableFolder);
        String filename = SequenceManager.toFilename(seq);
        Path tempFile = tempDir.resolve(tableFolder + "_" + filename);

        try {
            parquetWriter.write(meta, config.rowMarkerColumn, rows, config.compression, tempFile);
            storage.uploadFile(tableFolder, filename, tempFile);
            sequenceManager.commitSequence(tableFolder, seq);
            LOG.info("Flushed {} rows to {}/{}", rows.size(), tableFolder, filename);
        } catch (Exception e) {
            LOG.error("Failed to flush {} rows to {}: {}", rows.size(), tableFolder, e.getMessage(), e);
            // Re-buffer rows to avoid data loss; prepend failed rows before any new rows
            // that arrived while flushing to maintain chronological order
            synchronized (this) {
                List<Map<String, Object>> current = tableBuffers.computeIfAbsent(tableFolder, k -> new ArrayList<>());
                List<Map<String, Object>> merged = new ArrayList<>(rows);
                merged.addAll(current);
                tableBuffers.put(tableFolder, merged);
            }
            throw e;
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private void ensureTableInitialized(String tableFolder, String schema, String tableName) {
        if (initializedTables.containsKey(tableFolder)) return;

        try {
            TableMetadata meta = getOrLoadMetadata(tableFolder, schema, tableName);
            if (meta != null) {
                sequenceManager.initTable(tableFolder);
                metadataManager.ensureMetadata(tableFolder, meta);
            }
            initializedTables.put(tableFolder, Boolean.TRUE);
        } catch (Exception e) {
            LOG.error("Failed to initialize table {}: {}", tableFolder, e.getMessage(), e);
        }
    }

    private TableMetadata getOrLoadMetadata(String tableFolder, String schema, String tableName) {
        TableMetadata cached = tableMetadataMap.get(tableFolder);
        if (cached != null) return cached;

        if (metadataLoader != null) {
            try {
                TableMetadata meta = metadataLoader.getTableMetadata(schema, tableName);
                tableMetadataMap.put(tableFolder, meta);
                return meta;
            } catch (Exception e) {
                LOG.warn("Could not load Oracle metadata for {}.{}: {}", schema, tableName, e.getMessage());
            }
        }
        return null;
    }

    /**
     * Converts a Debezium topic name to a table folder name.
     * "oracle.HR.EMPLOYEES" with prefix "oracle" → "HR_EMPLOYEES"
     */
    private String topicToTableFolder(String topic) {
        String stripped = topic;
        if (!config.topicPrefix.isBlank() && topic.startsWith(config.topicPrefix + ".")) {
            stripped = topic.substring(config.topicPrefix.length() + 1);
        }
        // Replace first dot with underscore to get SCHEMA_TABLE
        return stripped.replace(".", "_");
    }

    private Map<String, Object> jsonNodeToMap(JsonNode node) {
        if (node == null || node.isNull()) return new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        node.fields().forEachRemaining(e -> map.put(e.getKey(), jsonNodeToObject(e.getValue())));
        return map;
    }

    private Object jsonNodeToObject(JsonNode node) {
        if (node == null || node.isNull()) return null;
        if (node.isBoolean()) return node.booleanValue();
        if (node.isInt()) return node.intValue();
        if (node.isLong()) return node.longValue();
        if (node.isDouble()) return node.doubleValue();
        if (node.isFloat()) return node.floatValue();
        if (node.isTextual()) return node.textValue();
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (IOException e) {
                return node.textValue();
            }
        }
        return node.toString();
    }

    /**
     * Allows tests to pre-register table metadata without a live Oracle connection.
     */
    public void registerTableMetadata(String tableFolder, TableMetadata metadata) {
        tableMetadataMap.put(tableFolder, metadata);
    }
}
