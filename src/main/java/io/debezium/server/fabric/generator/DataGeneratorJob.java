package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.OracleMetadataLoader;
import io.debezium.server.fabric.metadata.TableMetadata;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background job that continuously inserts randomly-generated rows into a
 * configured list of Oracle tables.
 *
 * <p>Enabled by setting {@code generator.enabled=true} in application.properties.
 * When disabled (the default), this bean is created but immediately returns
 * without starting any threads — existing deployments are unaffected.</p>
 *
 * <p>Configuration summary:
 * <pre>
 *   generator.enabled         = true
 *   generator.tables          = HR.DEPARTMENTS,HR.EMPLOYEES
 *   generator.interval.ms     = 5000
 *   generator.batch.size      = 10
 *   generator.null.probability= 0.1
 * </pre>
 * Oracle JDBC credentials are reused from {@code fabric.oracle.*} unless overridden
 * with {@code generator.jdbcUrl}, {@code generator.username}, {@code generator.password}.
 * </p>
 */
@ApplicationScoped
public class DataGeneratorJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorJob.class);

    private ScheduledExecutorService scheduler;
    private GeneratorConfig config;
    private OracleMetadataLoader metadataLoader;
    private List<String> orderedTables;
    private final Map<String, TableMetadata> metadataMap = new LinkedHashMap<>();
    private final ParentKeyPool parentKeyPool = new ParentKeyPool();

    /**
     * Monotonic counters for PK / unique columns, shared across cycles.
     * Keyed by {@code "SCHEMA.TABLE.COLUMN"}.
     */
    private final Map<String, Long> counters = new HashMap<>();

    void onStart(@Observes StartupEvent ev) {
        try {
            config = GeneratorConfig.load();
        } catch (Exception e) {
            LOG.warn("DataGeneratorJob: could not load generator config ({}). Generator disabled.", e.getMessage());
            return;
        }

        if (!config.enabled) {
            LOG.info("DataGeneratorJob is disabled (generator.enabled=false). Skipping.");
            return;
        }

        if (config.tables.isEmpty()) {
            LOG.warn("DataGeneratorJob: generator.tables is empty. Nothing to generate.");
            return;
        }

        if (config.jdbcUrl == null || config.jdbcUrl.isBlank()) {
            LOG.error("DataGeneratorJob: generator.jdbcUrl (or fabric.oracle.jdbcUrl) must be configured.");
            return;
        }

        metadataLoader = new OracleMetadataLoader(
                config.jdbcUrl, config.username, config.password, 300_000L);

        // Load metadata and resolve topological order
        try {
            loadAllMetadata();
            orderedTables = TableOrderResolver.resolve(config.tables, metadataMap);
            LOG.info("DataGeneratorJob: resolved table order: {}", orderedTables);
            initCounters();
        } catch (Exception e) {
            LOG.error("DataGeneratorJob: failed during initialization — generator not started.", e);
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "data-generator");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(
                this::runCycle,
                0L,
                config.intervalMs,
                TimeUnit.MILLISECONDS);

        LOG.info("DataGeneratorJob started: tables={}, batchSize={}, intervalMs={}",
                orderedTables, config.batchSize, config.intervalMs);
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    // ── generation cycle ────────────────────────────────────────────────────

    private void runCycle() {
        try (Connection conn = openConnection()) {
            conn.setAutoCommit(false);

            RandomDataGenerator generator = new RandomDataGenerator(config.nullProbability);

            for (String tableKey : orderedTables) {
                TableMetadata meta = metadataMap.get(tableKey.toUpperCase());
                if (meta == null) {
                    LOG.warn("No metadata for table '{}', skipping.", tableKey);
                    continue;
                }

                String schema = meta.schema;
                String table  = meta.tableName;

                List<Map<String, Object>> rows = new ArrayList<>(config.batchSize);
                for (int i = 0; i < config.batchSize; i++) {
                    rows.add(generator.generateRow(schema, table, meta,
                                                   parentKeyPool, counters, conn));
                }

                try {
                    int inserted = InsertBuilder.insertBatch(conn, meta, schema, table, rows);
                    conn.commit();
                    LOG.debug("Inserted {} rows into {}.{}", inserted, schema, table);

                    // Feed new PK values into the parent pool for child-table FK resolution
                    for (String pkCol : meta.pkColumns) {
                        List<Object> pkValues = new ArrayList<>();
                        for (Map<String, Object> row : rows) {
                            Object v = row.get(pkCol);
                            if (v != null) pkValues.add(v);
                        }
                        parentKeyPool.addKeys(schema, table, pkCol, pkValues);
                    }
                } catch (SQLException e) {
                    LOG.error("Failed to insert batch into {}.{}: {}", schema, table, e.getMessage(), e);
                    try { conn.rollback(); } catch (SQLException re) { /* ignore */ }
                }
            }
        } catch (Exception e) {
            LOG.error("DataGeneratorJob cycle error: {}", e.getMessage(), e);
        }
    }

    // ── initialization helpers ───────────────────────────────────────────────

    private void loadAllMetadata() {
        for (String tableKey : config.tables) {
            String[] parts = parseSchemaTable(tableKey);
            String schema = parts[0].toUpperCase();
            String table  = parts[1].toUpperCase();
            try {
                TableMetadata meta = metadataLoader.getTableMetadata(schema, table);
                metadataMap.put((schema + "." + table).toUpperCase(), meta);
                LOG.info("Loaded metadata for {}.{}: {} columns, {} FKs",
                        schema, table, meta.columns.size(), meta.foreignKeys.size());
            } catch (Exception e) {
                LOG.error("Could not load metadata for {}.{}: {}", schema, table, e.getMessage(), e);
            }
        }
    }

    /**
     * Seeds the PK counter for each table from the current database maximum so
     * that generated keys never collide with existing rows.
     */
    private void initCounters() {
        try (Connection conn = openConnection()) {
            for (Map.Entry<String, TableMetadata> entry : metadataMap.entrySet()) {
                TableMetadata meta = entry.getValue();
                for (String pkCol : meta.pkColumns) {
                    ColumnMetadata colMeta = meta.columns.stream()
                            .filter(c -> c.name.equalsIgnoreCase(pkCol))
                            .findFirst().orElse(null);
                    if (colMeta == null) continue;

                    // Only auto-increment numeric PK columns
                    String type = colMeta.oracleType.toUpperCase();
                    if (!type.startsWith("NUMBER") && !type.equals("INTEGER")
                            && !type.equals("INT") && !type.equals("SMALLINT")) continue;

                    long max = queryMax(conn, meta.schema, meta.tableName, pkCol);
                    String counterKey = meta.schema + "." + meta.tableName + "." + pkCol.toUpperCase();
                    counters.put(counterKey, max);
                    LOG.debug("Counter for {} initialized to {}", counterKey, max);
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not initialize PK counters from DB: {}", e.getMessage());
        }
    }

    private long queryMax(Connection conn, String schema, String table, String column)
            throws SQLException {
        String sql = "SELECT NVL(MAX(" + column + "), 0) FROM " + schema + "." + table;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
        }
        return 0L;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
    }

    private static String[] parseSchemaTable(String tableKey) {
        int dot = tableKey.indexOf('.');
        if (dot <= 0 || dot >= tableKey.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid table key '" + tableKey + "'. Expected format: SCHEMA.TABLE");
        }
        return new String[]{tableKey.substring(0, dot), tableKey.substring(dot + 1)};
    }
}
