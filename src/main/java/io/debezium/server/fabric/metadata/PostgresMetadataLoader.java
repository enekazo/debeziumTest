package io.debezium.server.fabric.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads table metadata (column definitions and primary keys) from a PostgreSQL database
 * using {@code information_schema}. Results are cached and refreshed on expiry.
 */
public class PostgresMetadataLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresMetadataLoader.class);

    private static final String SQL_COLUMNS =
            "SELECT column_name, data_type, numeric_precision, numeric_scale, " +
            "       is_nullable, ordinal_position " +
            "FROM information_schema.columns " +
            "WHERE table_schema = ? AND table_name = ? " +
            "ORDER BY ordinal_position";

    private static final String SQL_PK =
            "SELECT kcu.column_name, kcu.ordinal_position " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.key_column_usage kcu " +
            "    ON tc.constraint_name = kcu.constraint_name " +
            "    AND tc.table_schema = kcu.table_schema " +
            "WHERE tc.table_schema = ? AND tc.table_name = ? " +
            "  AND tc.constraint_type = 'PRIMARY KEY' " +
            "ORDER BY kcu.ordinal_position";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long refreshIntervalMs;

    private final ConcurrentHashMap<String, CachedMetadata> cache = new ConcurrentHashMap<>();

    public PostgresMetadataLoader(String jdbcUrl, String username, String password, long refreshIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    public TableMetadata getTableMetadata(String schema, String tableName) {
        String key = schema.toLowerCase() + "." + tableName.toLowerCase();
        CachedMetadata cached = cache.get(key);
        if (cached != null && !cached.isExpired(refreshIntervalMs)) {
            return cached.metadata;
        }
        TableMetadata metadata = loadFromDatabase(schema.toLowerCase(), tableName.toLowerCase());
        cache.put(key, new CachedMetadata(metadata));
        return metadata;
    }

    private TableMetadata loadFromDatabase(String schema, String tableName) {
        LOG.info("Loading metadata for {}.{}", schema, tableName);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            List<ColumnMetadata> columns = loadColumns(conn, schema, tableName);
            List<String> pkColumns = loadPkColumns(conn, schema, tableName);
            return new TableMetadata(schema, tableName, columns, pkColumns);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load metadata for " + schema + "." + tableName, e);
        }
    }

    private List<ColumnMetadata> loadColumns(Connection conn, String schema, String tableName) throws Exception {
        List<ColumnMetadata> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SQL_COLUMNS)) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    int precision = rs.getObject("numeric_precision") != null ? rs.getInt("numeric_precision") : 0;
                    int scale = rs.getObject("numeric_scale") != null ? rs.getInt("numeric_scale") : 0;
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
                    int ordinalPosition = rs.getInt("ordinal_position");
                    columns.add(new ColumnMetadata(colName, dataType, precision, scale, nullable, ordinalPosition));
                }
            }
        }
        return columns;
    }

    private List<String> loadPkColumns(Connection conn, String schema, String tableName) throws Exception {
        List<String> pkColumns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SQL_PK)) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pkColumns.add(rs.getString("column_name"));
                }
            }
        }
        return pkColumns;
    }

    /**
     * Fetches a full row from PostgreSQL by primary key values.
     * Used when Debezium update events have incomplete "after" data.
     */
    public Map<String, Object> fetchRowByPk(String schema, String tableName, Map<String, Object> pkValues) {
        if (pkValues == null || pkValues.isEmpty()) {
            return null;
        }
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(schema).append(".").append(tableName).append(" WHERE ");
        List<String> pkCols = new ArrayList<>(pkValues.keySet());
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append(pkCols.get(i)).append(" = ?");
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < pkCols.size(); i++) {
                ps.setObject(i + 1, pkValues.get(pkCols.get(i)));
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    int colCount = rs.getMetaData().getColumnCount();
                    for (int i = 1; i <= colCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    return row;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to fetch row for {}.{} with pk {}: {}", schema, tableName, pkValues, e.getMessage());
        }
        return null;
    }

    private static class CachedMetadata {
        final TableMetadata metadata;
        final long loadedAt;

        CachedMetadata(TableMetadata metadata) {
            this.metadata = metadata;
            this.loadedAt = System.currentTimeMillis();
        }

        boolean isExpired(long refreshIntervalMs) {
            return System.currentTimeMillis() - loadedAt > refreshIntervalMs;
        }
    }
}
