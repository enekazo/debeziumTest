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

public class OracleMetadataLoader {

    private static final Logger LOG = LoggerFactory.getLogger(OracleMetadataLoader.class);

    private static final String SQL_COLUMNS =
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID " +
            "FROM ALL_TAB_COLUMNS " +
            "WHERE OWNER = ? AND TABLE_NAME = ? " +
            "ORDER BY COLUMN_ID";

    private static final String SQL_PK =
            "SELECT acc.COLUMN_NAME, acc.POSITION " +
            "FROM ALL_CONSTRAINTS ac " +
            "JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME AND ac.OWNER = acc.OWNER " +
            "WHERE ac.OWNER = ? AND ac.TABLE_NAME = ? AND ac.CONSTRAINT_TYPE = 'P' " +
            "ORDER BY acc.POSITION";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long refreshIntervalMs;

    private final ConcurrentHashMap<String, CachedMetadata> cache = new ConcurrentHashMap<>();

    public OracleMetadataLoader(String jdbcUrl, String username, String password, long refreshIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    public TableMetadata getTableMetadata(String schema, String tableName) {
        String key = schema.toUpperCase() + "." + tableName.toUpperCase();
        CachedMetadata cached = cache.get(key);
        if (cached != null && !cached.isExpired(refreshIntervalMs)) {
            return cached.metadata;
        }
        TableMetadata metadata = loadFromDatabase(schema.toUpperCase(), tableName.toUpperCase());
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
                    String colName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getObject("DATA_PRECISION") != null ? rs.getInt("DATA_PRECISION") : 0;
                    int scale = rs.getObject("DATA_SCALE") != null ? rs.getInt("DATA_SCALE") : 0;
                    boolean nullable = "Y".equals(rs.getString("NULLABLE"));
                    int columnId = rs.getInt("COLUMN_ID");
                    columns.add(new ColumnMetadata(colName, dataType, precision, scale, nullable, columnId));
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
                    pkColumns.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        return pkColumns;
    }

    /**
     * Fetches a full row from Oracle by primary key values.
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
