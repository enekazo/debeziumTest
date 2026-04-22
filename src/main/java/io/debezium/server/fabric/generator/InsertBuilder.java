package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds and executes batch INSERT statements for a target table.
 *
 * <p>One prepared statement is created per call to {@link #insertBatch};
 * rows are added via {@link PreparedStatement#addBatch()} and executed as
 * a single round-trip.</p>
 */
public class InsertBuilder {

    private InsertBuilder() {}

    /**
     * Inserts a batch of rows into {@code schema.table}.
     *
     * @param conn      open JDBC connection (auto-commit state is respected)
     * @param tableMeta table metadata used to determine column order
     * @param schema    Oracle schema name
     * @param table     Oracle table name
     * @param rows      list of row maps (column name → value)
     * @return total number of rows actually inserted (sum of update counts)
     * @throws SQLException if the INSERT fails
     */
    public static int insertBatch(Connection conn,
                                  TableMetadata tableMeta,
                                  String schema,
                                  String table,
                                  List<Map<String, Object>> rows) throws SQLException {
        if (rows == null || rows.isEmpty()) return 0;

        // Column list derived from metadata to keep order stable
        List<String> columns = tableMeta.columns.stream()
                .map(c -> c.name)
                .collect(Collectors.toList());

        String sql = buildSql(schema, table, columns);

        int inserted = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < columns.size(); i++) {
                    Object value = row.get(columns.get(i));
                    ps.setObject(i + 1, value);
                }
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            for (int c : counts) {
                if (c >= 0) inserted += c;
                else if (c == PreparedStatement.SUCCESS_NO_INFO) inserted++;
            }
        }
        return inserted;
    }

    // ── private helpers ──────────────────────────────────────────────────────

    static String buildSql(String schema, String table, List<String> columns) {
        String cols = columns.stream().collect(Collectors.joining(", "));
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + schema + "." + table +
               " (" + cols + ") VALUES (" + placeholders + ")";
    }
}
