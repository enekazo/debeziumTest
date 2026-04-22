package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.ForeignKeyMetadata;
import io.debezium.server.fabric.metadata.OracleMetadataLoader;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Integration test for the dummy data generator against a live Oracle instance.
 *
 * <p>This test is <strong>skipped automatically</strong> if the Oracle JDBC
 * environment variables are not set. To run it, start the Docker Compose stack and set:
 * <pre>
 *   export ORACLE_JDBC_URL=jdbc:oracle:thin:@//localhost:1521/ORCLPDB1
 *   export ORACLE_USER=hr
 *   export ORACLE_PASSWORD=hr
 * </pre>
 * Then run: {@code mvn test -Dtest=DataGeneratorIntegrationTest}
 * </p>
 */
public class DataGeneratorIntegrationTest {

    private static final String ENV_JDBC_URL = "ORACLE_JDBC_URL";
    private static final String ENV_USER     = "ORACLE_USER";
    private static final String ENV_PASSWORD = "ORACLE_PASSWORD";

    private String jdbcUrl;
    private String user;
    private String password;

    @Before
    public void setUp() {
        jdbcUrl  = System.getenv(ENV_JDBC_URL);
        user     = System.getenv(ENV_USER);
        password = System.getenv(ENV_PASSWORD);

        // Skip when Oracle is not available
        Assume.assumeTrue("Oracle not configured — set ORACLE_JDBC_URL, ORACLE_USER, ORACLE_PASSWORD to run",
                jdbcUrl != null && user != null && password != null);
    }

    /**
     * Inserts one batch into HR.DEPARTMENTS, then one batch into HR.EMPLOYEES.
     * Verifies that:
     * <ul>
     *   <li>Row counts increase for both tables.</li>
     *   <li>Every DEPARTMENT_ID in EMPLOYEES references a valid DEPARTMENTS row.</li>
     * </ul>
     */
    @Test
    public void testInsertBatchesRespectForeignKeys() throws Exception {
        OracleMetadataLoader loader = new OracleMetadataLoader(jdbcUrl, user, password, 60_000L);

        TableMetadata deptMeta = loader.getTableMetadata("HR", "DEPARTMENTS");
        TableMetadata empMeta  = loader.getTableMetadata("HR", "EMPLOYEES");

        assertFalse("DEPARTMENTS must have at least one column", deptMeta.columns.isEmpty());
        assertFalse("EMPLOYEES must have at least one column",   empMeta.columns.isEmpty());

        ParentKeyPool pool = new ParentKeyPool();
        Map<String, Long> counters = new HashMap<>();
        Map<String, TableMetadata> metaMap = new LinkedHashMap<>();
        metaMap.put("HR.DEPARTMENTS", deptMeta);
        metaMap.put("HR.EMPLOYEES",   empMeta);

        List<String> ordered = TableOrderResolver.resolve(
                List.of("HR.DEPARTMENTS", "HR.EMPLOYEES"), metaMap);

        assertEquals("Both tables must be ordered (no cycles)", 2, ordered.size());
        assertEquals("DEPARTMENTS must come first", "HR.DEPARTMENTS", ordered.get(0));

        RandomDataGenerator gen = new RandomDataGenerator(0.0);
        int batchSize = 5;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            conn.setAutoCommit(false);

            long deptBefore = countRows(conn, "HR", "DEPARTMENTS");
            long empBefore  = countRows(conn, "HR", "EMPLOYEES");

            // Insert DEPARTMENTS first
            List<Map<String, Object>> deptRows = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                deptRows.add(gen.generateRow("HR", "DEPARTMENTS", deptMeta,
                        pool, counters, conn));
            }
            int deptInserted = InsertBuilder.insertBatch(conn, deptMeta, "HR", "DEPARTMENTS", deptRows);
            conn.commit();
            assertEquals("All department rows should be inserted", batchSize, deptInserted);

            // Feed new dept IDs into pool so EMPLOYEES FK can use them
            for (String pkCol : deptMeta.pkColumns) {
                List<Object> pkVals = new ArrayList<>();
                for (Map<String, Object> row : deptRows) {
                    Object v = row.get(pkCol);
                    if (v != null) pkVals.add(v);
                }
                pool.addKeys("HR", "DEPARTMENTS", pkCol, pkVals);
            }

            // Insert EMPLOYEES referencing the new departments
            List<Map<String, Object>> empRows = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                empRows.add(gen.generateRow("HR", "EMPLOYEES", empMeta,
                        pool, counters, conn));
            }
            int empInserted = InsertBuilder.insertBatch(conn, empMeta, "HR", "EMPLOYEES", empRows);
            conn.commit();
            assertEquals("All employee rows should be inserted", batchSize, empInserted);

            // Verify row counts increased
            long deptAfter = countRows(conn, "HR", "DEPARTMENTS");
            long empAfter  = countRows(conn, "HR", "EMPLOYEES");
            assertEquals("DEPARTMENTS count must increase by batch size",
                    deptBefore + batchSize, deptAfter);
            assertEquals("EMPLOYEES count must increase by batch size",
                    empBefore + batchSize, empAfter);

            // Verify FK validity: every DEPARTMENT_ID in newly-inserted employees must exist
            String fkCheckSql =
                    "SELECT COUNT(*) FROM HR.EMPLOYEES e " +
                    "WHERE e.DEPARTMENT_ID IS NOT NULL " +
                    "AND NOT EXISTS (SELECT 1 FROM HR.DEPARTMENTS d WHERE d.DEPARTMENT_ID = e.DEPARTMENT_ID)";
            try (PreparedStatement ps = conn.prepareStatement(fkCheckSql);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                long orphans = rs.getLong(1);
                assertEquals("No orphan DEPARTMENT_ID references should exist", 0L, orphans);
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private long countRows(Connection conn, String schema, String table) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM " + schema + "." + table);
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }
}
