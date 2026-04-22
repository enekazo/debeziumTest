package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.ForeignKeyMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link RandomDataGenerator}.
 * No Oracle connection required — uses metadata built in-process.
 */
public class RandomDataGeneratorTest {

    // Use a fixed seed so tests are deterministic
    private final RandomDataGenerator gen = new RandomDataGenerator(0.0, 42L);
    private final ParentKeyPool emptyPool = new ParentKeyPool();
    private final Map<String, Long> counters = new HashMap<>();

    // ── NUMBER types ────────────────────────────────────────────────────────

    @Test
    public void testNumberPrecision10Scale0ReturnsLong() {
        ColumnMetadata col = new ColumnMetadata("ID", "NUMBER", 10, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("Expected Long for NUMBER(10,0)", value instanceof Long);
    }

    @Test
    public void testNumberPrecision6Scale0ReturnsInteger() {
        ColumnMetadata col = new ColumnMetadata("DEPT_ID", "NUMBER", 6, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("Expected Integer for NUMBER(6,0)", value instanceof Integer);
    }

    @Test
    public void testNumberPrecision8Scale2ReturnsDouble() {
        ColumnMetadata col = new ColumnMetadata("SALARY", "NUMBER", 8, 2, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("Expected Double for NUMBER(8,2)", value instanceof Double);
    }

    @Test
    public void testNumberNoArgReturnsLong() {
        // Precision=0 → fallback to 10, scale=0 → Long
        ColumnMetadata col = new ColumnMetadata("VAL", "NUMBER", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        // precision defaults to 10, scale=0 → Long
        assertTrue("Expected Long for NUMBER with no precision", value instanceof Long);
    }

    // ── String types ────────────────────────────────────────────────────────

    @Test
    public void testVarchar2ReturnsString() {
        ColumnMetadata col = new ColumnMetadata("NAME", "VARCHAR2", 50, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("Expected String for VARCHAR2", value instanceof String);
        assertTrue("VARCHAR2 length should be ≤ 30", ((String) value).length() <= 30);
        assertFalse("VARCHAR2 should not be empty", ((String) value).isEmpty());
    }

    @Test
    public void testChar1ReturnsStringOfLength1() {
        ColumnMetadata col = new ColumnMetadata("FLAG", "CHAR", 1, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue(value instanceof String);
        assertEquals("CHAR(1) length must be 1", 1, ((String) value).length());
    }

    @Test
    public void testCharNReturnsStringOfExactLength() {
        ColumnMetadata col = new ColumnMetadata("CODE", "CHAR", 5, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue(value instanceof String);
        assertEquals("CHAR(5) length must be 5", 5, ((String) value).length());
    }

    @Test
    public void testClobReturnsNonEmptyString() {
        ColumnMetadata col = new ColumnMetadata("NOTES", "CLOB", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("CLOB should return a String", value instanceof String);
        assertFalse("CLOB string should not be empty", ((String) value).isEmpty());
    }

    // ── Date / Timestamp types ───────────────────────────────────────────────

    @Test
    public void testDateReturnsSqlDate() {
        ColumnMetadata col = new ColumnMetadata("HIRE_DATE", "DATE", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("DATE should return java.sql.Date", value instanceof Date);
    }

    @Test
    public void testTimestampReturnsSqlTimestamp() {
        ColumnMetadata col = new ColumnMetadata("CREATED_AT", "TIMESTAMP", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("TIMESTAMP should return java.sql.Timestamp", value instanceof Timestamp);
    }

    // ── Float types ──────────────────────────────────────────────────────────

    @Test
    public void testBinaryDoubleReturnsDouble() {
        ColumnMetadata col = new ColumnMetadata("RATE", "BINARY_DOUBLE", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("BINARY_DOUBLE should return Double", value instanceof Double);
    }

    @Test
    public void testFloatReturnsDouble() {
        ColumnMetadata col = new ColumnMetadata("PRICE", "FLOAT", 0, 0, false, 1);
        Object value = gen.generateByType(col);
        assertNotNull(value);
        assertTrue("FLOAT should return Double", value instanceof Double);
    }

    // ── Nullable behaviour ───────────────────────────────────────────────────

    @Test
    public void testNullableColumnCanProduceNull() {
        // 100% null probability → every nullable column must be null
        RandomDataGenerator alwaysNull = new RandomDataGenerator(1.0, 0L);
        TableMetadata meta = buildSimpleMeta();
        Map<String, Object> row = alwaysNull.generateRow(
                "HR", "TEST", meta, emptyPool, new HashMap<>(), null);

        // FIRST_NAME (nullable, not PK) must be null
        assertTrue("Nullable column should be null with p=1.0",
                row.containsKey("FIRST_NAME") && row.get("FIRST_NAME") == null);
    }

    @Test
    public void testNonNullableColumnNeverNull() {
        // 100% null probability — but non-nullable column must still have a value
        RandomDataGenerator alwaysNull = new RandomDataGenerator(1.0, 0L);
        TableMetadata meta = buildSimpleMeta();
        for (int i = 0; i < 20; i++) {
            Map<String, Object> row = alwaysNull.generateRow(
                    "HR", "TEST", meta, emptyPool, new HashMap<>(), null);
            assertNotNull("LAST_NAME is non-nullable and must not be null", row.get("LAST_NAME"));
        }
    }

    // ── PK counter behaviour ─────────────────────────────────────────────────

    @Test
    public void testPkColumnUsesAutoIncrementCounter() {
        TableMetadata meta = buildSimpleMeta();
        Map<String, Long> counters = new HashMap<>();

        Map<String, Object> row1 = gen.generateRow("HR", "TEST", meta, emptyPool, counters, null);
        Map<String, Object> row2 = gen.generateRow("HR", "TEST", meta, emptyPool, counters, null);
        Map<String, Object> row3 = gen.generateRow("HR", "TEST", meta, emptyPool, counters, null);

        long id1 = ((Number) row1.get("EMPLOYEE_ID")).longValue();
        long id2 = ((Number) row2.get("EMPLOYEE_ID")).longValue();
        long id3 = ((Number) row3.get("EMPLOYEE_ID")).longValue();

        assertEquals("PK values must increment by 1", id1 + 1, id2);
        assertEquals("PK values must increment by 1", id2 + 1, id3);
    }

    // ── FK resolution ────────────────────────────────────────────────────────

    @Test
    public void testFkColumnPulledFromParentPool() {
        ParentKeyPool pool = new ParentKeyPool();
        pool.addKeys("HR", "DEPARTMENTS", "DEPARTMENT_ID", List.of(10L, 20L, 30L));

        // Build a table with a FK on DEPARTMENT_ID → HR.DEPARTMENTS.DEPARTMENT_ID
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("EMPLOYEE_ID",  "NUMBER", 6, 0, false, 1));
        columns.add(new ColumnMetadata("LAST_NAME",    "VARCHAR2", 25, 0, false, 2));
        columns.add(new ColumnMetadata("DEPARTMENT_ID","NUMBER", 4, 0, true,  3));

        List<ForeignKeyMetadata> fks = List.of(
                new ForeignKeyMetadata("DEPARTMENT_ID", "HR", "DEPARTMENTS", "DEPARTMENT_ID"));

        TableMetadata meta = new TableMetadata("HR", "EMPLOYEES", columns,
                List.of("EMPLOYEE_ID"), fks, List.of());

        Map<String, Object> row = gen.generateRow("HR", "EMPLOYEES", meta, pool,
                                                  new HashMap<>(), null);

        Object deptId = row.get("DEPARTMENT_ID");
        assertNotNull("FK column must have a value from the pool", deptId);
        assertTrue("FK value must come from parent pool",
                List.of(10L, 20L, 30L).contains(((Number) deptId).longValue()));
    }

    // ── String utility ───────────────────────────────────────────────────────

    @Test
    public void testRandomStringLength() {
        String s = gen.randomString(10);
        assertEquals(10, s.length());
        assertTrue("Random string must be alphanumeric",
                s.chars().allMatch(c -> Character.isLetterOrDigit((char) c)));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static TableMetadata buildSimpleMeta() {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("EMPLOYEE_ID", "NUMBER", 6, 0, false, 1));
        columns.add(new ColumnMetadata("FIRST_NAME",  "VARCHAR2", 20, 0, true,  2));
        columns.add(new ColumnMetadata("LAST_NAME",   "VARCHAR2", 25, 0, false, 3));
        return new TableMetadata("HR", "TEST", columns, List.of("EMPLOYEE_ID"));
    }
}
