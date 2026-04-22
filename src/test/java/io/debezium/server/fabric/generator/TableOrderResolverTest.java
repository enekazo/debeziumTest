package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.ForeignKeyMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TableOrderResolver}.
 * Verifies topological ordering and cycle detection without an Oracle connection.
 */
public class TableOrderResolverTest {

    // ── basic ordering ───────────────────────────────────────────────────────

    @Test
    public void testNoDependenciesReturnsInputOrder() {
        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("HR.A", noFkTable("HR", "A"));
        map.put("HR.B", noFkTable("HR", "B"));
        map.put("HR.C", noFkTable("HR", "C"));

        List<String> ordered = TableOrderResolver.resolve(
                List.of("HR.A", "HR.B", "HR.C"), map);

        assertEquals(3, ordered.size());
        // All tables present; order can vary but nothing skipped
        assertTrue(ordered.containsAll(List.of("HR.A", "HR.B", "HR.C")));
    }

    @Test
    public void testParentBeforeChild() {
        // EMPLOYEES has FK → DEPARTMENTS
        TableMetadata departments = noFkTable("HR", "DEPARTMENTS");
        TableMetadata employees   = tableWithFk("HR", "EMPLOYEES",
                new ForeignKeyMetadata("DEPARTMENT_ID", "HR", "DEPARTMENTS", "DEPARTMENT_ID"));

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        // Insert in "wrong" order to verify resolver fixes it
        map.put("HR.EMPLOYEES",   employees);
        map.put("HR.DEPARTMENTS", departments);

        List<String> ordered = TableOrderResolver.resolve(
                List.of("HR.EMPLOYEES", "HR.DEPARTMENTS"), map);

        assertEquals(2, ordered.size());
        int deptIdx = ordered.indexOf("HR.DEPARTMENTS");
        int empIdx  = ordered.indexOf("HR.EMPLOYEES");
        assertTrue("DEPARTMENTS must precede EMPLOYEES", deptIdx < empIdx);
    }

    @Test
    public void testMultiLevelChain() {
        // C → B → A  (A must be first, then B, then C)
        TableMetadata a = noFkTable("S", "A");
        TableMetadata b = tableWithFk("S", "B",
                new ForeignKeyMetadata("A_ID", "S", "A", "ID"));
        TableMetadata c = tableWithFk("S", "C",
                new ForeignKeyMetadata("B_ID", "S", "B", "ID"));

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("S.A", a);
        map.put("S.B", b);
        map.put("S.C", c);

        List<String> ordered = TableOrderResolver.resolve(List.of("S.A", "S.B", "S.C"), map);

        assertEquals(3, ordered.size());
        assertTrue("A before B", ordered.indexOf("S.A") < ordered.indexOf("S.B"));
        assertTrue("B before C", ordered.indexOf("S.B") < ordered.indexOf("S.C"));
    }

    // ── cycle detection ──────────────────────────────────────────────────────

    @Test
    public void testCycleTableIsExcluded() {
        // A → B, B → A  (cycle between A and B)
        TableMetadata a = tableWithFk("S", "A",
                new ForeignKeyMetadata("B_ID", "S", "B", "ID"));
        TableMetadata b = tableWithFk("S", "B",
                new ForeignKeyMetadata("A_ID", "S", "A", "ID"));

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("S.A", a);
        map.put("S.B", b);

        List<String> ordered = TableOrderResolver.resolve(List.of("S.A", "S.B"), map);

        // Both tables are in a cycle — neither should appear in the ordered list
        assertTrue("Tables forming a cycle must be excluded",
                !ordered.contains("S.A") && !ordered.contains("S.B"));
    }

    @Test
    public void testCycleExcludedButOtherTablesIncluded() {
        // C is independent; A and B form a cycle
        TableMetadata a = tableWithFk("S", "A",
                new ForeignKeyMetadata("B_ID", "S", "B", "ID"));
        TableMetadata b = tableWithFk("S", "B",
                new ForeignKeyMetadata("A_ID", "S", "A", "ID"));
        TableMetadata c = noFkTable("S", "C");

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("S.A", a);
        map.put("S.B", b);
        map.put("S.C", c);

        List<String> ordered = TableOrderResolver.resolve(List.of("S.A", "S.B", "S.C"), map);

        assertEquals("Only C (no cycle) should remain", 1, ordered.size());
        assertTrue(ordered.contains("S.C"));
    }

    // ── FK to external parent (not in list) ──────────────────────────────────

    @Test
    public void testFkToExternalParentDoesNotAffectOrder() {
        // EMPLOYEES has FK → DEPARTMENTS, but DEPARTMENTS is not in the requested list
        TableMetadata employees = tableWithFk("HR", "EMPLOYEES",
                new ForeignKeyMetadata("DEPARTMENT_ID", "HR", "DEPARTMENTS", "DEPARTMENT_ID"));

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("HR.EMPLOYEES", employees);

        List<String> ordered = TableOrderResolver.resolve(List.of("HR.EMPLOYEES"), map);

        // The FK edge to an external table is ignored; EMPLOYEES should still appear
        assertEquals(1, ordered.size());
        assertTrue(ordered.contains("HR.EMPLOYEES"));
    }

    // ── duplicate FK edges (composite FK) ───────────────────────────────────

    @Test
    public void testCompositeFkDoesNotDuplicateEdge() {
        // Same parent referenced twice (composite FK on two columns to the same parent)
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("ID", "NUMBER", 6, 0, false, 1));
        columns.add(new ColumnMetadata("DEPT_ID1", "NUMBER", 4, 0, true, 2));
        columns.add(new ColumnMetadata("DEPT_ID2", "NUMBER", 4, 0, true, 3));

        List<ForeignKeyMetadata> fks = new ArrayList<>();
        fks.add(new ForeignKeyMetadata("DEPT_ID1", "HR", "DEPARTMENTS", "DEPARTMENT_ID1"));
        fks.add(new ForeignKeyMetadata("DEPT_ID2", "HR", "DEPARTMENTS", "DEPARTMENT_ID2"));

        TableMetadata child = new TableMetadata("HR", "CHILD", columns,
                List.of("ID"), fks, List.of());
        TableMetadata parent = noFkTable("HR", "DEPARTMENTS");

        Map<String, TableMetadata> map = new LinkedHashMap<>();
        map.put("HR.DEPARTMENTS", parent);
        map.put("HR.CHILD",       child);

        List<String> ordered = TableOrderResolver.resolve(
                List.of("HR.DEPARTMENTS", "HR.CHILD"), map);

        assertEquals(2, ordered.size());
        assertTrue("DEPARTMENTS must precede CHILD",
                ordered.indexOf("HR.DEPARTMENTS") < ordered.indexOf("HR.CHILD"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static TableMetadata noFkTable(String schema, String table) {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("ID", "NUMBER", 6, 0, false, 1));
        return new TableMetadata(schema, table, columns, List.of("ID"));
    }

    private static TableMetadata tableWithFk(String schema, String table,
                                             ForeignKeyMetadata fk) {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("ID", "NUMBER", 6, 0, false, 1));
        columns.add(new ColumnMetadata(fk.columnName, "NUMBER", 4, 0, true, 2));
        List<ForeignKeyMetadata> fks = new ArrayList<>();
        fks.add(fk);
        return new TableMetadata(schema, table, columns, List.of("ID"), fks, List.of());
    }
}
