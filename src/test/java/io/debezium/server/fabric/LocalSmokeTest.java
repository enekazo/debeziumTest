package io.debezium.server.fabric;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import io.debezium.server.fabric.parquet.AvroSchemaBuilder;
import io.debezium.server.fabric.parquet.ParquetFileWriter;
import io.debezium.server.fabric.storage.LocalStorageBackend;
import io.debezium.server.fabric.storage.SequenceManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Local smoke test for the Fabric sink Parquet writing pipeline.
 * Uses LocalStorageBackend with a temp directory; no PostgreSQL or Azure connection required.
 */
public class LocalSmokeTest {

    private static final String TABLE_FOLDER = "HR_EMPLOYEES";
    private static final String ROW_MARKER_COLUMN = "__rowMarker__";

    private Path tempDir;
    private LocalStorageBackend storage;
    private TableMetadata tableMetadata;

    @Before
    public void setUp() throws Exception {
        // Write to a fixed folder so generated Parquet files can be inspected after the run.
        // Output location: target/test-output/
        tempDir = Paths.get("target", "test-output").toAbsolutePath();
        Files.createDirectories(tempDir);

        // Ensure deterministic test state for sequence/file assertions.
        deleteDirectoryIfExists(tempDir.resolve(TABLE_FOLDER));

        storage = new LocalStorageBackend(tempDir.toUri().toString());

        // Define test table schema: id bigint PK, first_name varchar, last_name varchar,
        //                            hire_date date, salary numeric(8,2)
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("EMPLOYEE_ID", "bigint",            0,  0, false, 1));
        columns.add(new ColumnMetadata("FIRST_NAME",  "character varying", 0,  0, true,  2));
        columns.add(new ColumnMetadata("LAST_NAME",   "character varying", 0,  0, false, 3));
        columns.add(new ColumnMetadata("HIRE_DATE",   "date",              0,  0, true,  4));
        columns.add(new ColumnMetadata("SALARY",      "numeric",           8,  2, true,  5));

        tableMetadata = new TableMetadata("HR", "EMPLOYEES", columns, List.of("EMPLOYEE_ID"));
    }

    @After
    public void tearDown() throws Exception {
        // Output kept on disk intentionally — inspect generated files at target/test-output/
        // To clean: run `mvn clean` or delete the folder manually.
    }

    @Test
    public void testWriteAndReadParquet() throws Exception {
        ParquetFileWriter writer = new ParquetFileWriter();

        // Build 3 rows: insert, update, delete
        List<Map<String, Object>> rows = buildTestRows();

        // Write to a local temp file then "upload" via storage backend
        Path localTemp = tempDir.resolve("test-write.parquet");
        writer.write(tableMetadata, ROW_MARKER_COLUMN, rows, "SNAPPY", localTemp);

        // Upload via storage backend
        storage.uploadFile(TABLE_FOLDER, "00000000000000000001.parquet", localTemp);

        // Verify the file exists in storage
        assertTrue("Parquet file should exist in storage",
                storage.exists(TABLE_FOLDER, "00000000000000000001.parquet"));

        // Read back and verify
        Path storedFile = tempDir.resolve(TABLE_FOLDER).resolve("00000000000000000001.parquet");
        assertTrue("Stored parquet file must exist on disk", Files.exists(storedFile));

        List<GenericRecord> readRecords = readParquet(storedFile);

        assertEquals("Should have 3 rows", 3, readRecords.size());

        // Row 0: INSERT (marker=0)
        // __rowMarker__ is non-nullable INT → Integer; EMPLOYEE_ID NUMBER(10,0) p>9 → Long
        assertEquals("Row 0 marker should be INSERT(0)", 0, readRecords.get(0).get(ROW_MARKER_COLUMN));
        assertEquals("Row 0 EMPLOYEE_ID should be 101", 101L, readRecords.get(0).get("EMPLOYEE_ID"));
        assertEquals("Row 0 LAST_NAME should be Smith",
                "Smith", readRecords.get(0).get("LAST_NAME").toString());

        // Row 1: UPDATE (marker=1)
        assertEquals("Row 1 marker should be UPDATE(1)", 1, readRecords.get(1).get(ROW_MARKER_COLUMN));
        assertEquals("Row 1 EMPLOYEE_ID should be 102", 102L, readRecords.get(1).get("EMPLOYEE_ID"));

        // Row 2: DELETE (marker=2)
        assertEquals("Row 2 marker should be DELETE(2)", 2, readRecords.get(2).get(ROW_MARKER_COLUMN));
        assertEquals("Row 2 EMPLOYEE_ID should be 103", 103L, readRecords.get(2).get("EMPLOYEE_ID"));
    }

    @Test
    public void testSequenceManager() throws Exception {
        SequenceManager sequenceManager = new SequenceManager(storage, "_sequence.txt");

        // New table: should start at 0
        long initial = sequenceManager.initTable(TABLE_FOLDER);
        assertEquals("Initial sequence should be 0", 0L, initial);

        // Next sequence: 1
        long seq1 = sequenceManager.nextSequence(TABLE_FOLDER);
        assertEquals("First next should be 1", 1L, seq1);

        // Commit and re-initialize (simulates restart)
        sequenceManager.commitSequence(TABLE_FOLDER, seq1);

        SequenceManager restarted = new SequenceManager(storage, "_sequence.txt");
        long restored = restarted.initTable(TABLE_FOLDER);
        assertEquals("Restored sequence should be 1", 1L, restored);

        long seq2 = restarted.nextSequence(TABLE_FOLDER);
        assertEquals("Second next after restart should be 2", 2L, seq2);
    }

    @Test
    public void testAvroSchemaBuilt() {
        AvroSchemaBuilder schemaBuilder = new AvroSchemaBuilder();
        var schema = schemaBuilder.buildSchema(tableMetadata, ROW_MARKER_COLUMN);

        assertNotNull("Schema should not be null", schema);
        assertNotNull("Schema should have EMPLOYEE_ID field", schema.getField("EMPLOYEE_ID"));
        assertNotNull("Schema should have HIRE_DATE field", schema.getField("HIRE_DATE"));
        assertNotNull("Schema should have __rowMarker__ field", schema.getField(ROW_MARKER_COLUMN));

        // HIRE_DATE should map to Avro INT with logicalType=date
        var hireDateField = schema.getField("HIRE_DATE");
        // Nullable → union; check inner type
        var innerSchema = hireDateField.schema().getTypes().stream()
                .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                .findFirst()
                .orElseThrow();
        assertEquals("HIRE_DATE Avro type should be INT", org.apache.avro.Schema.Type.INT, innerSchema.getType());
        assertEquals("HIRE_DATE logical type should be date", "date", innerSchema.getProp("logicalType"));

        // __rowMarker__ should be non-nullable INT
        var markerField = schema.getField(ROW_MARKER_COLUMN);
        assertEquals("__rowMarker__ Avro type should be INT",
                org.apache.avro.Schema.Type.INT, markerField.schema().getType());
    }

    @Test
    public void testFilenameFormat() {
        String name = SequenceManager.toFilename(1L);
        assertEquals("00000000000000000001.parquet", name);

        String max = SequenceManager.toFilename(Long.MAX_VALUE);
        assertEquals(20 + ".parquet".length(), max.length());
    }

    // ---- helpers ----

    private List<Map<String, Object>> buildTestRows() {
        List<Map<String, Object>> rows = new ArrayList<>();

        // Row 0: INSERT
        Map<String, Object> insert = new HashMap<>();
        insert.put("EMPLOYEE_ID", 101L);  // NUMBER(10,0) precision>9 → long
        insert.put("FIRST_NAME", "John");
        insert.put("LAST_NAME", "Smith");
        insert.put("HIRE_DATE", 19800);      // days since epoch (e.g. 2024-03-04)
        insert.put("SALARY", 75000.0);
        insert.put(ROW_MARKER_COLUMN, 0);
        rows.add(insert);

        // Row 1: UPDATE
        Map<String, Object> update = new HashMap<>();
        update.put("EMPLOYEE_ID", 102L);
        update.put("FIRST_NAME", "Jane");
        update.put("LAST_NAME", "Doe");
        update.put("HIRE_DATE", 19900);
        update.put("SALARY", 85000.0);
        update.put(ROW_MARKER_COLUMN, 1);
        rows.add(update);

        // Row 2: DELETE
        Map<String, Object> delete = new HashMap<>();
        delete.put("EMPLOYEE_ID", 103L);
        delete.put("FIRST_NAME", null);
        delete.put("LAST_NAME", "Brown");
        delete.put("HIRE_DATE", null);
        delete.put("SALARY", null);
        delete.put(ROW_MARKER_COLUMN, 2);
        rows.add(delete);

        return rows;
    }

    private List<GenericRecord> readParquet(Path path) throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new LocalInputFile(path))
                .withConf(new Configuration())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private void deleteDirectoryIfExists(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
