package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.ForeignKeyMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Generates a single random row for a given table, respecting column types,
 * nullable flags, primary-key counters, foreign-key pools, and unique constraints.
 *
 * <p>One instance should be created per generation run; it maintains internal
 * state for unique-value tracking.</p>
 */
public class RandomDataGenerator {

    private static final String ALPHANUM =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String[] LOREM_WORDS =
            {"lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
             "adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
             "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua"};

    // Range for random dates: 2000-01-01 to 2024-12-31
    private static final LocalDate DATE_MIN = LocalDate.of(2000, 1, 1);
    private static final LocalDate DATE_MAX = LocalDate.of(2024, 12, 31);
    private static final long DATE_RANGE_DAYS = ChronoUnit.DAYS.between(DATE_MIN, DATE_MAX);

    private final Random random;
    private final double nullProbability;

    /**
     * Tracks generated string values per "SCHEMA.TABLE.COLUMN" key to avoid
     * duplicates for unique-constrained columns.
     */
    private final Map<String, Set<Object>> generatedUniques = new HashMap<>();

    public RandomDataGenerator(double nullProbability) {
        this.random = new Random();
        this.nullProbability = nullProbability;
    }

    /** Constructor for tests with a fixed seed for deterministic output. */
    RandomDataGenerator(double nullProbability, long seed) {
        this.random = new Random(seed);
        this.nullProbability = nullProbability;
    }

    /**
     * Generates one random row (column name → value) for the given table.
     *
     * @param schema     Oracle schema name (upper-case)
     * @param table      Oracle table name (upper-case)
     * @param tableMeta  full table metadata (columns, PK, FK, unique)
     * @param pool       parent-key pool for FK resolution
     * @param counters   monotonic counters keyed by "SCHEMA.TABLE.COLUMN", updated in-place
     * @param conn       live JDBC connection (used by pool seeding; may be null in tests)
     * @return map of column name → generated value
     */
    public Map<String, Object> generateRow(String schema, String table, TableMetadata tableMeta,
                                           ParentKeyPool pool, Map<String, Long> counters,
                                           Connection conn) {
        // Build a quick FK lookup by column name
        Map<String, ForeignKeyMetadata> fkByCol = new LinkedHashMap<>();
        for (ForeignKeyMetadata fk : tableMeta.foreignKeys) {
            fkByCol.put(fk.columnName.toUpperCase(), fk);
        }

        Set<String> pkSet = Set.copyOf(tableMeta.pkColumns);
        Set<String> uniqueSet = Set.copyOf(tableMeta.uniqueColumns);

        Map<String, Object> row = new LinkedHashMap<>();
        for (ColumnMetadata col : tableMeta.columns) {
            String colUpper = col.name.toUpperCase();
            Object value;

            // 1. FK column → resolve from parent pool
            ForeignKeyMetadata fk = fkByCol.get(colUpper);
            if (fk != null) {
                value = pool.randomKey(fk.referencedSchema, fk.referencedTable,
                                       fk.referencedColumn, conn);
                // If nullable and pool returned null, allow null; otherwise try again or use null
                row.put(col.name, value);
                continue;
            }

            // 2. Nullable → sometimes emit null
            if (col.nullable && random.nextDouble() < nullProbability) {
                row.put(col.name, null);
                continue;
            }

            // 3. PK or unique column → use auto-increment counter
            if (pkSet.contains(colUpper) || uniqueSet.contains(colUpper)) {
                String counterKey = schema + "." + table + "." + colUpper;
                long next = counters.getOrDefault(counterKey, 0L) + 1L;
                counters.put(counterKey, next);
                value = toColumnValue(col, next);
                row.put(col.name, value);
                continue;
            }

            // 4. Generate by Oracle type
            value = generateByType(col);
            row.put(col.name, value);
        }
        return row;
    }

    // ── type dispatch ────────────────────────────────────────────────────────

    Object generateByType(ColumnMetadata col) {
        String type = col.oracleType.toUpperCase();

        if (type.startsWith("NUMBER") || type.equals("INTEGER") || type.equals("INT")
                || type.equals("SMALLINT") || type.equals("DECIMAL")) {
            return generateNumber(col);
        }
        if (type.equals("FLOAT") || type.startsWith("FLOAT(")) {
            return (double) (random.nextFloat() * 1_000_000);
        }
        if (type.equals("BINARY_FLOAT")) {
            return (double) random.nextFloat() * 1_000_000;
        }
        if (type.equals("BINARY_DOUBLE")) {
            return random.nextDouble() * 1_000_000;
        }
        if (type.startsWith("VARCHAR2") || type.startsWith("NVARCHAR2")) {
            int maxLen = col.precision > 0 ? col.precision : 50;
            return randomString(Math.max(1, 1 + random.nextInt(Math.min(maxLen, 30))));
        }
        if (type.startsWith("CHAR") || type.startsWith("NCHAR")) {
            int len = col.precision > 0 ? col.precision : 1;
            return randomString(len);
        }
        if (type.equals("CLOB") || type.equals("NCLOB") || type.equals("LONG")) {
            return randomLoremSentence();
        }
        if (type.equals("DATE")) {
            LocalDate d = DATE_MIN.plusDays(random.nextInt((int) DATE_RANGE_DAYS + 1));
            return Date.valueOf(d);
        }
        if (type.startsWith("TIMESTAMP")) {
            LocalDate d = DATE_MIN.plusDays(random.nextInt((int) DATE_RANGE_DAYS + 1));
            LocalDateTime dt = d.atTime(random.nextInt(24), random.nextInt(60), random.nextInt(60));
            return Timestamp.valueOf(dt);
        }
        if (type.equals("BLOB") || type.startsWith("RAW")) {
            int len = col.precision > 0 ? Math.min(col.precision, 16) : 8;
            byte[] bytes = new byte[len];
            random.nextBytes(bytes);
            return bytes;
        }
        // Fallback: treat as varchar
        return randomString(10);
    }

    // ── private helpers ──────────────────────────────────────────────────────

    private Object generateNumber(ColumnMetadata col) {
        int precision = col.precision > 0 ? col.precision : 10;
        int scale = col.scale;

        if (scale > 0) {
            // Decimal — generate a double within a reasonable range
            double max = Math.pow(10, Math.min(precision - scale, 10));
            double raw = random.nextDouble() * max;
            return BigDecimal.valueOf(raw).setScale(scale, RoundingMode.HALF_UP).doubleValue();
        }
        // Integer — determine range from precision
        long max = (long) Math.min(Math.pow(10, precision) - 1, Long.MAX_VALUE);
        long value = (long) (random.nextDouble() * max);
        if (precision <= 9) return (int) value;
        return value;
    }

    /**
     * Converts a monotonically-increasing counter to the appropriate Java type
     * for the column (respects number precision/scale for PK-style counters).
     */
    private Object toColumnValue(ColumnMetadata col, long counter) {
        String type = col.oracleType.toUpperCase();
        if (type.startsWith("NUMBER") || type.equals("INTEGER") || type.equals("INT")) {
            int precision = col.precision > 0 ? col.precision : 10;
            if (precision <= 9) return (int) counter;
            return counter;
        }
        if (type.startsWith("VARCHAR2") || type.startsWith("NVARCHAR2")
                || type.startsWith("CHAR") || type.startsWith("NCHAR")) {
            return String.valueOf(counter);
        }
        return counter;
    }

    String randomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHANUM.charAt(random.nextInt(ALPHANUM.length())));
        }
        return sb.toString();
    }

    private String randomLoremSentence() {
        int wordCount = 5 + random.nextInt(11); // 5–15 words
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) sb.append(' ');
            sb.append(LOREM_WORDS[random.nextInt(LOREM_WORDS.length)]);
        }
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        sb.append('.');
        return sb.toString();
    }
}
