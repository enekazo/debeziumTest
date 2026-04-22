package io.debezium.server.fabric.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of primary-key values for parent tables so that FK columns
 * in child tables can reference valid existing rows.
 *
 * <p>Keys are stored as {@code "SCHEMA.TABLE.COLUMN"} strings.
 *
 * <p>If a pool is empty when a FK value is needed, the pool is seeded from the
 * live database by sampling up to 100 existing PK values.</p>
 */
public class ParentKeyPool {

    private static final Logger LOG = LoggerFactory.getLogger(ParentKeyPool.class);
    private static final int SEED_LIMIT = 100;

    private final Map<String, List<Object>> pool = new ConcurrentHashMap<>();
    private final Random random = new Random();

    /** Appends newly-inserted PK values to the pool for a column. */
    public void addKeys(String schema, String table, String column, List<Object> keys) {
        if (keys == null || keys.isEmpty()) return;
        String poolKey = poolKey(schema, table, column);
        pool.computeIfAbsent(poolKey, k -> new ArrayList<>()).addAll(keys);
    }

    /**
     * Returns a random value from the pool, seeding from the database if needed.
     *
     * @return a random PK value, or {@code null} if the pool cannot be seeded
     */
    public Object randomKey(String schema, String table, String column, Connection conn) {
        String poolKey = poolKey(schema, table, column);
        List<Object> values = pool.get(poolKey);
        if (values == null || values.isEmpty()) {
            values = seedFromDatabase(conn, schema, table, column);
            if (values == null || values.isEmpty()) {
                LOG.warn("Parent key pool is empty for {}.{}.{} and DB seed returned nothing",
                        schema, table, column);
                return null;
            }
            pool.put(poolKey, values);
        }
        return values.get(random.nextInt(values.size()));
    }

    /** Returns true if the pool already has values for the given column. */
    public boolean hasKeys(String schema, String table, String column) {
        List<Object> values = pool.get(poolKey(schema, table, column));
        return values != null && !values.isEmpty();
    }

    /** Clears all cached keys (e.g., between test runs). */
    public void clear() {
        pool.clear();
    }

    // ── private helpers ──────────────────────────────────────────────────────

    private List<Object> seedFromDatabase(Connection conn, String schema, String table, String column) {
        if (conn == null) return Collections.emptyList();
        String sql = "SELECT " + column + " FROM " + schema + "." + table +
                     " ORDER BY DBMS_RANDOM.VALUE FETCH FIRST " + SEED_LIMIT + " ROWS ONLY";
        List<Object> values = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Object v = rs.getObject(1);
                if (v != null) values.add(v);
            }
            LOG.debug("Seeded {} values for parent pool {}.{}.{}", values.size(), schema, table, column);
        } catch (Exception e) {
            LOG.warn("Failed to seed parent pool for {}.{}.{}: {}", schema, table, column, e.getMessage());
        }
        return values;
    }

    private static String poolKey(String schema, String table, String column) {
        return schema.toUpperCase() + "." + table.toUpperCase() + "." + column.toUpperCase();
    }
}
