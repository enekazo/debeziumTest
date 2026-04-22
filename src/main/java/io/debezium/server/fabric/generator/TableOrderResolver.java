package io.debezium.server.fabric.generator;

import io.debezium.server.fabric.metadata.ForeignKeyMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Determines a safe INSERT order for a set of tables using Kahn's topological
 * sort algorithm, so that parent tables are always inserted before child tables
 * that have foreign-key dependencies on them.
 *
 * <p>Tables involved in a cycle are detected and excluded (with a warning).
 * Tables that reference a parent which is NOT in the requested list are still
 * included — the FK value will be seeded from the live database.</p>
 */
public class TableOrderResolver {

    private static final Logger LOG = LoggerFactory.getLogger(TableOrderResolver.class);

    private TableOrderResolver() {}

    /**
     * Returns an ordered copy of {@code tableKeys} where parents precede children.
     *
     * @param tableKeys   list of "SCHEMA.TABLE" strings (upper-case)
     * @param metadataMap map from "SCHEMA.TABLE" → {@link TableMetadata}
     * @return ordered list; tables forming a cycle are omitted and logged as warnings
     */
    public static List<String> resolve(List<String> tableKeys,
                                       Map<String, TableMetadata> metadataMap) {
        // Normalize keys to upper-case
        List<String> keys = new ArrayList<>();
        for (String k : tableKeys) keys.add(k.toUpperCase());

        Set<String> keySet = new HashSet<>(keys);

        // Build in-degree map and adjacency list (parent → set of children)
        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, List<String>> children = new HashMap<>();

        for (String key : keys) {
            inDegree.put(key, 0);
            children.put(key, new ArrayList<>());
        }

        for (String key : keys) {
            TableMetadata meta = metadataMap.get(key);
            if (meta == null) continue;

            for (ForeignKeyMetadata fk : meta.foreignKeys) {
                String parentKey = (fk.referencedSchema + "." + fk.referencedTable).toUpperCase();
                if (!keySet.contains(parentKey)) continue; // parent not in list — skip edge

                // edge: parentKey → key (parent must come first)
                if (!children.containsKey(parentKey)) {
                    children.put(parentKey, new ArrayList<>());
                }
                // Avoid duplicate edges from composite FK constraints
                if (!children.get(parentKey).contains(key)) {
                    children.get(parentKey).add(key);
                    inDegree.merge(key, 1, Integer::sum);
                }
            }
        }

        // Kahn's algorithm
        Queue<String> queue = new ArrayDeque<>();
        for (String key : keys) {
            if (inDegree.getOrDefault(key, 0) == 0) queue.add(key);
        }

        List<String> ordered = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            ordered.add(current);
            for (String child : children.getOrDefault(current, List.of())) {
                int newDeg = inDegree.merge(child, -1, Integer::sum);
                if (newDeg == 0) queue.add(child);
            }
        }

        // Any remaining table with in-degree > 0 is part of a cycle
        if (ordered.size() < keys.size()) {
            Set<String> orderedSet = new HashSet<>(ordered);
            for (String key : keys) {
                if (!orderedSet.contains(key)) {
                    LOG.warn("Table '{}' is part of a foreign-key cycle and will be skipped "
                            + "by the data generator.", key);
                }
            }
        }

        return ordered;
    }
}
