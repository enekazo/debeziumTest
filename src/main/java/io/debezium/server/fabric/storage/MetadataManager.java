package io.debezium.server.fabric.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages _metadata.json files per table folder in the landing zone.
 *
 * Microsoft Fabric Open Mirroring reads this file to understand the table structure.
 * Format (case-sensitive — Fabric requires lowercase keys):
 * {
 *   "keyColumns": ["col1", "col2"]
 * }
 *
 * The file is created/overwritten on sink startup for each table.
 */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);
    private static final String METADATA_FILENAME = "_metadata.json";

    private final StorageBackend storage;
    private final ObjectMapper objectMapper;

    public MetadataManager(StorageBackend storage) {
        this.storage = storage;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Ensures the _metadata.json exists and is up-to-date for the given table.
     * Always overwrites to keep in sync with current PK definition.
     */
    public void ensureMetadata(String tableFolder, TableMetadata metadata) throws Exception {
        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode keyColumns = root.putArray("keyColumns");  // lowercase — Fabric spec is case-sensitive
        for (String pk : metadata.getPkColumns()) {
            keyColumns.add(pk);
        }
        String json = objectMapper.writeValueAsString(root);
        storage.writeTextFile(tableFolder, METADATA_FILENAME, json);
        LOG.info("Wrote {} for table {}: {}", METADATA_FILENAME, tableFolder, json);
    }
}
