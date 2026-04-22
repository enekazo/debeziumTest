package io.debezium.server.fabric.metadata;

import java.util.Collections;
import java.util.List;

public class TableMetadata {

    public final String schema;
    public final String tableName;
    public final List<ColumnMetadata> columns;
    public final List<String> pkColumns;
    /** Foreign-key relationships for columns in this table. Empty if none. */
    public final List<ForeignKeyMetadata> foreignKeys;
    /** Column names that participate in a UNIQUE constraint (beyond the PK). */
    public final List<String> uniqueColumns;

    /** Full constructor. */
    public TableMetadata(String schema, String tableName,
                         List<ColumnMetadata> columns, List<String> pkColumns,
                         List<ForeignKeyMetadata> foreignKeys, List<String> uniqueColumns) {
        this.schema = schema;
        this.tableName = tableName;
        this.columns = Collections.unmodifiableList(columns);
        this.pkColumns = Collections.unmodifiableList(pkColumns);
        this.foreignKeys = Collections.unmodifiableList(foreignKeys);
        this.uniqueColumns = Collections.unmodifiableList(uniqueColumns);
    }

    /** Backward-compatible constructor — no FK or unique data. */
    public TableMetadata(String schema, String tableName, List<ColumnMetadata> columns, List<String> pkColumns) {
        this(schema, tableName, columns, pkColumns, Collections.emptyList(), Collections.emptyList());
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public List<String> getPkColumns() {
        return pkColumns;
    }

    public List<ForeignKeyMetadata> getForeignKeys() {
        return foreignKeys;
    }

    public List<String> getUniqueColumns() {
        return uniqueColumns;
    }

    @Override
    public String toString() {
        return "TableMetadata{schema='" + schema + "', tableName='" + tableName +
                "', columns=" + columns.size() + ", pkColumns=" + pkColumns +
                ", foreignKeys=" + foreignKeys.size() + ", uniqueColumns=" + uniqueColumns + "}";
    }
}
