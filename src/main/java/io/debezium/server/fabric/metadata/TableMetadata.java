package io.debezium.server.fabric.metadata;

import java.util.Collections;
import java.util.List;

public class TableMetadata {

    public final String schema;
    public final String tableName;
    public final List<ColumnMetadata> columns;
    public final List<String> pkColumns;

    public TableMetadata(String schema, String tableName, List<ColumnMetadata> columns, List<String> pkColumns) {
        this.schema = schema;
        this.tableName = tableName;
        this.columns = Collections.unmodifiableList(columns);
        this.pkColumns = Collections.unmodifiableList(pkColumns);
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

    @Override
    public String toString() {
        return "TableMetadata{schema='" + schema + "', tableName='" + tableName +
                "', columns=" + columns.size() + ", pkColumns=" + pkColumns + "}";
    }
}
