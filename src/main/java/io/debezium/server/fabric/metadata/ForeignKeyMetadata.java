package io.debezium.server.fabric.metadata;

/**
 * Describes a single foreign-key column relationship discovered from
 * ALL_CONSTRAINTS (type='R') in Oracle.
 */
public class ForeignKeyMetadata {

    public final String columnName;
    public final String referencedSchema;
    public final String referencedTable;
    public final String referencedColumn;

    public ForeignKeyMetadata(String columnName,
                              String referencedSchema,
                              String referencedTable,
                              String referencedColumn) {
        this.columnName = columnName;
        this.referencedSchema = referencedSchema;
        this.referencedTable = referencedTable;
        this.referencedColumn = referencedColumn;
    }

    public String getColumnName()        { return columnName; }
    public String getReferencedSchema()  { return referencedSchema; }
    public String getReferencedTable()   { return referencedTable; }
    public String getReferencedColumn()  { return referencedColumn; }

    @Override
    public String toString() {
        return "ForeignKeyMetadata{column='" + columnName +
                "', references=" + referencedSchema + "." + referencedTable +
                "(" + referencedColumn + ")}";
    }
}
