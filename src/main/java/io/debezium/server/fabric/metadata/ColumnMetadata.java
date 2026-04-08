package io.debezium.server.fabric.metadata;

public class ColumnMetadata {

    public final String name;
    public final String dataType;
    public final int precision;
    public final int scale;
    public final boolean nullable;
    public final int columnId;

    public ColumnMetadata(String name, String dataType, int precision, int scale, boolean nullable, int columnId) {
        this.name = name;
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.columnId = columnId;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public int getColumnId() {
        return columnId;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{name='" + name + "', dataType='" + dataType +
                "', precision=" + precision + ", scale=" + scale +
                ", nullable=" + nullable + ", columnId=" + columnId + "}";
    }
}
