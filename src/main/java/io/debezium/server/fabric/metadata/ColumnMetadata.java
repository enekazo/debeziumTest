package io.debezium.server.fabric.metadata;

public class ColumnMetadata {

    public final String name;
    public final String oracleType;
    public final int precision;
    public final int scale;
    public final boolean nullable;
    public final int columnId;

    public ColumnMetadata(String name, String oracleType, int precision, int scale, boolean nullable, int columnId) {
        this.name = name;
        this.oracleType = oracleType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.columnId = columnId;
    }

    public String getName() {
        return name;
    }

    public String getOracleType() {
        return oracleType;
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
        return "ColumnMetadata{name='" + name + "', oracleType='" + oracleType +
                "', precision=" + precision + ", scale=" + scale +
                ", nullable=" + nullable + ", columnId=" + columnId + "}";
    }
}
