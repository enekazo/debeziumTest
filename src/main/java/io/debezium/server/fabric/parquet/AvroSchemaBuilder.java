package io.debezium.server.fabric.parquet;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds an Avro Schema from Oracle TableMetadata for use with Parquet file writing.
 *
 * Oracle type → Avro logical type mapping:
 * - DATE                                 → int (logicalType=date), Parquet INT32 DATE
 * - TIMESTAMP, TIMESTAMP(n)              → long (logicalType=timestamp-millis)
 * - TIMESTAMP WITH TIME ZONE             → string
 * - TIMESTAMP WITH LOCAL TIME ZONE       → string
 * - NUMBER (no precision)                → double
 * - NUMBER(p,0) p<=9                     → int
 * - NUMBER(p,0) p<=18                    → long
 * - NUMBER(p,s) s>0                      → double
 * - FLOAT, BINARY_FLOAT                  → float
 * - BINARY_DOUBLE                        → double
 * - VARCHAR2, CHAR, NVARCHAR2, NCHAR, VARCHAR → string
 * - CLOB, NCLOB, LONG                    → string
 * - RAW, BLOB, LONG RAW                  → bytes
 * - Nullable columns wrapped in ["null", <type>] union with default null
 * - Non-nullable INT field "__rowMarker__" appended at end
 */
public class AvroSchemaBuilder {

    public Schema buildSchema(TableMetadata tableMetadata, String rowMarkerColumn) {
        String recordName = tableMetadata.getSchema() + "_" + tableMetadata.getTableName();
        String namespace = "io.debezium.server.fabric";

        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnMetadata col : tableMetadata.getColumns()) {
            Schema fieldSchema = oracleTypeToAvroSchema(col);
            // All data columns are nullable in the Parquet schema regardless of Oracle nullability.
            // CDC events (UPDATE via LogMiner, DELETE with only PK) routinely omit non-PK columns,
            // which would cause "Null-value for required field" errors at flush time.
            Schema nullableSchema = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    fieldSchema);
            Schema.Field field = new Schema.Field(col.getName(), nullableSchema, null, Schema.Field.NULL_DEFAULT_VALUE);
            fields.add(field);
        }

        // Append __rowMarker__ as non-nullable INT
        fields.add(new Schema.Field(rowMarkerColumn, Schema.create(Schema.Type.INT), null, (Object) null));

        return Schema.createRecord(recordName, null, namespace, false, fields);
    }

    /**
     * Builds a schema for a list of explicitly named columns (e.g. from test data).
     */
    public Schema buildSchemaFromColumns(String schemaName, String tableName,
                                         List<ColumnMetadata> columns, String rowMarkerColumn) {
        TableMetadata meta = new TableMetadata(schemaName, tableName, columns, List.of());
        return buildSchema(meta, rowMarkerColumn);
    }

    private Schema oracleTypeToAvroSchema(ColumnMetadata col) {
        String type = col.getOracleType().toUpperCase();

        // DATE → Avro int with logicalType=date (days since epoch)
        if (type.equals("DATE")) {
            return LogicalTypes.DATE;
        }

        // TIMESTAMP variants
        if (type.startsWith("TIMESTAMP WITH TIME ZONE") || type.startsWith("TIMESTAMP WITH LOCAL TIME ZONE")) {
            return Schema.create(Schema.Type.STRING);
        }
        if (type.startsWith("TIMESTAMP")) {
            return LogicalTypes.TIMESTAMP_MILLIS;
        }

        // NUMBER
        if (type.equals("NUMBER")) {
            int precision = col.getPrecision();
            int scale = col.getScale();
            if (precision == 0) {
                // No precision specified → double
                return Schema.create(Schema.Type.DOUBLE);
            }
            if (scale > 0) {
                return Schema.create(Schema.Type.DOUBLE);
            }
            if (precision <= 9) {
                return Schema.create(Schema.Type.INT);
            }
            if (precision <= 18) {
                return Schema.create(Schema.Type.LONG);
            }
            return Schema.create(Schema.Type.DOUBLE);
        }

        // FLOAT types
        if (type.equals("FLOAT") || type.equals("BINARY_FLOAT")) {
            return Schema.create(Schema.Type.FLOAT);
        }
        if (type.equals("BINARY_DOUBLE")) {
            return Schema.create(Schema.Type.DOUBLE);
        }

        // String types
        if (type.equals("VARCHAR2") || type.equals("CHAR") || type.equals("NVARCHAR2") ||
                type.equals("NCHAR") || type.equals("VARCHAR")) {
            return Schema.create(Schema.Type.STRING);
        }
        if (type.equals("CLOB") || type.equals("NCLOB") || type.equals("LONG")) {
            return Schema.create(Schema.Type.STRING);
        }

        // Binary types
        if (type.equals("RAW") || type.startsWith("RAW(") || type.equals("BLOB") || type.equals("LONG RAW")) {
            return Schema.create(Schema.Type.BYTES);
        }

        // Default fallback to string
        return Schema.create(Schema.Type.STRING);
    }

    /** Avro logical type schemas (reusable instances). */
    private static final class LogicalTypes {
        static final Schema DATE;
        static final Schema TIMESTAMP_MILLIS;

        static {
            DATE = Schema.create(Schema.Type.INT);
            DATE.addProp("logicalType", "date");

            TIMESTAMP_MILLIS = Schema.create(Schema.Type.LONG);
            TIMESTAMP_MILLIS.addProp("logicalType", "timestamp-millis");
        }
    }
}
