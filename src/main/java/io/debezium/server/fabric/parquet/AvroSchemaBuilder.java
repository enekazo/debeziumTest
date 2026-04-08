package io.debezium.server.fabric.parquet;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds an Avro Schema from PostgreSQL {@link TableMetadata} for use with Parquet file writing.
 *
 * PostgreSQL data_type → Avro logical type mapping:
 * - date                                      → int  (logicalType=date), Parquet INT32 DATE
 * - timestamp without time zone               → long (logicalType=timestamp-millis)
 * - timestamp with time zone                  → string
 * - smallint, integer                         → int
 * - bigint                                    → long
 * - boolean                                   → boolean
 * - real                                      → float
 * - double precision                          → double
 * - numeric / decimal (scale>0)               → double
 * - numeric / decimal (scale=0, precision≤9)  → int
 * - numeric / decimal (scale=0, precision≤18) → long
 * - numeric / decimal (no precision)          → double
 * - character varying, text, char, uuid, json, jsonb → string
 * - bytea                                     → bytes
 * - Nullable columns wrapped in ["null", <type>] union with default null
 * - Non-nullable INT field "__rowMarker__" appended at end
 */
public class AvroSchemaBuilder {

    public Schema buildSchema(TableMetadata tableMetadata, String rowMarkerColumn) {
        String recordName = tableMetadata.getSchema() + "_" + tableMetadata.getTableName();
        String namespace = "io.debezium.server.fabric";

        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnMetadata col : tableMetadata.getColumns()) {
            Schema fieldSchema = dataTypeToAvroSchema(col);
            // All data columns are nullable in the Parquet schema regardless of source nullability.
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

    private Schema dataTypeToAvroSchema(ColumnMetadata col) {
        String type = col.getDataType().toLowerCase();

        // date → Avro int with logicalType=date (days since epoch)
        if (type.equals("date")) {
            return LogicalTypes.DATE;
        }

        // timestamp variants
        if (type.equals("timestamp with time zone") || type.equals("timestamptz")) {
            return Schema.create(Schema.Type.STRING);
        }
        if (type.startsWith("timestamp")) {
            return LogicalTypes.TIMESTAMP_MILLIS;
        }

        // integer types
        if (type.equals("smallint") || type.equals("integer") || type.equals("int2") || type.equals("int4")) {
            return Schema.create(Schema.Type.INT);
        }
        if (type.equals("bigint") || type.equals("int8")) {
            return Schema.create(Schema.Type.LONG);
        }

        // boolean
        if (type.equals("boolean") || type.equals("bool")) {
            return Schema.create(Schema.Type.BOOLEAN);
        }

        // floating point
        if (type.equals("real") || type.equals("float4")) {
            return Schema.create(Schema.Type.FLOAT);
        }
        if (type.equals("double precision") || type.equals("float8") || type.equals("float")) {
            return Schema.create(Schema.Type.DOUBLE);
        }

        // numeric / decimal
        if (type.equals("numeric") || type.equals("decimal")) {
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

        // binary
        if (type.equals("bytea")) {
            return Schema.create(Schema.Type.BYTES);
        }

        // Default: string (covers character varying, text, char, uuid, json, jsonb, etc.)
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
