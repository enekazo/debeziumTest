package io.debezium.server.fabric.parquet;

import io.debezium.server.fabric.metadata.ColumnMetadata;
import io.debezium.server.fabric.metadata.TableMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Writes CDC row data (as List of Map<String,Object>) to a Parquet file using Avro schema.
 *
 * Value conversion rules (PostgreSQL data_type values from information_schema):
 * - date columns: Debezium sends int (days since epoch) or Long (millis) → convert to int days
 * - timestamp columns: Debezium sends Long (micros) → divide by 1000 to get millis
 *   If connect precision mode is used, value may already be millis
 * - smallint, integer → int
 * - bigint → long
 * - numeric/decimal (p≤9, scale=0) → int; (p≤18, scale=0) → long; else → double
 * - real/float4 → float
 * - double precision → double
 * - boolean → boolean
 * - bytea → ByteBuffer
 * - String → as-is
 * - null → null (for nullable fields)
 * - __rowMarker__ → int (0=insert, 1=update, 2=delete)
 */
public class ParquetFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFileWriter.class);

    /** Milliseconds per day, used to convert epoch-millis DATE values to days. */
    private static final long MILLIS_PER_DAY = 86400000L;

    /**
     * Threshold to distinguish microsecond timestamps (Debezium default mode) from
     * millisecond timestamps (connect precision mode). Values above this are treated as micros.
     */
    private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;

    private final AvroSchemaBuilder schemaBuilder;

    public ParquetFileWriter() {
        this.schemaBuilder = new AvroSchemaBuilder();
    }

    public void write(TableMetadata tableMetadata, String rowMarkerColumn,
                      List<Map<String, Object>> rows, String compression,
                      Path outputPath) throws Exception {
        if (rows.isEmpty()) {
            return;
        }

        Schema avroSchema = schemaBuilder.buildSchema(tableMetadata, rowMarkerColumn);
        CompressionCodecName codec = parseCodec(compression);

        LocalOutputFile outputFile = new LocalOutputFile(outputPath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(avroSchema)
                .withCompressionCodec(codec)
                .withConf(new Configuration())
                .build()) {

            for (Map<String, Object> row : rows) {
                GenericRecord record = buildRecord(avroSchema, tableMetadata, rowMarkerColumn, row);
                writer.write(record);
            }
        }

        LOG.debug("Wrote {} rows to {}", rows.size(), outputPath);
    }

    private GenericRecord buildRecord(Schema avroSchema, TableMetadata tableMetadata,
                                      String rowMarkerColumn, Map<String, Object> row) {
        GenericRecord record = new GenericData.Record(avroSchema);

        for (ColumnMetadata col : tableMetadata.getColumns()) {
            Object raw = row.get(col.getName());
            Object converted = convertValue(col, raw);
            record.put(col.getName(), converted);
        }

        // row marker value
        Object markerVal = row.get(rowMarkerColumn);
        int marker = 0;
        if (markerVal instanceof Number) {
            marker = ((Number) markerVal).intValue();
        } else if (markerVal instanceof String && !((String) markerVal).isBlank()) {
            try {
                marker = Integer.parseInt(((String) markerVal).trim());
            } catch (NumberFormatException ignored) {
                marker = 0;
            }
        }
        record.put(rowMarkerColumn, marker);

        return record;
    }

    private Object convertValue(ColumnMetadata col, Object raw) {
        if (raw == null) {
            return null;
        }
        String type = col.getDataType().toLowerCase();

        if (type.equals("date")) {
            // Debezium with time.precision.mode=connect sends DATE as int (days since epoch)
            // Debezium default sends as long (millis since epoch)
            if (raw instanceof Integer) {
                return raw;
            }
            if (raw instanceof Long) {
                long millis = (Long) raw;
                // If value is large it's millis, convert to days
                if (millis > MILLIS_PER_DAY) {
                    return (int) (millis / MILLIS_PER_DAY);
                }
                return (int) millis;
            }
            return ((Number) raw).intValue();
        }

        if (type.equals("timestamp with time zone") || type.equals("timestamptz")) {
            return raw.toString();
        }

        if (type.startsWith("timestamp")) {
            // Debezium sends micros for TIMESTAMP, millis for connect mode
            if (raw instanceof Long) {
                long val = (Long) raw;
                // Heuristic: if value > MICROS_THRESHOLD it's micros, divide by 1000
                if (val > MICROS_THRESHOLD) {
                    return val / 1000L;
                }
                return val;
            }
            return ((Number) raw).longValue();
        }

        if (type.equals("smallint") || type.equals("integer") || type.equals("int2") || type.equals("int4")) {
            if (raw instanceof Integer) return raw;
            return ((Number) raw).intValue();
        }

        if (type.equals("bigint") || type.equals("int8")) {
            if (raw instanceof Long) return raw;
            return ((Number) raw).longValue();
        }

        if (type.equals("boolean") || type.equals("bool")) {
            if (raw instanceof Boolean) return raw;
            return Boolean.parseBoolean(raw.toString());
        }

        if (type.equals("numeric") || type.equals("decimal")) {
            return convertNumeric(col, raw);
        }

        if (type.equals("real") || type.equals("float4")) {
            if (raw instanceof Float) return raw;
            return ((Number) raw).floatValue();
        }

        if (type.equals("double precision") || type.equals("float8") || type.equals("float")) {
            if (raw instanceof Double) return raw;
            return ((Number) raw).doubleValue();
        }

        if (type.equals("bytea")) {
            if (raw instanceof byte[]) {
                return ByteBuffer.wrap((byte[]) raw);
            }
            if (raw instanceof ByteBuffer) {
                return raw;
            }
            return ByteBuffer.wrap(raw.toString().getBytes());
        }

        // Default: string (covers character varying, text, uuid, json, jsonb, etc.)
        return raw.toString();
    }

    private Object convertNumeric(ColumnMetadata col, Object raw) {
        int precision = col.getPrecision();
        int scale = col.getScale();

        if (precision == 0 || scale > 0) {
            if (raw instanceof Double) return raw;
            return ((Number) raw).doubleValue();
        }
        if (precision <= 9) {
            if (raw instanceof Integer) return raw;
            return ((Number) raw).intValue();
        }
        if (precision <= 18) {
            if (raw instanceof Long) return raw;
            return ((Number) raw).longValue();
        }
        if (raw instanceof Double) return raw;
        return ((Number) raw).doubleValue();
    }

    private CompressionCodecName parseCodec(String compression) {
        if (compression == null) return CompressionCodecName.SNAPPY;
        return switch (compression.toUpperCase()) {
            case "GZIP" -> CompressionCodecName.GZIP;
            case "LZ4" -> CompressionCodecName.LZ4;
            case "ZSTD" -> CompressionCodecName.ZSTD;
            case "UNCOMPRESSED" -> CompressionCodecName.UNCOMPRESSED;
            default -> CompressionCodecName.SNAPPY;
        };
    }
}
