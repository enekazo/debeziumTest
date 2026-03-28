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
 * Value conversion rules:
 * - DATE columns: Debezium sends int (days since epoch) or Long (millis) → convert to int days
 *   For Long: divide by 86400000L
 * - TIMESTAMP columns: Debezium sends Long (micros from Debezium connector) → divide by 1000 to get millis
 *   If connect precision mode is used, value may already be millis
 * - NUMBER(p,0) p<=9 → int
 * - NUMBER(p,0) p<=18 → long
 * - NUMBER no prec or scale>0 → double
 * - FLOAT/BINARY_FLOAT → float
 * - BINARY_DOUBLE → double
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

        // rowMarker value
        Object markerVal = row.get(rowMarkerColumn);
        int marker = 0;
        if (markerVal instanceof Number) {
            marker = ((Number) markerVal).intValue();
        }
        record.put(rowMarkerColumn, marker);

        return record;
    }

    private Object convertValue(ColumnMetadata col, Object raw) {
        if (raw == null) {
            return null;
        }
        String type = col.getOracleType().toUpperCase();

        if (type.equals("DATE")) {
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

        if (type.startsWith("TIMESTAMP WITH TIME ZONE") || type.startsWith("TIMESTAMP WITH LOCAL TIME ZONE")) {
            return raw.toString();
        }

        if (type.startsWith("TIMESTAMP")) {
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

        if (type.equals("NUMBER")) {
            return convertNumber(col, raw);
        }

        if (type.equals("FLOAT") || type.equals("BINARY_FLOAT")) {
            if (raw instanceof Float) return raw;
            return ((Number) raw).floatValue();
        }

        if (type.equals("BINARY_DOUBLE")) {
            if (raw instanceof Double) return raw;
            return ((Number) raw).doubleValue();
        }

        if (type.equals("RAW") || type.startsWith("RAW(") || type.equals("BLOB") || type.equals("LONG RAW")) {
            if (raw instanceof byte[]) {
                return ByteBuffer.wrap((byte[]) raw);
            }
            if (raw instanceof ByteBuffer) {
                return raw;
            }
            return ByteBuffer.wrap(raw.toString().getBytes());
        }

        // Default: string
        return raw.toString();
    }

    private Object convertNumber(ColumnMetadata col, Object raw) {
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
