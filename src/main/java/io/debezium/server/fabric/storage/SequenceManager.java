package io.debezium.server.fabric.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages per-table sequence numbers for naming Parquet files.
 *
 * Files are named as 20-digit zero-padded integers: e.g. 00000000000000000001.parquet
 * State is persisted in _sequence.txt inside each table's folder.
 *
 * On startup, the sequence is initialized by:
 *   1. Reading _sequence.txt if it exists (fast path)
 *   2. Scanning for .parquet files and finding the max sequence number (fallback)
 *
 * nextSequence() increments the in-memory counter atomically and returns the new value.
 * commitSequence() persists the sequence to storage after a successful write.
 */
public class SequenceManager {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceManager.class);
    private static final int FILENAME_DIGITS = 20;

    private final StorageBackend storage;
    private final String stateFileName;
    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public SequenceManager(StorageBackend storage, String stateFileName) {
        this.storage = storage;
        this.stateFileName = stateFileName;
    }

    /**
     * Initializes the sequence counter for the given table folder.
     * Should be called once per table on startup.
     *
     * @return the current (last committed) sequence number, or 0 if the table is new
     */
    public long initTable(String tableFolder) throws Exception {
        long seq = 0;

        // Fast path: read _sequence.txt
        String content = storage.readTextFile(tableFolder, stateFileName);
        if (content != null) {
            try {
                seq = Long.parseLong(content.trim());
                LOG.info("Table {}: restored sequence from {}: {}", tableFolder, stateFileName, seq);
            } catch (NumberFormatException e) {
                LOG.warn("Table {}: invalid sequence file content '{}', scanning files", tableFolder, content);
                seq = scanMaxSequence(tableFolder);
            }
        } else {
            // Fallback: scan existing parquet files for max sequence
            seq = scanMaxSequence(tableFolder);
            if (seq > 0) {
                LOG.info("Table {}: discovered max sequence {} from file scan", tableFolder, seq);
            }
        }

        sequences.put(tableFolder, new AtomicLong(seq));
        return seq;
    }

    /**
     * Atomically increments and returns the next sequence number.
     * initTable() must have been called first.
     */
    public long nextSequence(String tableFolder) {
        AtomicLong counter = sequences.computeIfAbsent(tableFolder, k -> new AtomicLong(0L));
        return counter.incrementAndGet();
    }

    /**
     * Persists the given sequence number to storage.
     * Should be called after a successful file upload.
     */
    public void commitSequence(String tableFolder, long seq) throws Exception {
        storage.writeTextFile(tableFolder, stateFileName, String.valueOf(seq));
        LOG.debug("Table {}: committed sequence {}", tableFolder, seq);
    }

    /**
     * Rolls back the in-memory sequence if a flush failed after reserving a number.
     * This prevents gaps like ...0005, ...0007 when ...0006 upload fails.
     */
    public void rollbackSequence(String tableFolder, long failedSeq) {
        AtomicLong counter = sequences.get(tableFolder);
        if (counter == null) return;

        counter.updateAndGet(current -> {
            if (current == failedSeq && failedSeq > 0) {
                return failedSeq - 1;
            }
            return current;
        });
        LOG.debug("Table {}: rolled back failed reserved sequence {}", tableFolder, failedSeq);
    }

    /**
     * Formats a sequence number as a 20-digit zero-padded Parquet filename.
     */
    public static String toFilename(long seq) {
        return String.format("%0" + FILENAME_DIGITS + "d.parquet", seq);
    }

    private long scanMaxSequence(String tableFolder) throws Exception {
        List<String> files = storage.listFiles(tableFolder);
        long max = 0;
        for (String file : files) {
            if (!file.endsWith(".parquet")) continue;
            String base = file.substring(0, file.length() - ".parquet".length());
            try {
                long seq = Long.parseLong(base);
                if (seq > max) max = seq;
            } catch (NumberFormatException ignored) {
                // Not a sequence-named file, skip
            }
        }
        return max;
    }
}
