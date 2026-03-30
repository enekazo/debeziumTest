package io.debezium.server.fabric.storage;

import java.nio.file.Path;
import java.util.List;

/**
 * Abstraction over storage backends for landing zone files.
 * Supports local filesystem (for dev/test) and ADLS Gen2/OneLake.
 */
public interface StorageBackend {

    /**
     * Upload a local temporary file to permanent storage.
     *
     * @param tableFolder  folder name, e.g. "HR_EMPLOYEES"
     * @param filename     target filename, e.g. "00000000000000000001.parquet"
     * @param localFile    path to the local temp file to upload
     */
    void uploadFile(String tableFolder, String filename, Path localFile) throws Exception;

    /**
     * Read a small text file from storage (e.g. _sequence.txt, _metadata.json).
     * Returns null if the file does not exist.
     */
    String readTextFile(String tableFolder, String filename) throws Exception;

    /**
     * Write a small text file to storage (e.g. _sequence.txt, _metadata.json).
     */
    void writeTextFile(String tableFolder, String filename, String content) throws Exception;

    /**
     * Check whether a file exists in storage.
     */
    boolean exists(String tableFolder, String filename) throws Exception;

    /**
     * List all files in a table folder (used for sequence scanning on startup).
     */
    List<String> listFiles(String tableFolder) throws Exception;
}
