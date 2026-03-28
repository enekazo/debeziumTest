package io.debezium.server.fabric.storage;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Azure Data Lake Storage Gen2 implementation of StorageBackend.
 * Connects to Microsoft Fabric OneLake via ABFS URI and SAS token authentication.
 *
 * baseUri format: abfss://<filesystem>@<account>.dfs.fabric.microsoft.com/<basePath>
 * Example:        abfss://mycontainer@onelake.dfs.fabric.microsoft.com/workspace/db/landing
 */
public class AbfsStorageBackend implements StorageBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AbfsStorageBackend.class);
    private static final int UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

    private final DataLakeFileSystemClient fileSystemClient;
    private final String basePath;

    public AbfsStorageBackend(String baseUri, String sasToken) {
        ParsedUri parsed = parseAbfsUri(baseUri);

        String serviceUrl = "https://" + parsed.accountHost;

        DataLakeServiceClient serviceClient = new DataLakeServiceClientBuilder()
                .endpoint(serviceUrl)
                .credential(new AzureSasCredential(sasToken))
                .buildClient();

        this.fileSystemClient = serviceClient.getFileSystemClient(parsed.filesystem);
        this.basePath = parsed.path.isEmpty() ? "" : (parsed.path.endsWith("/") ? parsed.path : parsed.path + "/");

        LOG.info("AbfsStorageBackend initialized: filesystem={}, basePath={}", parsed.filesystem, this.basePath);
    }

    @Override
    public void uploadFile(String tableFolder, String filename, Path localFile) throws Exception {
        String tmpPath = basePath + tableFolder + "/" + filename + ".tmp";
        String finalPath = basePath + tableFolder + "/" + filename;

        byte[] data = Files.readAllBytes(localFile);

        // Create the file in storage (overwrite if exists)
        DataLakeFileClient tmpFileClient = fileSystemClient.createFile(tmpPath, true);

        // Upload in chunks
        try (InputStream is = new ByteArrayInputStream(data)) {
            long offset = 0;
            byte[] buffer = new byte[UPLOAD_CHUNK_SIZE];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                byte[] chunk = bytesRead == buffer.length ? buffer : java.util.Arrays.copyOf(buffer, bytesRead);
                tmpFileClient.append(new ByteArrayInputStream(chunk), offset, bytesRead);
                offset += bytesRead;
            }
            tmpFileClient.flush(offset, true);
        }

        // Atomic rename: .tmp → final
        String newName = basePath + tableFolder + "/" + filename;
        tmpFileClient.rename(null, newName);

        LOG.debug("Uploaded {} bytes to {}", data.length, finalPath);
    }

    @Override
    public String readTextFile(String tableFolder, String filename) throws Exception {
        String filePath = basePath + tableFolder + "/" + filename;
        try {
            DataLakeFileClient fileClient = fileSystemClient.getFileClient(filePath);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            fileClient.read(baos);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            if (isNotFoundError(e)) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public void writeTextFile(String tableFolder, String filename, String content) throws Exception {
        String filePath = basePath + tableFolder + "/" + filename;
        byte[] data = content.getBytes(StandardCharsets.UTF_8);

        // Ensure directory exists
        String dirPath = basePath + tableFolder;
        try {
            fileSystemClient.createDirectoryIfNotExists(dirPath);
        } catch (Exception e) {
            LOG.debug("Directory creation skipped (may already exist): {}", dirPath);
        }

        DataLakeFileClient fileClient = fileSystemClient.createFile(filePath, true);
        fileClient.append(new ByteArrayInputStream(data), 0, data.length);
        fileClient.flush(data.length, true);
    }

    @Override
    public boolean exists(String tableFolder, String filename) throws Exception {
        String filePath = basePath + tableFolder + "/" + filename;
        try {
            DataLakeFileClient fileClient = fileSystemClient.getFileClient(filePath);
            return fileClient.exists();
        } catch (Exception e) {
            if (isNotFoundError(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public List<String> listFiles(String tableFolder) throws Exception {
        String dirPath = basePath + tableFolder;
        List<String> files = new ArrayList<>();
        try {
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(dirPath);
            options.setRecursive(false);
            fileSystemClient.listPaths(options, null).forEach(pathItem -> {
                if (!pathItem.isDirectory()) {
                    String fullPath = pathItem.getName();
                    // Extract filename from full path
                    int lastSlash = fullPath.lastIndexOf('/');
                    String filename = lastSlash >= 0 ? fullPath.substring(lastSlash + 1) : fullPath;
                    files.add(filename);
                }
            });
        } catch (Exception e) {
            if (isNotFoundError(e)) {
                return files;
            }
            throw e;
        }
        return files;
    }

    private boolean isNotFoundError(Exception e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("404") || msg.contains("BlobNotFound") ||
                msg.contains("PathNotFound") || msg.contains("FilesystemNotFound"));
    }

    private static ParsedUri parseAbfsUri(String uri) {
        // abfss://<filesystem>@<account>.dfs.fabric.microsoft.com/<path>
        if (!uri.startsWith("abfss://")) {
            throw new IllegalArgumentException("ABFS URI must start with abfss://: " + uri);
        }
        String rest = uri.substring("abfss://".length());
        int atIdx = rest.indexOf('@');
        if (atIdx < 0) {
            throw new IllegalArgumentException("Invalid ABFS URI (missing @): " + uri);
        }
        String filesystem = rest.substring(0, atIdx);
        String afterAt = rest.substring(atIdx + 1);
        int slashIdx = afterAt.indexOf('/');
        String accountHost;
        String path;
        if (slashIdx < 0) {
            accountHost = afterAt;
            path = "";
        } else {
            accountHost = afterAt.substring(0, slashIdx);
            path = afterAt.substring(slashIdx + 1);
        }
        return new ParsedUri(filesystem, accountHost, path);
    }

    private record ParsedUri(String filesystem, String accountHost, String path) {}
}
