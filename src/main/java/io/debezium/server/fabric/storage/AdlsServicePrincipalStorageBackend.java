package io.debezium.server.fabric.storage;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
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
 * Authenticates using an Entra ID (Azure AD) service principal via the
 * client credentials flow (tenant ID + client ID + client secret).
 *
 * baseUri format: abfss://<filesystem>@<account>.dfs.core.windows.net/<basePath>
 * Example:        abfss://raw@myadlsaccount.dfs.core.windows.net/landing/debezium
 */
public class AdlsServicePrincipalStorageBackend implements StorageBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AdlsServicePrincipalStorageBackend.class);
    private static final int UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

    private final DataLakeFileSystemClient fileSystemClient;
    private final String basePath;

    /**
     * @param baseUri      ABFS URI — abfss://&lt;filesystem&gt;@&lt;account&gt;.dfs.core.windows.net/&lt;path&gt;
     * @param tenantId     Entra ID (Azure AD) tenant ID (directory ID)
     * @param clientId     Service principal application (client) ID
     * @param clientSecret Service principal client secret value
     */
    public AdlsServicePrincipalStorageBackend(String baseUri,
                                              String tenantId,
                                              String clientId,
                                              String clientSecret) {
        ParsedUri parsed = parseAbfsUri(baseUri);

        ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

        // Filesystem-scoped endpoint: https://<account>.dfs.core.windows.net/<filesystem>
        String filesystemUrl = "https://" + parsed.accountHost + "/" + parsed.filesystem;

        this.fileSystemClient = new DataLakeFileSystemClientBuilder()
                .endpoint(filesystemUrl)
                .credential(credential)
                .buildClient();

        this.basePath = parsed.path.isEmpty() ? "" : (parsed.path.endsWith("/") ? parsed.path : parsed.path + "/");

        LOG.info("AdlsServicePrincipalStorageBackend initialized: account={}, filesystem={}, basePath={}",
                parsed.accountHost, parsed.filesystem, this.basePath);
    }

    @Override
    public void uploadFile(String tableFolder, String filename, Path localFile) throws Exception {
        // Write to a .tmp path first, then atomically rename to the final path.
        String tmpPath   = resolvePath(tableFolder, filename + ".tmp");
        String finalPath = resolvePath(tableFolder, filename);

        byte[] data = Files.readAllBytes(localFile);

        // Create (or overwrite) the temporary file in storage
        DataLakeFileClient tmpFileClient = fileSystemClient.createFile(tmpPath, true);

        // Upload in chunks to avoid large in-memory payloads on the wire
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

        // Atomic rename within the same filesystem (null → same filesystem)
        tmpFileClient.rename(null, finalPath);

        LOG.debug("Uploaded {} bytes to {}", data.length, finalPath);
    }

    @Override
    public String readTextFile(String tableFolder, String filename) throws Exception {
        String filePath = resolvePath(tableFolder, filename);
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
        String filePath = resolvePath(tableFolder, filename);
        byte[] data = content.getBytes(StandardCharsets.UTF_8);

        // Ensure parent directory exists
        String dirPath = resolveDir(tableFolder);
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
        String filePath = resolvePath(tableFolder, filename);
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
        String dirPath = resolveDir(tableFolder);
        List<String> files = new ArrayList<>();
        try {
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(dirPath);
            options.setRecursive(false);
            fileSystemClient.listPaths(options, null).forEach(pathItem -> {
                if (!pathItem.isDirectory()) {
                    String fullPath = pathItem.getName();
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

    /** Resolves a file path within the filesystem, handling empty tableFolder (root-level files). */
    private String resolvePath(String tableFolder, String filename) {
        if (tableFolder == null || tableFolder.isEmpty()) {
            return basePath + filename;
        }
        return basePath + tableFolder + "/" + filename;
    }

    /** Resolves a directory path within the filesystem, handling empty tableFolder (root level). */
    private String resolveDir(String tableFolder) {
        if (tableFolder == null || tableFolder.isEmpty()) {
            return basePath.isEmpty() ? "/" : basePath;
        }
        return basePath + tableFolder;
    }

    private boolean isNotFoundError(Exception e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("404") || msg.contains("BlobNotFound") ||
                msg.contains("PathNotFound") || msg.contains("FilesystemNotFound"));
    }

    private static ParsedUri parseAbfsUri(String uri) {
        // Expected: abfss://<filesystem>@<account>.dfs.core.windows.net/<path>
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
