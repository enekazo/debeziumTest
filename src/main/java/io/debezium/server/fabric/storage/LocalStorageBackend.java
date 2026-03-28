package io.debezium.server.fabric.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local filesystem implementation of StorageBackend.
 * Uses java.nio.file APIs. Intended for dev/smoke-test use.
 *
 * baseUri format: file:///absolute/path/to/landing
 */
public class LocalStorageBackend implements StorageBackend {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageBackend.class);

    private final Path basePath;

    public LocalStorageBackend(String baseUri) {
        String uriStr = baseUri;
        if (uriStr.startsWith("file://")) {
            this.basePath = Paths.get(URI.create(uriStr));
        } else {
            this.basePath = Paths.get(uriStr);
        }
        LOG.info("LocalStorageBackend initialized at {}", basePath);
    }

    @Override
    public void uploadFile(String tableFolder, String filename, Path localFile) throws Exception {
        Path dest = resolveFile(tableFolder, filename);
        Files.createDirectories(dest.getParent());
        // Atomic move preferred; fall back to copy+replace on cross-filesystem
        try {
            Files.move(localFile, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            Files.copy(localFile, dest, StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(localFile);
        }
        LOG.debug("Uploaded {} → {}", localFile, dest);
    }

    @Override
    public String readTextFile(String tableFolder, String filename) throws Exception {
        Path file = resolveFile(tableFolder, filename);
        if (!Files.exists(file)) {
            return null;
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    @Override
    public void writeTextFile(String tableFolder, String filename, String content) throws Exception {
        Path file = resolveFile(tableFolder, filename);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    @Override
    public boolean exists(String tableFolder, String filename) throws Exception {
        return Files.exists(resolveFile(tableFolder, filename));
    }

    @Override
    public List<String> listFiles(String tableFolder) throws Exception {
        Path folder = basePath.resolve(tableFolder);
        if (!Files.exists(folder)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(folder)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toList());
        }
    }

    private Path resolveFile(String tableFolder, String filename) {
        return basePath.resolve(tableFolder).resolve(filename);
    }
}
