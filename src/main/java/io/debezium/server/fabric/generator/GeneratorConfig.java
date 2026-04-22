package io.debezium.server.fabric.generator;

import org.eclipse.microprofile.config.ConfigProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configuration for the dummy data generator.
 * All properties live under the {@code generator.*} namespace and are optional
 * (the generator is disabled by default so it never interferes with existing deployments).
 */
public class GeneratorConfig {

    /** Whether the generator is active. Default: false. */
    public final boolean enabled;

    /**
     * Comma-separated list of tables to insert into, in "SCHEMA.TABLE" format.
     * Example: {@code generator.tables=HR.DEPARTMENTS,HR.EMPLOYEES}
     */
    public final List<String> tables;

    /** Milliseconds to wait between successive insert batches. Default: 5000. */
    public final long intervalMs;

    /** Number of rows inserted per table per cycle. Default: 10. */
    public final int batchSize;

    /**
     * Probability (0.0–1.0) that a nullable column gets a NULL value instead of
     * a random value. Default: 0.1 (10%).
     */
    public final double nullProbability;

    /** Oracle JDBC URL — reused from {@code fabric.oracle.jdbcUrl} if not overridden. */
    public final String jdbcUrl;

    /** Oracle username — reused from {@code fabric.oracle.username}. */
    public final String username;

    /** Oracle password — reused from {@code fabric.oracle.password} / ORACLE_PASSWORD env var. */
    public final String password;

    private GeneratorConfig(boolean enabled, List<String> tables, long intervalMs,
                            int batchSize, double nullProbability,
                            String jdbcUrl, String username, String password) {
        this.enabled = enabled;
        this.tables = Collections.unmodifiableList(tables);
        this.intervalMs = intervalMs;
        this.batchSize = batchSize;
        this.nullProbability = nullProbability;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public static GeneratorConfig load() {
        var cfg = ConfigProvider.getConfig();

        boolean enabled = cfg.getOptionalValue("generator.enabled", Boolean.class).orElse(false);
        if (!enabled) {
            return new GeneratorConfig(false, Collections.emptyList(), 5000L, 10, 0.1,
                    null, null, null);
        }

        String tablesRaw = cfg.getOptionalValue("generator.tables", String.class).orElse("");
        List<String> tables = tablesRaw.isBlank()
                ? Collections.emptyList()
                : Arrays.stream(tablesRaw.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());

        long intervalMs = cfg.getOptionalValue("generator.interval.ms", Long.class).orElse(5000L);
        int batchSize   = cfg.getOptionalValue("generator.batch.size",  Integer.class).orElse(10);
        double nullProb = cfg.getOptionalValue("generator.null.probability", Double.class).orElse(0.1);

        // Reuse Oracle connection properties from the fabric.oracle.* namespace
        String jdbcUrl = cfg.getOptionalValue("generator.jdbcUrl", String.class)
                .orElseGet(() -> cfg.getOptionalValue("fabric.oracle.jdbcUrl", String.class).orElse(null));
        String username = cfg.getOptionalValue("generator.username", String.class)
                .orElseGet(() -> cfg.getOptionalValue("fabric.oracle.username", String.class).orElse(null));
        String password = cfg.getOptionalValue("generator.password", String.class)
                .orElseGet(() -> cfg.getOptionalValue("fabric.oracle.password", String.class)
                        .orElseGet(() -> {
                            String env = System.getenv("ORACLE_PASSWORD");
                            return env != null ? env : null;
                        }));

        return new GeneratorConfig(true, tables, intervalMs, batchSize, nullProb,
                jdbcUrl, username, password);
    }
}
