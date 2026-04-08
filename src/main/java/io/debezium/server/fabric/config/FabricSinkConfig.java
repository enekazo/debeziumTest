package io.debezium.server.fabric.config;

import org.eclipse.microprofile.config.ConfigProvider;

public class FabricSinkConfig {

    public final String baseUri;
    public final String authType;
    public final String sasToken;
    // Service-principal (Entra ID) credentials — used when authType="service-principal"
    public final String spTenantId;
    public final String spClientId;
    public final String spClientSecret;
    public final long flushMaxBytes;
    public final int flushMaxRecords;
    public final long flushIntervalMs;
    public final String compression;
    public final String rowMarkerColumn;
    public final String stateFileName;
    public final String postgresJdbcUrl;
    public final String postgresUsername;
    public final String postgresPassword;
    public final long postgresMetaRefreshMs;
    public final boolean fetchRowOnUpdate;
    public final String topicPrefix;

    private FabricSinkConfig(
            String baseUri, String authType, String sasToken,
            String spTenantId, String spClientId, String spClientSecret,
            long flushMaxBytes, int flushMaxRecords, long flushIntervalMs,
            String compression, String rowMarkerColumn, String stateFileName,
            String postgresJdbcUrl, String postgresUsername, String postgresPassword,
            long postgresMetaRefreshMs, boolean fetchRowOnUpdate, String topicPrefix) {
        this.baseUri = baseUri;
        this.authType = authType;
        this.sasToken = sasToken;
        this.spTenantId = spTenantId;
        this.spClientId = spClientId;
        this.spClientSecret = spClientSecret;
        this.flushMaxBytes = flushMaxBytes;
        this.flushMaxRecords = flushMaxRecords;
        this.flushIntervalMs = flushIntervalMs;
        this.compression = compression;
        this.rowMarkerColumn = rowMarkerColumn;
        this.stateFileName = stateFileName;
        this.postgresJdbcUrl = postgresJdbcUrl;
        this.postgresUsername = postgresUsername;
        this.postgresPassword = postgresPassword;
        this.postgresMetaRefreshMs = postgresMetaRefreshMs;
        this.fetchRowOnUpdate = fetchRowOnUpdate;
        this.topicPrefix = topicPrefix;
    }

    public static FabricSinkConfig load() {
        var cfg = ConfigProvider.getConfig();

        String baseUri = cfg.getValue("fabric.landing.baseUri", String.class);

        // Authentication type: "sas" (default) or "service-principal"
        String authType = cfg.getOptionalValue("fabric.landing.auth", String.class)
                .orElse("sas");

        // SAS token: check config property first, then env var (used when authType="sas")
        String sasToken = cfg.getOptionalValue("fabric.landing.sasToken", String.class)
                .orElseGet(() -> {
                    String env = System.getenv("ONELAKE_SAS");
                    return env != null ? env : "";
                });

        // Service-principal credentials (used when authType="service-principal")
        String spTenantId = cfg.getOptionalValue("fabric.sp.tenantId", String.class)
                .orElseGet(() -> System.getenv("AZURE_TENANT_ID"));
        String spClientId = cfg.getOptionalValue("fabric.sp.clientId", String.class)
                .orElseGet(() -> System.getenv("AZURE_CLIENT_ID"));
        String spClientSecret = cfg.getOptionalValue("fabric.sp.clientSecret", String.class)
                .orElseGet(() -> System.getenv("AZURE_CLIENT_SECRET"));

        long flushMaxBytes = cfg.getOptionalValue("fabric.flush.maxBytes", Long.class)
                .orElse(1073741824L);
        int flushMaxRecords = cfg.getOptionalValue("fabric.flush.maxRecords", Integer.class)
                .orElse(200000);
        long flushIntervalMs = cfg.getOptionalValue("fabric.flush.intervalMs", Long.class)
                .orElse(10000L);

        String compression = cfg.getOptionalValue("fabric.parquet.compression", String.class)
                .orElse("SNAPPY");
        String rowMarkerColumn = cfg.getOptionalValue("fabric.rowMarker.column", String.class)
                .orElse("__rowMarker__")
                .trim();
        // Fabric Open Mirroring requires this exact case-sensitive column name.
        if (!"__rowMarker__".equals(rowMarkerColumn)) {
            rowMarkerColumn = "__rowMarker__";
        }
        String stateFileName = cfg.getOptionalValue("fabric.sequence.stateFileName", String.class)
                .orElse("_sequence.txt");

        String postgresJdbcUrl = cfg.getOptionalValue("fabric.postgres.jdbcUrl", String.class)
                .orElse(null);
        String postgresUsername = cfg.getOptionalValue("fabric.postgres.username", String.class)
                .orElse(null);

        // PostgreSQL password: check config property first, then env var
        String postgresPassword = cfg.getOptionalValue("fabric.postgres.password", String.class)
                .orElseGet(() -> {
                    String env = System.getenv("POSTGRES_PASSWORD");
                    return env != null ? env : null;
                });

        long postgresMetaRefreshMs = cfg.getOptionalValue("fabric.postgres.metadata.refreshIntervalMs", Long.class)
                .orElse(300000L);
        boolean fetchRowOnUpdate = cfg.getOptionalValue("fabric.postgres.fetchRowOnUpdate", Boolean.class)
                .orElse(false);

        String topicPrefix = cfg.getOptionalValue("debezium.source.topic.prefix", String.class)
                .orElse("");

        return new FabricSinkConfig(
                baseUri, authType, sasToken,
                spTenantId, spClientId, spClientSecret,
                flushMaxBytes, flushMaxRecords, flushIntervalMs,
                compression, rowMarkerColumn, stateFileName,
                postgresJdbcUrl, postgresUsername, postgresPassword,
                postgresMetaRefreshMs, fetchRowOnUpdate, topicPrefix);
    }
}
