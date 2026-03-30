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
    public final String oracleJdbcUrl;
    public final String oracleUsername;
    public final String oraclePassword;
    public final long oracleMetaRefreshMs;
    public final boolean fetchRowOnUpdate;
    public final String topicPrefix;

    private FabricSinkConfig(
            String baseUri, String authType, String sasToken,
            String spTenantId, String spClientId, String spClientSecret,
            long flushMaxBytes, int flushMaxRecords, long flushIntervalMs,
            String compression, String rowMarkerColumn, String stateFileName,
            String oracleJdbcUrl, String oracleUsername, String oraclePassword,
            long oracleMetaRefreshMs, boolean fetchRowOnUpdate, String topicPrefix) {
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
        this.oracleJdbcUrl = oracleJdbcUrl;
        this.oracleUsername = oracleUsername;
        this.oraclePassword = oraclePassword;
        this.oracleMetaRefreshMs = oracleMetaRefreshMs;
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
                .orElse("__rowMarker__");
        String stateFileName = cfg.getOptionalValue("fabric.sequence.stateFileName", String.class)
                .orElse("_sequence.txt");

        String oracleJdbcUrl = cfg.getOptionalValue("fabric.oracle.jdbcUrl", String.class)
                .orElse(null);
        String oracleUsername = cfg.getOptionalValue("fabric.oracle.username", String.class)
                .orElse(null);

        // Oracle password: check config property first, then env var
        String oraclePassword = cfg.getOptionalValue("fabric.oracle.password", String.class)
                .orElseGet(() -> {
                    String env = System.getenv("ORACLE_PASSWORD");
                    return env != null ? env : null;
                });

        long oracleMetaRefreshMs = cfg.getOptionalValue("fabric.oracle.metadata.refreshIntervalMs", Long.class)
                .orElse(300000L);
        boolean fetchRowOnUpdate = cfg.getOptionalValue("fabric.oracle.fetchRowOnUpdate", Boolean.class)
                .orElse(true);

        String topicPrefix = cfg.getOptionalValue("debezium.source.topic.prefix", String.class)
                .orElse("");

        return new FabricSinkConfig(
                baseUri, authType, sasToken,
                spTenantId, spClientId, spClientSecret,
                flushMaxBytes, flushMaxRecords, flushIntervalMs,
                compression, rowMarkerColumn, stateFileName,
                oracleJdbcUrl, oracleUsername, oraclePassword,
                oracleMetaRefreshMs, fetchRowOnUpdate, topicPrefix);
    }
}
