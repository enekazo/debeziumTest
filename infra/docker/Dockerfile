# ── Stage 1: build ────────────────────────────────────────────────────────────────────────────
# Quarkus build — CDI bean discovery (including @Named("fabric")) happens HERE,
# baking FabricMirroringSink into the generated application code.
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /build

# Pre-fetch dependencies using pom.xml only (cached layer, only re-runs when pom.xml changes)
COPY pom.xml .
RUN mvn -q dependency:go-offline || true

# Build the Quarkus fast-jar application
COPY src ./src
RUN mvn package -DskipTests -q

# ── Stage 2: runtime ──────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /debezium

# Install curl for the health check in docker-compose
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy the Quarkus fast-jar layout produced by quarkus-maven-plugin
COPY --from=builder /build/target/quarkus-app ./

# Config is mounted at /debezium/config/application.properties (Quarkus default)
# Data / offset storage is mounted at /debezium/data/
RUN mkdir -p /debezium/config /debezium/data

# Port 8080: Quarkus HTTP (health endpoint)
# Port 5005: JDWP remote debug (active when JAVA_TOOL_OPTIONS is set via docker-compose)
EXPOSE 8080 5005

ENTRYPOINT ["java", "-jar", "/debezium/quarkus-run.jar"]
