FROM quay.io/debezium/server:3.4.2.Final

# Add the custom sink JAR to Debezium Server's classpath
COPY target/debezium-server-sink-fabric-1.0.0-SNAPSHOT-all.jar /debezium/lib/

# Config is mounted at /debezium/conf/application.properties
# Oracle JDBC driver is already included in the fat jar

EXPOSE 8080
