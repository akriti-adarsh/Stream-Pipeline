# Flink image for the stream-pipeline project.
# Base: Apache Flink 1.20.5 (Scala 2.12 binary compat, Temurin Java 17).
# Flink 1.x chosen deliberately; the connector ecosystem for 2.x still lags.
# All connector jars are pinned and pulled from repo1.maven.org.
# See flink/VERSIONS.md for the compatibility rationale for each jar.
FROM flink:1.20.5-scala_2.12-java17

# Pinned connector versions (single source of truth for this image).
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG JDBC_CONNECTOR_VERSION=3.4.0-1.20
ARG POSTGRES_DRIVER_VERSION=42.7.13
ARG AVRO_CONFLUENT_VERSION=1.20.5

# Build-time RUN executes as root; jars land root-owned with mode 644,
# which the runtime "flink" user (dropped to via gosu) can read fine.
RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -O "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -O "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fSL -O "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"; \
    curl -fSL -O "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${AVRO_CONFLUENT_VERSION}/flink-sql-avro-confluent-registry-${AVRO_CONFLUENT_VERSION}.jar"
