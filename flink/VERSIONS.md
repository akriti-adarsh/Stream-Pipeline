# Pinned versions for the Flink and Iceberg catalog layers

Everything below was verified on 2026-07-30 by pulling the images, fetching
maven-metadata.xml from repo1.maven.org, building the image, and running
smoke tests. No :latest tags anywhere.

## Flink base image

| Item | Value |
|---|---|
| Image | `flink:1.20.5-scala_2.12-java17` |
| Digest (as pulled) | `sha256:5a781c3a3bf694d4befba7709fbc192403a461c2d852fed533587a0970e03f5e` |
| Java | Temurin 17.0.19 |
| Why | Newest 1.20.x patch on Docker Hub (pushed 2026-07-02). Flink 1.x kept on purpose, the connector ecosystem for Flink 2.x still lags. Scala 2.12 build is the standard binary for connector artifacts. |

Note: the task brief suggested 1.20.3 as an example. Docker Hub now has
1.20.4 and 1.20.5, so 1.20.5 was chosen and verified by pulling it.

## Connector jars baked into /opt/flink/lib

All four were confirmed present in the built image
(`stream-pipeline-flink:dev`, built from `flink/Dockerfile`) with sizes
matching the Content-Length reported by repo1.maven.org exactly.

| Artifact | Version | Size in image | URL |
|---|---|---|---|
| `org.apache.flink:flink-sql-connector-kafka` | `3.4.0-1.20` | 5,602,516 bytes | https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar |
| `org.apache.flink:flink-connector-jdbc` | `3.4.0-1.20` | 444,657 bytes | https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar |
| `org.postgresql:postgresql` | `42.7.13` | 1,220,948 bytes | https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar |
| `org.apache.flink:flink-sql-avro-confluent-registry` | `1.20.5` | 22,648,748 bytes | https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.5/flink-sql-avro-confluent-registry-1.20.5.jar |

Compatibility rationale, one line each:

- **flink-sql-connector-kafka 3.4.0-1.20**: the Kafka connector is released
  separately from Flink core; the `-1.20` suffix names the Flink minor it
  targets, and 3.4.0-1.20 is the newest 1.20-line release in
  maven-metadata.xml (later versions 4.x/5.x target Flink 2.x only). The
  `flink-sql-` uber jar bundles kafka-clients, so nothing else is needed.
- **flink-connector-jdbc 3.4.0-1.20**: newest JDBC connector release for
  Flink 1.20. Since 3.3.0 the source split into `flink-connector-jdbc-core`
  plus per-database artifacts (`flink-connector-jdbc-postgres` etc.), but
  the plain `org.apache.flink:flink-connector-jdbc` coordinate is still
  published as the combined artifact. Verified by unzipping the jar: it
  contains `org/apache/flink/connector/jdbc/postgres/**` and registers
  `PostgresFactory` (plus MySQL, Oracle, SQL Server, and others) in
  `META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory`.
  So this single jar is the right choice, no need for the split artifacts.
- **postgresql 42.7.13**: the JDBC connector does not bundle any database
  driver; 42.7.13 is the newest 42.7.x on Maven Central (July 2026) and the
  driver is independent of the Flink version.
- **flink-sql-avro-confluent-registry 1.20.5**: this format lives in the
  main Flink repo and is versioned in lockstep with Flink core, so it must
  match the image version exactly (1.20.5). It provides the
  `avro-confluent` format for reading Avro topics against a
  Confluent-compatible schema registry, which Redpanda exposes.

### Build verification performed

- `docker build -t stream-pipeline-flink:dev flink/` succeeded
  (image digest `sha256:972562646e280291674e7f2cb522ebaa7bf747a8b5a918611cb73daf55f23f2c`).
- `docker run --rm stream-pipeline-flink:dev ls -la /opt/flink/lib` shows
  all four jars at the sizes listed above. They land root-owned with mode
  644 (build-time RUN executes as root; the entrypoint drops to the
  `flink` user via gosu at runtime, which can read them fine).
- `docker run --rm stream-pipeline-flink:dev bash -c "/opt/flink/bin/sql-client.sh --help"`
  starts the SQL client and prints usage (`./sql-client [MODE] [OPTIONS]`).

## Iceberg REST catalog

| Item | Value |
|---|---|
| Chosen image | `apache/iceberg-rest-fixture:1.10.1` |
| Digest (as pulled) | `sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f` |
| Port | 8181 |
| Rejected | `tabulario/iceberg-rest:1.6.0` (last pushed 2024-08-28, Iceberg 1.6.0, effectively unmaintained; the project moved into the Apache repo as the rest-fixture image) |

Why this one: it is the Apache-maintained image (built from
`docker/iceberg-rest-fixture` in the apache/iceberg repo). Newest versioned
tag on Docker Hub is 1.10.1 (pushed 2025-12-22, newer than the 1.10.0
suggested in the brief), and the repo is actively maintained (`latest`
rebuilt 2026-04-29). Both candidate images were pulled to verify the tags
exist. PyIceberg's own integration CI (`dev/docker-compose-integration.yml`
in apache/iceberg-python) runs against exactly this image with MinIO, so
compatibility with PyIceberg 0.11.1 is exercised upstream.

Verification performed:

- Booted `apache/iceberg-rest-fixture:1.10.1` and hit
  `GET /v1/config`; it returned the full REST spec endpoint list
  (namespaces, tables, views, transactions), Jetty listening on 8181.
- Confirmed S3 support is shaded into the server uber jar
  (`/usr/lib/iceberg-rest/iceberg-rest-adapter.jar`, ~212 MB): the zip
  directory contains `org/apache/iceberg/aws/s3/S3FileIO` entries, so no
  extra aws-bundle jar is needed.
- With no env vars it defaults to an in-memory SQLite JdbcCatalog and a
  temp warehouse, so config must be provided via env vars in compose.

### Env var convention (verified from the image README in apache/iceberg)

Any env var prefixed `CATALOG_` becomes an Iceberg catalog property:
strip the `CATALOG_` prefix, lowercase the rest, a single underscore
becomes a dot, a double underscore becomes a hyphen.
Examples: `CATALOG_WAREHOUSE` -> `warehouse`,
`CATALOG_IO__IMPL` -> `io-impl`, `CATALOG_S3_ENDPOINT` -> `s3.endpoint`,
`CATALOG_JDBC_USER` -> `jdbc.user`.
AWS credentials use the standard SDK vars (`AWS_ACCESS_KEY_ID` etc.),
not the `CATALOG_` prefix.

Recommended block for pointing it at MinIO (same shape PyIceberg CI uses):

```yaml
environment:
  - CATALOG_WAREHOUSE=s3://lakehouse/
  - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
  - CATALOG_S3_ENDPOINT=http://minio:9000
  - CATALOG_S3_PATH__STYLE__ACCESS=true
  - AWS_ACCESS_KEY_ID=<minio access key>
  - AWS_SECRET_ACCESS_KEY=<minio secret key>
  - AWS_REGION=us-east-1
```

Notes: `CATALOG_S3_PATH__STYLE__ACCESS` maps to `s3.path-style-access`,
which MinIO generally needs (PyIceberg CI omits it and works, but setting
it is the safe default for MinIO behind a hostname). `AWS_REGION` is
required by the AWS SDK even though MinIO ignores it. Wiring into
docker-compose.yml is owned by another task; this section only records the
verified image and its configuration surface.
