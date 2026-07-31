# Infrastructure layer

Local dev stack for the streaming pipeline: Redpanda (Kafka API plus built in Schema Registry), Redpanda Console, Postgres, MinIO. Defined in `docker-compose.yml` at the repo root, all services carry both the `core` and `full` compose profiles.

```
docker compose --profile core up -d --wait    # start, blocks until healthy
docker compose --profile core down            # stop (volumes are kept)
```

## Pinned images

| Service         | Image                                                    |
| --------------- | -------------------------------------------------------- |
| redpanda        | docker.redpanda.com/redpandadata/redpanda:v25.3.15       |
| console         | docker.redpanda.com/redpandadata/console:v3.8.0          |
| postgres        | postgres:16.14-alpine                                    |
| minio           | minio/minio:RELEASE.2025-09-07T16-13-09Z                 |
| bucket-init     | minio/mc:RELEASE.2025-08-13T08-35-41Z                    |

## Ports

Some defaults are shifted because 8080, 9000 and 9001 are already occupied by other projects on this machine.

| Service                  | From host (localhost)    | From other containers   |
| ------------------------ | ------------------------ | ----------------------- |
| Kafka API                | localhost:19092          | redpanda:9092           |
| Schema Registry          | localhost:18081          | redpanda:8081           |
| Redpanda HTTP proxy      | localhost:18082          | redpanda:8082           |
| Redpanda Admin API       | localhost:9644           | redpanda:9644           |
| Redpanda Console UI      | localhost:18080          | console:8080            |
| Postgres                 | localhost:5433           | postgres:5432           |
| MinIO S3 API             | localhost:19000          | minio:9000              |
| MinIO web console        | localhost:19001          | minio:9001              |

Container to container access requires being attached to the `pipeline` network (compose services in this file already are; external containers can join with `docker network connect pipeline <name>`).

## Credentials (local dev only, not secrets)

| System   | User       | Password   | Extra                          |
| -------- | ---------- | ---------- | ------------------------------ |
| Postgres | stream     | stream     | database `stream`              |
| MinIO    | minioadmin | minioadmin | buckets `lakehouse`, `warehouse` |

Redpanda and Console run without authentication in this dev setup.

## Notes

- Redpanda runs a single node in dev mode (`--mode dev-container --smp 1 --memory 1G --overprovisioned`). It exposes two Kafka listeners: `internal` (advertised as `redpanda:9092`, for containers) and `external` (advertised as `localhost:19092`, for host processes). Point your client at the side you are running on, otherwise the broker will advertise an address you cannot reach.
- The Schema Registry follows the same split: containers use `http://redpanda:8081`, host processes use `http://localhost:18081` (try `curl localhost:18081/subjects`).
- `postgres/init/00_init.sql` creates schemas `raw` and `serving`. Init scripts only run on the first start of an empty `postgres-data` volume; run `docker compose down -v` first if you need them to run again.
- `bucket-init` is a one shot service: it waits for MinIO to be healthy, creates the two buckets idempotently (`mc mb --ignore-existing`), lists them, then exits 0. Rerunning it is safe.
- Data lives in named volumes `redpanda-data`, `postgres-data`, `minio-data`. `docker compose --profile core down` keeps them; add `-v` to wipe.
