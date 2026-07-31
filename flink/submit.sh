#!/usr/bin/env bash
# Submit every Flink SQL job to the session cluster, idempotently.
#
# Each job file carries a stable pipeline.name; a name already in a
# non-terminal state on the cluster is skipped, so re-running this script
# (or restarting the flink-sql-submit one-shot) never duplicates jobs.
# INSERT submissions are asynchronous: the SQL client returns once the job is
# accepted, and the jobs then run forever.
set -euo pipefail

HOST="${JOBMANAGER_HOST:-flink-jobmanager}"
REST="http://${HOST}:8081"
SQL_DIR="${SQL_DIR:-/opt/sql}"
CLIENT=/opt/flink/bin/sql-client.sh

# Point the SQL client at the session cluster (1.20 images use conf/config.yaml).
for conf in /opt/flink/conf/config.yaml /opt/flink/conf/flink-conf.yaml; do
    if [ -f "$conf" ]; then
        printf '\nrest.address: %s\nrest.port: 8081\n' "$HOST" >> "$conf"
    fi
done

echo "waiting for jobmanager REST at ${REST}"
for _ in $(seq 1 60); do
    if curl -sf "${REST}/overview" > /dev/null; then break; fi
    sleep 2
done
curl -sf "${REST}/overview" > /dev/null || { echo "jobmanager REST unreachable"; exit 1; }

running_names() {
    curl -sf "${REST}/jobs/overview" \
        | tr '{' '\n' \
        | grep -oE '"name":"[^"]+"|"state":"[A-Z]+"' \
        | paste - - 2>/dev/null || true
}

is_running() {
    running_names | grep -F "\"name\":\"$1\"" | grep -qE 'RUNNING|CREATED|RESTARTING'
}

submit() {
    local file="$1" name="$2"
    if is_running "$name"; then
        echo "skip ${name}: already on the cluster"
        return 0
    fi
    echo "submitting ${name} from ${file}"
    "$CLIENT" -i "${SQL_DIR}/01_sources.sql" -f "${SQL_DIR}/${file}"
}

submit 02_city_metrics.sql      city-metrics-and-late-output
submit 03_driver_utilisation.sql driver-utilisation
submit 04_ride_payment_join.sql  ride-payment-join

echo "submitted; current jobs:"
curl -sf "${REST}/jobs/overview" || true
echo
