#!/usr/bin/env bash
# End-to-end acceptance: cold start the WHOLE platform, push a compressed day
# of traffic through it, assert real row counts at every layer, tear down.
#
#   make e2e            full run (fresh volumes, full profile)
#   KEEP_UP=1 make e2e  leave the stack running afterwards
#
# Every assertion prints the observed number; failures name the layer.
set -euo pipefail
cd "$(dirname "$0")/.."

SECONDS=0
say() { printf '\n== [%4ds] %s\n' "$SECONDS" "$*"; }

PSQL="docker compose exec -T postgres psql -U stream -d stream -Atc"

fail() { echo "E2E FAIL: $*" >&2; exit 1; }

assert_ge() { # value threshold label
    [ "$1" -ge "$2" ] || fail "$3: got $1, need >= $2"
    echo "   ok: $3 = $1 (>= $2)"
}

say "cold start: removing any previous stack and volumes"
docker compose --profile full down -v --remove-orphans >/dev/null 2>&1 || true

say "bringing up the FULL profile"
docker compose --profile full up -d --wait --build
UP_AT=$SECONDS

say "waiting for first data at the broker (constraint: producing within 60s of up)"
deadline=$((UP_AT + 90))
while [ "$SECONDS" -lt "$deadline" ]; do
    events=$(docker compose exec -T redpanda rpk topic consume rides.events -n 1 -o start -f '%v' 2>/dev/null | head -c 1 || true)
    [ -n "$events" ] && break
    sleep 3
done
[ -n "${events:-}" ] || fail "no rides.events within $((deadline - UP_AT))s of up"
echo "   ok: first event on the broker at t=$((SECONDS - UP_AT))s after up"

say "letting the compressed day run"
sleep 75

say "layer 1: broker topics"
for topic in rides.events rides.events.clean rides.sessions payments.transactions drivers.locations; do
    hwm=$(docker compose exec -T redpanda rpk topic describe "$topic" -p 2>/dev/null \
        | awk 'NR>1 {sum += $4} END {print sum+0}')
    assert_ge "${hwm:-0}" 1 "topic $topic high watermark"
done

say "layer 2: postgres raw (idempotent sink)"
assert_ge "$($PSQL 'SELECT count(*) FROM raw.rides_events')" 500 "raw.rides_events rows"
assert_ge "$($PSQL 'SELECT count(*) FROM raw.ride_sessions')" 20 "raw.ride_sessions rows"
assert_ge "$($PSQL 'SELECT count(*) FROM raw.driver_locations')" 5000 "raw.driver_locations rows"
assert_ge "$($PSQL 'SELECT count(*) FROM raw.payments_transactions')" 10 "raw.payments rows"
dups=$($PSQL 'SELECT count(*) - count(DISTINCT event_id) FROM raw.rides_events')
[ "$dups" -eq 0 ] || fail "duplicate event_ids in raw.rides_events: $dups"
echo "   ok: zero duplicate event_ids"

say "layer 3: flink jobs and windowed sinks"
running=$(curl -sf localhost:18083/jobs/overview | grep -o '"state":"RUNNING"' | wc -l)
assert_ge "$running" 3 "flink RUNNING jobs"
assert_ge "$($PSQL 'SELECT count(*) FROM flink.city_metrics')" 3 "flink.city_metrics windows"
assert_ge "$($PSQL 'SELECT count(*) FROM flink.driver_utilisation')" 3 "flink.driver_utilisation sessions"

say "layer 4: iceberg lakehouse"
ICE_ROWS=$(uv run python -c "
from sinks.iceberg_sink import IcebergSinkConfig, open_catalog, TABLE_NAME
print(open_catalog(IcebergSinkConfig()).load_table(TABLE_NAME).scan().to_arrow().num_rows)")
assert_ge "$ICE_ROWS" 500 "iceberg lakehouse.rides_events rows"

say "layer 5: dbt build (models, snapshot, seed, all tests)"
uv run dbt build --project-dir dbt --profiles-dir dbt | tail -2
summary=$(uv run dbt build --project-dir dbt --profiles-dir dbt 2>/dev/null | grep -oE 'ERROR=[0-9]+' | tail -1)
[ "$summary" = "ERROR=0" ] || fail "dbt build not clean: $summary"
assert_ge "$($PSQL 'SELECT count(*) FROM analytics_marts.fct_rides')" 20 "fct_rides rows"

say "layer 6: data quality gates"
uv run python -m quality.runner --mode fail --freshness-minutes 30 \
    || fail "quality gates failed in fail mode"
assert_ge "$($PSQL 'SELECT count(*) FROM serving.dq_results')" 20 "dq_results rows"

say "e2e green in ${SECONDS}s"
if [ "${KEEP_UP:-0}" != "1" ]; then
    say "tearing down"
    docker compose --profile full down -v --remove-orphans
fi
