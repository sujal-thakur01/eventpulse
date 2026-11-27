#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVENTS="${EVENTS:-30000}"
RATE="${RATE:-1000}"
CONCURRENCY="${CONCURRENCY:-100}"
CUSTOMERS="${CUSTOMERS:-100}"
TIMEOUT="${TIMEOUT:-5m}"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/healthz}"

psql_query() {
  docker compose exec -T postgres psql -U eventpulse -d eventpulse -tAc "$1"
}

wait_for_health() {
  local retries=60
  for ((i = 1; i <= retries; i++)); do
    if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for eventpulse healthz at $HEALTH_URL" >&2
  return 1
}

echo "Rebuilding eventpulse image..."
docker compose build eventpulse

echo "Ensuring docker compose stack is running..."
docker compose up -d --force-recreate eventpulse
wait_for_health

echo "Resetting database tables..."
psql_query "TRUNCATE TABLE reward_ledger, processed_events, customer_balances RESTART IDENTITY;"

LOADGEN_LOG="$(mktemp)"
LOADGEN_BIN="$(mktemp)"
EXPECTED_JSON="$(mktemp)"
trap 'rm -f "$LOADGEN_LOG" "$LOADGEN_BIN" "$EXPECTED_JSON"' EXIT

echo "Building load generator..."
go build -o "$LOADGEN_BIN" ./cmd/loadgen

TEST_START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Starting load generator (events=$EVENTS, rate=$RATE/s, concurrency=$CONCURRENCY, customers=$CUSTOMERS)..."
"$LOADGEN_BIN" \
  -n "$EVENTS" \
  -rate "$RATE" \
  -concurrency "$CONCURRENCY" \
  -customers "$CUSTOMERS" \
  -timeout "$TIMEOUT" \
  -expected-out "$EXPECTED_JSON" >"$LOADGEN_LOG" 2>&1 &
LOADGEN_PID=$!

sleep 10
if ! kill -0 "$LOADGEN_PID" 2>/dev/null; then
  echo "Load generator finished or exited before 10s kill" >&2
  cat "$LOADGEN_LOG"
  echo "FAIL"
  exit 1
fi

mid_count="$(psql_query "SELECT COUNT(*) FROM processed_events;" | tr -d '[:space:]')"
echo "Killing eventpulse container 10s in (processed_events=$mid_count/$EVENTS)..."
docker compose kill eventpulse

sleep 2
down_count="$(psql_query "SELECT COUNT(*) FROM processed_events;" | tr -d '[:space:]')"
echo "Starting eventpulse container again after 2s (processed_events=$down_count)..."
docker compose start eventpulse
wait_for_health

echo "Waiting for load generator to finish..."
loadgen_status=0
wait "$LOADGEN_PID" || loadgen_status=$?
cat "$LOADGEN_LOG"

if (( loadgen_status != 0 )); then
  echo "Load generator failed with exit code $loadgen_status"
  echo "FAIL"
  exit 1
fi

processed_count="$(psql_query "SELECT COUNT(*) FROM processed_events;" | tr -d '[:space:]')"
ledger_count="$(psql_query "SELECT COUNT(*) FROM reward_ledger;" | tr -d '[:space:]')"
expected_b64="$(base64 < "$EXPECTED_JSON" | tr -d '\n')"
balance_mismatch_count="$(psql_query "
  WITH expected AS (
    SELECT key AS customer_id, value::bigint AS expected_points
    FROM jsonb_each_text(convert_from(decode('$expected_b64', 'base64'), 'UTF8')::jsonb)
  )
  SELECT COUNT(*)
  FROM expected e
  FULL OUTER JOIN customer_balances cb ON cb.customer_id = e.customer_id
  WHERE cb.customer_id IS NULL
     OR e.customer_id IS NULL
     OR cb.balance <> e.expected_points;
" | tr -d '[:space:]')"

duplicate_skipped_count="$(docker compose logs --no-color --since "$TEST_START_TS" eventpulse | grep -c '"duplicate_skipped"' || true)"

echo "Verification: processed_events=$processed_count (expected $EVENTS), reward_ledger=$ledger_count (expected $EVENTS), balance_mismatches=$balance_mismatch_count, duplicate_skipped=$duplicate_skipped_count"

if [[ "$processed_count" == "$EVENTS" && "$ledger_count" == "$EVENTS" && "$balance_mismatch_count" == "0" ]]; then
  echo "PASS"
  exit 0
else
  echo "FAIL"
  exit 1
fi
