#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EVENTS="${EVENTS:-10000}"
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

echo "Ensuring docker compose stack is running..."
docker compose up -d
wait_for_health

echo "Resetting database tables..."
psql_query "TRUNCATE TABLE reward_ledger, processed_events, customer_balances RESTART IDENTITY;"

LOADGEN_LOG="$(mktemp)"
LOADGEN_BIN="$(mktemp)"
trap 'rm -f "$LOADGEN_LOG" "$LOADGEN_BIN"' EXIT

echo "Building load generator..."
go build -o "$LOADGEN_BIN" ./cmd/loadgen

echo "Starting load generator (events=$EVENTS, concurrency=$CONCURRENCY, customers=$CUSTOMERS)..."
"$LOADGEN_BIN" \
  -n "$EVENTS" \
  -concurrency "$CONCURRENCY" \
  -customers "$CUSTOMERS" \
  -timeout "$TIMEOUT" >"$LOADGEN_LOG" 2>&1 &
LOADGEN_PID=$!

KILL_THRESHOLD=$((EVENTS / 2))
killed=0
for ((i = 1; i <= 300; i++)); do
  if ! kill -0 "$LOADGEN_PID" 2>/dev/null; then
    break
  fi
  count="$(psql_query "SELECT COUNT(*) FROM processed_events;" | tr -d '[:space:]')"
  if [[ -n "$count" ]] && (( count >= KILL_THRESHOLD && count < EVENTS )); then
    echo "Killing eventpulse container mid-run at processed_events=$count/$EVENTS..."
    docker compose kill eventpulse
    killed=1
    break
  fi
  sleep 0.2
done

if (( killed == 0 )); then
  echo "Warning: loadgen finished or did not reach threshold before kill; killing eventpulse now..."
  docker compose kill eventpulse
fi

sleep 2
mid_count="$(psql_query "SELECT COUNT(*) FROM processed_events;" | tr -d '[:space:]')"
echo "Starting eventpulse container again (processed_events while down=$mid_count)..."
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
mismatch_count="$(psql_query "
  SELECT COUNT(*) FROM (
    SELECT
      COALESCE(cb.customer_id, rl.customer_id) AS customer_id
    FROM customer_balances cb
    FULL OUTER JOIN (
      SELECT customer_id, SUM(points_delta) AS ledger_sum
      FROM reward_ledger
      GROUP BY customer_id
    ) rl ON cb.customer_id = rl.customer_id
    WHERE COALESCE(cb.balance, -1) <> COALESCE(rl.ledger_sum, -1)
       OR cb.customer_id IS NULL
       OR rl.customer_id IS NULL
  ) mismatches;
" | tr -d '[:space:]')"

echo "Verification: processed_events=$processed_count (expected $EVENTS), balance_mismatches=$mismatch_count"

if [[ "$processed_count" == "$EVENTS" && "$mismatch_count" == "0" ]]; then
  echo "PASS"
  exit 0
else
  echo "FAIL"
  exit 1
fi
