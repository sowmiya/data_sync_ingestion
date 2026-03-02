#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${INGESTION_RUN_ID:-datasync-ingestion-main}"
USE_MOCK_API="${USE_MOCK_API:-0}"

if [[ "${USE_MOCK_API}" == "1" ]]; then
  export API_BASE_URL="${API_BASE_URL:-http://datasync-mock:8080/api/v1}"
  export TARGET_API_KEY="${TARGET_API_KEY:-mock-key}"
  export MOCK_TOTAL_EVENTS="${MOCK_TOTAL_EVENTS:-50000}"
  export INGESTION_EXPECTED_EVENTS="${INGESTION_EXPECTED_EVENTS:-${MOCK_TOTAL_EVENTS}}"
  printf 'Mode: %s\n' "mock API (no external calls)"
  INFRA_SERVICES=(postgres temporal datasync-mock temporal-ui)
else
  printf 'Mode: %s\n' "live API"
  INFRA_SERVICES=(postgres temporal temporal-ui)
fi

printf '%s\n' "=============================================="
printf '%s\n' "DataSync Ingestion (Temporal)"
printf '%s\n' "=============================================="
printf 'Run ID: %s\n' "$RUN_ID"

printf '\n%s\n' "Starting infra services..."
docker compose up -d --build "${INFRA_SERVICES[@]}"

printf '\n%s\n' "Waiting for PostgreSQL..."
until docker exec assignment-postgres pg_isready -U postgres >/dev/null 2>&1; do
  sleep 2
done

printf '%s\n' "Waiting for Temporal API..."
ATTEMPT=0

until nc -z 127.0.0.1 7233 >/dev/null 2>&1; do
  ATTEMPT=$((ATTEMPT + 1))
  printf '  waiting for temporal... (%d/90)\n' "$ATTEMPT"
  printf '  temporal container: %s\n' "$(docker inspect -f '{{.State.Status}}' assignment-temporal 2>/dev/null || echo 'unknown')"
  if (( ATTEMPT >= 90 )); then
    echo "Temporal API did not become reachable on localhost:7233 in time."
    echo "Recent temporal logs:"
    docker logs --tail 120 assignment-temporal || true
    exit 1
  fi
  sleep 2
done

printf '\n%s\n' "Starting ingestion services..."
docker compose up -d --build ingestion-worker ingestion-runner

printf '\n%s\n' "Monitoring ingestion progress..."
printf '%s\n' "(Press Ctrl+C to stop monitoring)"
printf '%s\n' "=============================================="

while true; do
  EVENT_COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -At -c "SELECT COUNT(*) FROM events;" 2>/dev/null || echo "0")

  PROGRESS_ROW=$(docker exec assignment-postgres psql -U postgres -d ingestion -At -F ',' -c "SELECT COALESCE(total_events,0), COALESCE(completed_segments,0), COALESCE(total_segments,0), COALESCE(status,'running'), COALESCE(events_per_sec,0) FROM ingestion_progress WHERE run_id = '${RUN_ID}' ORDER BY started_at DESC LIMIT 1;" 2>/dev/null || true)

  if [[ -n "${PROGRESS_ROW}" ]]; then
    IFS=',' read -r TOTAL_EVENTS COMPLETED_SEGMENTS TOTAL_SEGMENTS RUN_STATUS EVENTS_PER_SEC <<< "${PROGRESS_ROW}"
  else
    TOTAL_EVENTS=0
    COMPLETED_SEGMENTS=0
    TOTAL_SEGMENTS=0
    RUN_STATUS="starting"
    EVENTS_PER_SEC=0
  fi

  HEALTH_ROW=$(docker exec assignment-postgres psql -U postgres -d ingestion -At -F ',' -c "SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'working'), COUNT(*) FILTER (WHERE last_heartbeat < NOW() - INTERVAL '30 seconds') FROM worker_heartbeats WHERE run_id = '${RUN_ID}';" 2>/dev/null || echo "0,0,0")
  IFS=',' read -r TOTAL_WORKERS ACTIVE_WORKERS STALE_WORKERS <<< "${HEALTH_ROW}"

  RUNNER_STATE=$(docker inspect -f '{{.State.Status}}' assignment-ingestion 2>/dev/null || echo "missing")
  RUNNER_EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' assignment-ingestion 2>/dev/null || echo "-1")
  WORKER_STATE=$(docker inspect -f '{{.State.Status}}' assignment-ingestion-worker 2>/dev/null || echo "missing")

  printf '[%s] events=%s run_events=%s segments=%s/%s status=%s eps=%s workers=%s active=%s stale=%s worker_state=%s\n' \
    "$(date '+%H:%M:%S')" \
    "$EVENT_COUNT" "$TOTAL_EVENTS" "$COMPLETED_SEGMENTS" "$TOTAL_SEGMENTS" "$RUN_STATUS" "$EVENTS_PER_SEC" "$TOTAL_WORKERS" "$ACTIVE_WORKERS" "$STALE_WORKERS" "$WORKER_STATE"

  if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete"; then
    printf '\n%s\n' "=============================================="
    printf '%s\n' "INGESTION COMPLETE"
    printf 'Events in DB: %s\n' "$EVENT_COUNT"
    printf '%s\n' "=============================================="
    exit 0
  fi

  if [[ "$RUNNER_STATE" == "exited" && "$RUNNER_EXIT_CODE" != "0" ]]; then
    printf '\n%s\n' "=============================================="
    printf '%s\n' "INGESTION FAILED"
    printf 'Runner exit code: %s\n' "$RUNNER_EXIT_CODE"
    printf '%s\n' "----- Runner Logs (tail) -----"
    docker logs --tail 120 assignment-ingestion || true
    printf '%s\n' "----- Worker Logs (tail) -----"
    docker logs --tail 200 assignment-ingestion-worker || true
    printf '%s\n' "----- Failed Segments -----"
    docker exec assignment-postgres psql -U postgres -d ingestion -At -F '|' -c "SELECT segment_id, retry_count, COALESCE(error_message,'') FROM ingestion_segments WHERE run_id = '${RUN_ID}' AND status = 'failed' ORDER BY updated_at DESC LIMIT 10;" || true
    printf '%s\n' "=============================================="
    exit 1
  fi

  sleep 5
done
