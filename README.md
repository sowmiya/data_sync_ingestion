## DataSync Ingestion (Temporal + PostgreSQL)

This repository is organized as a production-style ingestion system using **Temporal** for orchestration and **PostgreSQL** for durable storage/checkpointing.

### What this solution does

- Connects to the DataSync API
- Extracts all events (target: 3,000,000)
- Handles pagination and cursor responses
- Respects API limits with a global rate limiter
- Stores data idempotently in PostgreSQL
- Resumes safely after crashes using DB checkpoints + Temporal durable workflow state
- Tracks throughput, progress, and worker health

## Project layout

- `packages/ingestion/` - TypeScript ingestion app
- `packages/ingestion/src/workflows/` - Temporal workflow orchestration
- `packages/ingestion/src/activities.ts` - ingestion activities (claim, fetch, store, checkpoint)
- `packages/ingestion/src/repositories/` - PostgreSQL repositories
- `packages/ingestion/sql/001_init.sql` - schema + indexes + progress view
- `docker-compose.yml` - Postgres + Temporal + worker + runner services
- `run-ingestion.sh` - one-command runner + progress monitor

## Run

1. Add your API key:

```bash
cp .env.example .env
# edit .env and set TARGET_API_KEY
```

2. Run ingestion:

```bash
sh run-ingestion.sh
```

Local end-to-end test mode (no external API traffic):

```bash
USE_MOCK_API=1 MOCK_TOTAL_EVENTS=20000 sh run-ingestion.sh
```

This starts all services in Docker and continuously prints:

- total events stored in `events`
- segment completion progress
- run status and events/sec
- worker health (active/stale workers)

## Temporal orchestration model

- **Workflow**: `ingestionWorkflow`
  - Plans strategy (page-based when supported, cursor fallback)
  - Claims work segments from PostgreSQL checkpoints
  - Runs segment ingestion activities in parallel
  - Updates progress and run throughput
  - Marks run `completed` or `failed`

- **Activities**
  - `planRun`: detect API capabilities and seed segments
  - `claimSegments`: atomically claim pending/failed/stale segments
  - `ingestSegment`: fetch page/cursor data and insert events
  - `getProgress` / `evaluateRun` / `updateRunRps`

- **Resumability**
  - Temporal persists workflow execution state
  - PostgreSQL stores per-segment status, retries, and cursor checkpoints
  - Stale in-progress segments are reclaimed automatically
  - Event writes are idempotent (`ON CONFLICT DO NOTHING`)

## Throughput and limits

- Global API rate limiting is handled with Bottleneck (`API_RATE_LIMIT_RPS`)
- Parallelism is controlled with:
  - `INGESTION_ACTIVITY_PARALLELISM`
  - `INGESTION_WORKER_CONCURRENCY`
- Bulk inserts are chunked for PostgreSQL parameter limits

## Useful SQL checks

```sql
SELECT * FROM ingestion_progress;
SELECT COUNT(*) FROM events;
SELECT * FROM worker_heartbeats ORDER BY last_heartbeat DESC;
```

## Notes

- If `TARGET_API_KEY` is missing/expired, ingestion fails fast.
- Re-running with the same `INGESTION_RUN_ID` resumes unfinished work.
- Use a new `INGESTION_RUN_ID` for a fresh full ingestion.

## AI tooling disclosure

This implementation was developed with OpenAI Codex assistance for architecture, coding, and refactoring.
