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

## How to run your solution?

1. Create `.env` and set credentials:

```bash
cp .env.example .env
# edit .env and set TARGET_API_KEY
```

2. Run the full stack (Postgres + Temporal + worker + runner):

```bash
sh run-ingestion.sh
```

3. Run local end-to-end test mode without external API traffic:

```bash
USE_MOCK_API=1 MOCK_TOTAL_EVENTS=20000 INGESTION_RUN_ID=mock-test-1 sh run-ingestion.sh
```

4. Optional one-worker debug mode:

```bash
INGESTION_ACTIVITY_PARALLELISM=1 INGESTION_WORKER_CONCURRENCY=1 INGESTION_CURSOR_SHARDS=1 sh run-ingestion.sh
```

5. Watch worker logs:

```bash
docker logs -f assignment-ingestion-worker
```

## Architecture overview

- **Temporal workflow (`ingestionWorkflow`)**
  - Plans the run strategy and segments.
  - Claims segments from PostgreSQL.
  - Executes ingestion activities.
  - Tracks progress and marks run completed/failed.
- **Activities (`packages/ingestion/src/activities.ts`)**
  - `planRun`, `claimSegments`, `ingestSegment`, `getProgress`, `evaluateRun`, `updateRunRps`.
  - Cursor strategy supports resumability via checkpointed `last_cursor`.
- **PostgreSQL**
  - Stores events in partitioned `events` table.
  - Stores run state in `ingestion_runs`, segment checkpoints/retries in `ingestion_segments`, and health in `worker_heartbeats`.
  - Event writes are idempotent with `ON CONFLICT (event_id, timestamp) DO NOTHING`.
- **Runner/Worker split**
  - Runner starts/attaches to workflow and monitors progress.
  - Worker executes activities and sends heartbeats.
- **Rate limiting**
  - Global Bottleneck limiter with sliding-window settings:
    - `API_RATE_LIMIT_REQUESTS`
    - `API_RATE_LIMIT_WINDOW_SECONDS`
    - `API_MIN_TIME_MS`
  - Uses `X-RateLimit-*` headers and cooldowns on 429.

## Any discoveries about the API?

There were 5 key discoveries about the API:

1) The `limit` can be set as high as `5000`, reducing total requests to `3,000,000 / 5,000 = 600`.
2) All 3M events are concentrated in July 21-31, 2026 (an 11-day window).
3) Cursors are unsigned base64 JSON, so they are craftable for partitioning and parallel fetch planning.
4) The API is sorted newest-first, and cursor `ts` behaves as a seek position.
5) Rate limiting is approximately 10 requests per sliding window.

## What you would improve with more time?

Without parallelism: 600 requests sequential = ~10 minutes
With 11 parallel workers: 55 requests each = ~1 minute. We would need to stagger worker startup in this case so we don't hit the ceiling on requests/window.

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
