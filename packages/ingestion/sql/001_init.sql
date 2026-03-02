CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  source TEXT,
  tenant_id TEXT,
  user_id TEXT,
  session_id TEXT,
  timestamp TIMESTAMPTZ NOT NULL,
  properties JSONB,
  metadata JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE TABLE IF NOT EXISTS events_2023 PARTITION OF events
  FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS events_2024 PARTITION OF events
  FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS events_2025 PARTITION OF events
  FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS events_2026 PARTITION OF events
  FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_ts ON events (event_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_tenant_id ON events (tenant_id, timestamp DESC) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events (user_id, timestamp DESC) WHERE user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'running',
  strategy TEXT NOT NULL DEFAULT 'paginated',
  total_segments INTEGER NOT NULL DEFAULT 0,
  completed_segments INTEGER NOT NULL DEFAULT 0,
  failed_segments INTEGER NOT NULL DEFAULT 0,
  total_events BIGINT NOT NULL DEFAULT 0,
  events_per_sec NUMERIC(12,2) NOT NULL DEFAULT 0,
  error_message TEXT,
  config JSONB,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_segments (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
  segment_id TEXT NOT NULL,
  segment_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  worker_id TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  events_fetched INTEGER NOT NULL DEFAULT 0,
  events_stored INTEGER NOT NULL DEFAULT 0,
  last_cursor TEXT,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, segment_id)
);

CREATE INDEX IF NOT EXISTS idx_segments_run_status ON ingestion_segments (run_id, status);
CREATE INDEX IF NOT EXISTS idx_segments_run_retry ON ingestion_segments (run_id, retry_count);
CREATE INDEX IF NOT EXISTS idx_segments_worker ON ingestion_segments (worker_id);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  segment_id TEXT,
  status TEXT NOT NULL DEFAULT 'idle',
  events_processed BIGINT NOT NULL DEFAULT 0,
  current_rps NUMERIC(10,2) NOT NULL DEFAULT 0,
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_run_id ON worker_heartbeats (run_id);

CREATE OR REPLACE VIEW ingestion_progress AS
SELECT
  r.run_id,
  r.status,
  r.strategy,
  r.total_segments,
  r.completed_segments,
  r.failed_segments,
  (SELECT COUNT(*) FROM ingestion_segments s WHERE s.run_id = r.run_id AND s.status = 'pending') AS pending_segments,
  (SELECT COUNT(*) FROM ingestion_segments s WHERE s.run_id = r.run_id AND s.status = 'in_progress') AS in_progress_segments,
  r.total_events,
  r.events_per_sec,
  ROUND(
    CASE
      WHEN r.total_segments = 0 THEN 0
      ELSE (r.completed_segments::numeric / r.total_segments::numeric) * 100
    END,
    2
  ) AS pct_complete,
  r.started_at,
  r.completed_at,
  r.updated_at
FROM ingestion_runs r
ORDER BY r.started_at DESC;
