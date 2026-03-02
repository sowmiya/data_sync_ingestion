import { getPool } from '../db';
import { config } from '../config';
import { RunEvaluation, RunProgress, Segment, SegmentType } from '../types';

interface SegmentSeed {
  segmentId: string;
  segmentType: SegmentType;
}

export class RunRepository {
  async initRun(runId: string, strategy: string, totalSegments: number): Promise<{ isNew: boolean }> {
    const existing = await getPool().query(
      `SELECT status FROM ingestion_runs WHERE run_id = $1`,
      [runId]
    );

    if (existing.rows.length > 0) {
      await getPool().query(
        `UPDATE ingestion_runs
         SET status = CASE WHEN status = 'completed' THEN status ELSE 'running' END,
             updated_at = NOW()
         WHERE run_id = $1`,
        [runId]
      );
      await getPool().query(`DELETE FROM worker_heartbeats WHERE run_id = $1`, [runId]);
      return { isNew: false };
    }

    await getPool().query(
      `INSERT INTO ingestion_runs (run_id, status, strategy, total_segments, config)
       VALUES ($1, 'running', $2, $3, $4)`,
      [runId, strategy, totalSegments, JSON.stringify({ strategy })]
    );

    return { isNew: true };
  }

  async seedSegments(runId: string, segments: SegmentSeed[]): Promise<void> {
    if (segments.length === 0) return;

    const batchSize = 5000;
    for (let i = 0; i < segments.length; i += batchSize) {
      const batch = segments.slice(i, i + batchSize);
      const values: unknown[] = [runId];
      const tuples: string[] = [];

      batch.forEach((segment, index) => {
        const offset = index * 2;
        tuples.push(`($1, $${offset + 2}, $${offset + 3}, 'pending')`);
        values.push(segment.segmentId, segment.segmentType);
      });

      await getPool().query(
        `INSERT INTO ingestion_segments (run_id, segment_id, segment_type, status)
         VALUES ${tuples.join(', ')}
         ON CONFLICT (run_id, segment_id) DO NOTHING`,
        values
      );
    }
  }

  async claimSegments(
    runId: string,
    workerId: string,
    limit: number,
    staleSeconds: number,
    maxRetries: number
  ): Promise<Segment[]> {
    const result = await getPool().query<Segment>(
      `WITH candidates AS (
         SELECT id
         FROM ingestion_segments
         WHERE run_id = $1
           AND retry_count < $5
           AND (
             status = 'pending'
             OR status = 'failed'
             OR (status = 'in_progress' AND updated_at < NOW() - ($4::text || ' seconds')::interval)
           )
         ORDER BY
           CASE status
             WHEN 'pending' THEN 1
             WHEN 'failed' THEN 2
             ELSE 3
           END,
           id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       )
       UPDATE ingestion_segments AS s
       SET status = 'in_progress', worker_id = $3, started_at = COALESCE(s.started_at, NOW()), updated_at = NOW()
       FROM candidates
       WHERE s.id = candidates.id
       RETURNING s.segment_id AS "segmentId",
                 s.segment_type AS "segmentType",
                 s.status,
                 s.retry_count AS "retryCount",
                 s.last_cursor AS "lastCursor"`,
      [runId, limit, workerId, staleSeconds, maxRetries]
    );

    return result.rows;
  }

  async completeSegment(
    runId: string,
    segmentId: string,
    eventsFetched: number,
    eventsInserted: number
  ): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_segments
       SET status = 'completed',
           events_fetched = $1,
           events_stored = $2,
           completed_at = NOW(),
           updated_at = NOW()
       WHERE run_id = $3 AND segment_id = $4`,
      [eventsFetched, eventsInserted, runId, segmentId]
    );

    await getPool().query(
      `UPDATE ingestion_runs
       SET completed_segments = (
             SELECT COUNT(*) FROM ingestion_segments
             WHERE run_id = $1 AND status = 'completed'
           ),
           failed_segments = (
             SELECT COUNT(*) FROM ingestion_segments
             WHERE run_id = $1 AND status = 'failed' AND retry_count >= $2
           ),
           total_events = (
             SELECT COALESCE(SUM(events_stored), 0) FROM ingestion_segments
             WHERE run_id = $1 AND status = 'completed'
           ),
           updated_at = NOW()
       WHERE run_id = $1`,
      [runId, config.ingestion.maxSegmentRetries]
    );
  }

  async failSegment(
    runId: string,
    segmentId: string,
    errorMessage: string,
    lastCursor: string | null
  ): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_segments
       SET status = 'failed',
           retry_count = retry_count + 1,
           error_message = LEFT($1, 1000),
           last_cursor = COALESCE($2, last_cursor),
           updated_at = NOW()
       WHERE run_id = $3 AND segment_id = $4`,
      [errorMessage, lastCursor, runId, segmentId]
    );
  }

  async updateSegmentCursor(
    runId: string,
    segmentId: string,
    cursor: string,
    eventsFetched: number
  ): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_segments
       SET last_cursor = $1, events_fetched = $2, updated_at = NOW()
       WHERE run_id = $3 AND segment_id = $4`,
      [cursor, eventsFetched, runId, segmentId]
    );
  }

  async updateRunRps(runId: string, eventsPerSec: number): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_runs
       SET events_per_sec = $1, updated_at = NOW()
       WHERE run_id = $2`,
      [eventsPerSec, runId]
    );
  }

  async getProgress(runId: string): Promise<RunProgress> {
    const result = await getPool().query(
      `SELECT
         run_id,
         status,
         total_segments,
         completed_segments,
         failed_segments,
         total_events,
         COALESCE(events_per_sec, 0) AS events_per_sec,
         (SELECT COUNT(*) FROM ingestion_segments s WHERE s.run_id = r.run_id AND s.status = 'pending') AS pending_segments,
         (SELECT COUNT(*) FROM ingestion_segments s WHERE s.run_id = r.run_id AND s.status = 'in_progress') AS in_progress_segments
       FROM ingestion_runs r
       WHERE run_id = $1`,
      [runId]
    );

    if (result.rows.length === 0) {
      return {
        runId,
        status: 'not_found',
        totalSegments: 0,
        completedSegments: 0,
        failedSegments: 0,
        pendingSegments: 0,
        inProgressSegments: 0,
        totalEvents: 0,
        eventsPerSec: 0,
      };
    }

    const row = result.rows[0];
    return {
      runId,
      status: String(row.status),
      totalSegments: Number(row.total_segments ?? 0),
      completedSegments: Number(row.completed_segments ?? 0),
      failedSegments: Number(row.failed_segments ?? 0),
      pendingSegments: Number(row.pending_segments ?? 0),
      inProgressSegments: Number(row.in_progress_segments ?? 0),
      totalEvents: Number(row.total_events ?? 0),
      eventsPerSec: Number(row.events_per_sec ?? 0),
    };
  }

  async evaluateRun(runId: string, maxRetries: number): Promise<RunEvaluation> {
    const result = await getPool().query(
      `SELECT
         COUNT(*) FILTER (WHERE status = 'pending') AS pending,
         COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress,
         COUNT(*) FILTER (WHERE status = 'failed' AND retry_count < $2) AS retryable_failed,
         COUNT(*) FILTER (WHERE status = 'failed' AND retry_count >= $2) AS exhausted_failed
       FROM ingestion_segments
       WHERE run_id = $1`,
      [runId, maxRetries]
    );

    const row = result.rows[0];
    const pending = Number(row.pending ?? 0);
    const inProgress = Number(row.in_progress ?? 0);
    const retryableFailed = Number(row.retryable_failed ?? 0);
    const exhaustedFailed = Number(row.exhausted_failed ?? 0);

    const done = pending === 0 && inProgress === 0 && retryableFailed === 0;
    return {
      done,
      hasFailures: exhaustedFailed > 0,
      retryableFailed,
      exhaustedFailed,
      pending,
      inProgress,
    };
  }

  async markRunCompleted(runId: string): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_runs
       SET status = 'completed', completed_at = NOW(), updated_at = NOW()
       WHERE run_id = $1`,
      [runId]
    );
  }

  async markRunFailed(runId: string, errorMessage: string): Promise<void> {
    await getPool().query(
      `UPDATE ingestion_runs
       SET status = 'failed', error_message = LEFT($1, 1000), updated_at = NOW()
       WHERE run_id = $2`,
      [errorMessage, runId]
    );
  }

  async heartbeat(
    runId: string,
    workerId: string,
    status: 'idle' | 'working' | 'error',
    segmentId: string | null,
    eventsProcessed: number
  ): Promise<void> {
    await getPool().query(
      `INSERT INTO worker_heartbeats (worker_id, run_id, status, segment_id, events_processed, last_heartbeat)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT (worker_id)
       DO UPDATE SET
         run_id = EXCLUDED.run_id,
         status = EXCLUDED.status,
         segment_id = EXCLUDED.segment_id,
         events_processed = EXCLUDED.events_processed,
         last_heartbeat = NOW()`,
      [workerId, runId, status, segmentId, eventsProcessed]
    );
  }

  async getWorkerHealth(runId: string): Promise<{
    totalWorkers: number;
    activeWorkers: number;
    staleWorkers: number;
    latestHeartbeatSeconds: number | null;
  }> {
    const result = await getPool().query(
      `SELECT
         COUNT(*) AS total_workers,
         COUNT(*) FILTER (WHERE status = 'working') AS active_workers,
         COUNT(*) FILTER (WHERE last_heartbeat < NOW() - INTERVAL '30 seconds') AS stale_workers,
         MAX(EXTRACT(EPOCH FROM (NOW() - last_heartbeat))) AS latest_heartbeat_seconds
       FROM worker_heartbeats
       WHERE run_id = $1`,
      [runId]
    );

    const row = result.rows[0];
    return {
      totalWorkers: Number(row.total_workers ?? 0),
      activeWorkers: Number(row.active_workers ?? 0),
      staleWorkers: Number(row.stale_workers ?? 0),
      latestHeartbeatSeconds:
        row.latest_heartbeat_seconds === null ? null : Number(row.latest_heartbeat_seconds),
    };
  }
}
