import { AxiosError } from 'axios';
import { DataSyncClient, FetchCursorResponse } from './api-client';
import { config } from './config';
import { logger } from './logger';
import { EventRepository } from './repositories/event-repository';
import { RunRepository } from './repositories/run-repository';
import { ApiEvent, RunEvaluation, RunProgress, Segment, SegmentType } from './types';

const api = new DataSyncClient();
const runs = new RunRepository();
const events = new EventRepository();
const API_PAGE_LIMIT_MAX = 5000;

export interface PlanRunInput {
  runId: string;
}

export interface PlanRunOutput {
  strategy: string;
  totalSegments: number;
  totalCount: number;
}

export interface ClaimSegmentsInput {
  runId: string;
  limit: number;
}

export interface ClaimSegmentsOutput {
  segments: Segment[];
}

export interface IngestSegmentInput {
  runId: string;
  segment: Segment;
}

export interface IngestSegmentOutput {
  ok: boolean;
  segmentId: string;
  fetched: number;
  inserted: number;
  error?: string;
}

export interface EvaluateRunInput {
  runId: string;
}

export interface ProgressInput {
  runId: string;
}

export interface UpdateRpsInput {
  runId: string;
  eventsPerSec: number;
}

interface CursorSegmentBounds {
  upperMs: number;
  lowerMs: number;
}

function compactCursor(cursor: string | null): string | null {
  if (!cursor) return null;
  if (cursor.length <= 24) return cursor;
  return `${cursor.slice(0, 24)}...(${cursor.length})`;
}

function parseEventTimestampMs(event: ApiEvent): number | null {
  if (typeof event.timestamp === 'number') {
    return Number.isFinite(event.timestamp) ? event.timestamp : null;
  }
  if (typeof event.timestamp === 'string') {
    const parsed = Date.parse(event.timestamp);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function getCursorWindowBounds(): CursorSegmentBounds | null {
  const start = config.ingestion.cursorWindowStart;
  const end = config.ingestion.cursorWindowEnd;
  if (!start || !end) return null;

  const lowerMs = Date.parse(start);
  const upperMs = Date.parse(end);
  if (!Number.isFinite(lowerMs) || !Number.isFinite(upperMs) || lowerMs >= upperMs) {
    throw new Error(
      `Invalid cursor window: INGESTION_CURSOR_WINDOW_START=${start} INGESTION_CURSOR_WINDOW_END=${end}`
    );
  }

  return { upperMs, lowerMs };
}

function buildCursorSegments(
  shardCount: number,
  bounds: CursorSegmentBounds | null
): Array<{ segmentId: string; segmentType: SegmentType }> {
  if (!bounds || shardCount <= 1) {
    return [{ segmentId: 'cursor:root', segmentType: 'cursor' }];
  }

  const span = bounds.upperMs - bounds.lowerMs;
  const segments: Array<{ segmentId: string; segmentType: SegmentType }> = [];
  for (let i = 0; i < shardCount; i += 1) {
    const shardUpper = bounds.upperMs - Math.floor((span * i) / shardCount);
    const shardLower = bounds.upperMs - Math.floor((span * (i + 1)) / shardCount);
    if (shardUpper <= shardLower) continue;
    segments.push({
      segmentId: `cursor:${shardUpper}:${shardLower}`,
      segmentType: 'cursor',
    });
  }

  if (segments.length === 0) {
    return [{ segmentId: 'cursor:root', segmentType: 'cursor' }];
  }

  return segments;
}

function parseCursorSegmentBounds(segmentId: string): CursorSegmentBounds | null {
  const parts = segmentId.split(':');
  if (parts.length !== 3 || parts[0] !== 'cursor') return null;

  const upperMs = Number(parts[1]);
  const lowerMs = Number(parts[2]);
  if (!Number.isFinite(upperMs) || !Number.isFinite(lowerMs) || upperMs <= lowerMs) {
    return null;
  }

  return { upperMs, lowerMs };
}

function buildSegments(
  strategy: string,
  totalCount: number,
  pageSize: number
): Array<{ segmentId: string; segmentType: SegmentType }> {
  if (strategy === 'cursor') {
    return buildCursorSegments(config.ingestion.cursorShards, getCursorWindowBounds());
  }

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  return Array.from({ length: totalPages }, (_, index) => ({
    segmentId: `page:${index + 1}`,
    segmentType: 'page',
  }));
}

function getWorkerId(): string {
  const host = process.env.HOSTNAME ?? 'worker';
  // Stable per worker process; do not use activityId or worker counts will be inflated.
  return `${host}:${process.pid}`;
}

export async function planRun(input: PlanRunInput): Promise<PlanRunOutput> {
  const capabilities = api.getCapabilities();
  const strategy = capabilities.supportsPagination ? 'paginated' : 'cursor';
  const pageSize = Math.min(API_PAGE_LIMIT_MAX, capabilities.maxPageSize, config.api.pageSize);
  const totalCount = capabilities.totalCount;
  const segments = buildSegments(strategy, totalCount, pageSize);
  if (strategy === 'cursor') {
    logger.info('Cursor strategy planned', {
      runId: input.runId,
      shardCount: segments.length,
      windowStart: config.ingestion.cursorWindowStart,
      windowEnd: config.ingestion.cursorWindowEnd,
    });
  }

  const run = await runs.initRun(input.runId, strategy, segments.length);
  if (run.isNew) {
    await runs.seedSegments(input.runId, segments);
  }

  return {
    strategy,
    totalSegments: segments.length,
    totalCount,
  };
}

export async function claimSegments(input: ClaimSegmentsInput): Promise<ClaimSegmentsOutput> {
  const workerId = getWorkerId();
  const segments = await runs.claimSegments(
    input.runId,
    workerId,
    input.limit,
    config.ingestion.staleSegmentSeconds,
    config.ingestion.maxSegmentRetries
  );

  return { segments };
}

export async function ingestSegment(input: IngestSegmentInput): Promise<IngestSegmentOutput> {
  const { runId, segment } = input;
  const workerId = getWorkerId();
  const maxApiCalls = Math.max(0, Math.floor(config.ingestion.testMaxApiCalls));
  let apiCalls = 0;
  let fetched = 0;
  let inserted = 0;
  let lastCursor = segment.lastCursor;

  await runs.heartbeat(runId, workerId, 'working', segment.segmentId, 0);

  try {
    if (segment.segmentType === 'page') {
      if (maxApiCalls > 0 && apiCalls >= maxApiCalls) {
        await runs.completeSegment(runId, segment.segmentId, fetched, inserted);
        await runs.heartbeat(runId, workerId, 'idle', null, inserted);
        return {
          ok: true,
          segmentId: segment.segmentId,
          fetched,
          inserted,
        };
      }
      const page = Number(segment.segmentId.split(':')[1]);
      const response = await api.fetchPage(page, config.api.pageSize);
      apiCalls += 1;
      fetched += response.events.length;
      inserted += await events.storeBatch(response.events);
    } else {
      const bounds = parseCursorSegmentBounds(segment.segmentId);
      let cursor = segment.lastCursor;
      let usingCraftedStartCursor = false;
      if (!cursor && bounds) {
        cursor = api.buildCursorFromTimestamp(new Date(bounds.upperMs).toISOString());
        usingCraftedStartCursor = true;
      }

      while (true) {
        if (maxApiCalls > 0 && apiCalls >= maxApiCalls) {
          break;
        }
        const requestCursor = cursor;
        let response: FetchCursorResponse;
        try {
          response = await api.fetchCursor(cursor, config.api.pageSize);
        } catch (error) {
          const status = error instanceof AxiosError ? error.response?.status : undefined;
          if (status === 400 && usingCraftedStartCursor && !segment.lastCursor && bounds) {
            logger.warn('Crafted cursor rejected by API; retrying shard from root cursor', {
              runId,
              segmentId: segment.segmentId,
            });
            cursor = null;
            lastCursor = null;
            usingCraftedStartCursor = false;
            continue;
          }
          throw error;
        }

        apiCalls += 1;

        if (response.events.length === 0) {
          break;
        }

        let eventsToStore = response.events;
        let reachedLowerBound = false;
        if (bounds) {
          eventsToStore = response.events.filter((event) => {
            const ts = parseEventTimestampMs(event);
            if (ts === null) return true;
            if (ts > bounds.upperMs) {
              return false;
            }
            if (ts < bounds.lowerMs) {
              reachedLowerBound = true;
              return false;
            }
            return true;
          });
        }

        fetched += response.events.length;
        const beforeInserted = inserted;
        if (eventsToStore.length > 0) {
          inserted += await events.storeBatch(eventsToStore);
        }
        const insertedBatch = inserted - beforeInserted;

        if (response.nextCursor) {
          cursor = response.nextCursor;
          lastCursor = response.nextCursor;
          await runs.updateSegmentCursor(runId, segment.segmentId, cursor, fetched);
        }

        await runs.heartbeat(runId, workerId, 'working', segment.segmentId, inserted);

        logger.debug('Cursor segment progress', {
          runId,
          segmentId: segment.segmentId,
          requestCursor: compactCursor(requestCursor),
          nextCursor: compactCursor(response.nextCursor),
          hasMore: response.hasMore,
          fetchedBatch: response.events.length,
          insertedBatch,
          totalFetched: fetched,
          totalInserted: inserted,
          reachedLowerBound,
        });

        if (!response.hasMore || !response.nextCursor || reachedLowerBound) {
          break;
        }
      }
    }

    await runs.completeSegment(runId, segment.segmentId, fetched, inserted);
    await runs.heartbeat(runId, workerId, 'idle', null, inserted);

    return {
      ok: true,
      segmentId: segment.segmentId,
      fetched,
      inserted,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await runs.failSegment(runId, segment.segmentId, message, lastCursor);
    await runs.heartbeat(runId, workerId, 'error', segment.segmentId, inserted);

    return {
      ok: false,
      segmentId: segment.segmentId,
      fetched,
      inserted,
      error: message,
    };
  }
}

export async function getProgress(input: ProgressInput): Promise<RunProgress> {
  return runs.getProgress(input.runId);
}

export async function evaluateRun(input: EvaluateRunInput): Promise<RunEvaluation> {
  return runs.evaluateRun(input.runId, config.ingestion.maxSegmentRetries);
}

export async function updateRunRps(input: UpdateRpsInput): Promise<void> {
  await runs.updateRunRps(input.runId, input.eventsPerSec);
}

export async function markRunCompleted(input: { runId: string }): Promise<void> {
  await runs.markRunCompleted(input.runId);
}

export async function markRunFailed(input: { runId: string; errorMessage: string }): Promise<void> {
  await runs.markRunFailed(input.runId, input.errorMessage);
}
