import axios, { AxiosError, AxiosInstance } from 'axios';
import axiosRetry from 'axios-retry';
import Bottleneck from 'bottleneck';
import { config } from './config';
import { logger } from './logger';
import { ApiEvent } from './types';

const API_PAGE_LIMIT_MAX = 5000;

export interface ApiCapabilities {
  supportsPagination: boolean;
  supportsCursor: boolean;
  totalCount: number;
  maxPageSize: number;
}

export interface FetchPageResponse {
  events: ApiEvent[];
  totalCount: number;
  totalPages: number;
  page: number;
  hasMore: boolean;
  nextCursor: string | null;
}

export interface FetchCursorResponse {
  events: ApiEvent[];
  hasMore: boolean;
  nextCursor: string | null;
  totalCount?: number;
}

let limiter: Bottleneck | undefined;
let cooldownUntilMs = 0;
const RATE_LIMIT_HEADROOM = 1;
const RATE_LIMIT_RESET_BUFFER_MS = 300;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCooldown(): Promise<void> {
  const waitMs = cooldownUntilMs - Date.now();
  if (waitMs > 0) {
    await sleep(waitMs);
  }
}

function parseHeaderNumber(value: string | string[] | undefined): number | null {
  const raw = Array.isArray(value) ? value[0] : value;
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function getLimiter(): Bottleneck {
  if (!limiter) {
    const windowMs = Math.max(1000, Math.floor(config.api.rateLimitWindowSeconds * 1000));
    const requestsPerWindow = Math.max(1, Math.floor(config.api.rateLimitRequests) - RATE_LIMIT_HEADROOM);
    limiter = new Bottleneck({
      reservoir: requestsPerWindow,
      reservoirRefreshAmount: requestsPerWindow,
      reservoirRefreshInterval: windowMs,
      minTime: Math.max(config.api.minTimeMs, Math.ceil(windowMs / requestsPerWindow)),
      // Sliding-window APIs are sensitive to bursts. Keep calls serialized.
      maxConcurrent: 1,
    });
  }
  return limiter;
}

function applyRateLimitHeaders(headers: Record<string, unknown>): void {
  if (!limiter) return;

  const limit = parseHeaderNumber(headers['x-ratelimit-limit'] as string | string[] | undefined);
  const remaining = parseHeaderNumber(
    headers['x-ratelimit-remaining'] as string | string[] | undefined
  );
  const resetSeconds = parseHeaderNumber(headers['x-ratelimit-reset'] as string | string[] | undefined);

  if (limit && limit > 0) {
    const windowMs =
      resetSeconds && resetSeconds > 0
        ? Math.max(1000, Math.ceil(resetSeconds * 1000) + RATE_LIMIT_RESET_BUFFER_MS)
        : Math.max(1000, Math.floor(config.api.rateLimitWindowSeconds * 1000));
    const effectiveLimit = Math.max(1, Math.floor(limit) - RATE_LIMIT_HEADROOM);
    const effectiveRemaining =
      remaining === null ? null : Math.max(0, Math.floor(remaining) - RATE_LIMIT_HEADROOM);
    const minTimeFromReset =
      effectiveRemaining !== null && effectiveRemaining > 0
        ? Math.ceil(windowMs / effectiveRemaining)
        : Math.ceil(windowMs / effectiveLimit);

    const nextSettings: {
      reservoirRefreshAmount: number;
      reservoirRefreshInterval: number;
      minTime: number;
      reservoir?: number;
    } = {
      reservoirRefreshAmount: effectiveLimit,
      reservoirRefreshInterval: windowMs,
      minTime: Math.max(config.api.minTimeMs, minTimeFromReset),
    };

    if (effectiveRemaining !== null) {
      nextSettings.reservoir = effectiveRemaining;
    }

    void limiter.updateSettings(nextSettings);
  }

  if (remaining !== null && remaining <= 1 && resetSeconds && resetSeconds > 0) {
    cooldownUntilMs = Math.max(
      cooldownUntilMs,
      Date.now() + Math.ceil(resetSeconds * 1000) + RATE_LIMIT_RESET_BUFFER_MS
    );
    logger.warn('Rate limit headroom low; applying cooldown', {
      limit,
      remaining,
      resetSeconds,
    });
  }
}

function buildClient(): AxiosInstance {
  const client = axios.create({
    baseURL: config.api.baseUrl,
    timeout: config.api.requestTimeoutMs,
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'User-Agent': 'datasync-temporal-ingestion/1.0',
      ...(config.api.apiKey ? { 'X-API-Key': config.api.apiKey, Authorization: `Bearer ${config.api.apiKey}` } : {}),
    },
  });

  axiosRetry(client, {
    retries: config.api.maxRetries,
    retryCondition: (error: AxiosError) => {
      const status = error.response?.status;
      if (!status) return true;
      return status === 429 || status >= 500;
    },
    retryDelay: (attempt: number, error: AxiosError) => {
      let delayMs: number;
      const retryAfter = error.response?.headers?.['retry-after'];
      if (retryAfter) {
        const seconds = Number(retryAfter);
        if (Number.isFinite(seconds)) {
          delayMs = Math.max(1000, seconds * 1000 + RATE_LIMIT_RESET_BUFFER_MS);
          logger.warn('API retry scheduled', {
            attempt,
            status: error.response?.status,
            method: error.config?.method,
            url: error.config?.url,
            delayMs,
            reason: 'retry-after',
          });
          return delayMs;
        }
      }
      const reset = error.response?.headers?.['x-ratelimit-reset'];
      if (reset) {
        const seconds = Number(Array.isArray(reset) ? reset[0] : reset);
        if (Number.isFinite(seconds) && seconds > 0) {
          delayMs = Math.max(1000, seconds * 1000 + RATE_LIMIT_RESET_BUFFER_MS);
          logger.warn('API retry scheduled', {
            attempt,
            status: error.response?.status,
            method: error.config?.method,
            url: error.config?.url,
            delayMs,
            reason: 'x-ratelimit-reset',
          });
          return delayMs;
        }
      }
      delayMs = axiosRetry.exponentialDelay(attempt);
      logger.warn('API retry scheduled', {
        attempt,
        status: error.response?.status,
        method: error.config?.method,
        url: error.config?.url,
        delayMs,
        reason: 'exponential-backoff',
      });
      return delayMs;
    },
    onRetry: (attempt: number, error: AxiosError) => {
      logger.warn('API retry started', {
        attempt,
        status: error.response?.status,
        method: error.config?.method,
        url: error.config?.url,
        message: error.message,
      });
    },
  });

  client.interceptors.response.use((response) => {
    applyRateLimitHeaders(response.headers as Record<string, unknown>);
    return response;
  }, (error: AxiosError) => {
    if (error.response?.headers) {
      applyRateLimitHeaders(error.response.headers as Record<string, unknown>);
    }
    if (error.response?.status === 429) {
      const retryAfter = parseHeaderNumber(
        error.response.headers?.['retry-after'] as string | string[] | undefined
      );
      const reset = parseHeaderNumber(
        error.response.headers?.['x-ratelimit-reset'] as string | string[] | undefined
      );
      const waitSeconds = Math.max(1, retryAfter ?? reset ?? config.api.rateLimitWindowSeconds);
      cooldownUntilMs = Math.max(
        cooldownUntilMs,
        Date.now() + Math.ceil(waitSeconds * 1000) + RATE_LIMIT_RESET_BUFFER_MS
      );
      logger.warn('Received 429; forcing cooldown', {
        waitSeconds,
      });
    }
    return Promise.reject(error);
  });

  return client;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
}

function normalizeEvent(raw: unknown): ApiEvent {
  const row = asRecord(raw);
  const metadata = asRecord(row.metadata);
  const properties = asRecord(row.properties);

  return {
    id: String(row.id ?? row.eventId ?? row.event_id ?? ''),
    type: String(row.type ?? row.eventType ?? row.event_type ?? 'unknown'),
    source: row.source ? String(row.source) : null,
    tenantId: row.tenantId ? String(row.tenantId) : row.tenant_id ? String(row.tenant_id) : null,
    userId: row.userId ? String(row.userId) : row.user_id ? String(row.user_id) : null,
    sessionId: row.sessionId ? String(row.sessionId) : row.session_id ? String(row.session_id) : null,
    timestamp: row.timestamp ?? row.occurredAt ?? row.createdAt ?? row.created_at,
    properties: Object.keys(properties).length > 0 ? properties : null,
    metadata: Object.keys(metadata).length > 0 ? metadata : null,
  };
}

function extractEvents(data: unknown): ApiEvent[] {
  if (Array.isArray(data)) {
    return data.map(normalizeEvent).filter((item) => item.id !== '');
  }
  const root = asRecord(data);
  const payload = root.data ?? root.events ?? root.items ?? [];
  if (!Array.isArray(payload)) return [];
  return payload.map(normalizeEvent).filter((item) => item.id !== '');
}

function normalizePageSize(pageSize: number): number {
  if (!Number.isFinite(pageSize) || pageSize < 1) {
    return API_PAGE_LIMIT_MAX;
  }
  return Math.min(API_PAGE_LIMIT_MAX, Math.floor(pageSize));
}

export class DataSyncClient {
  private readonly http: AxiosInstance;
  private readonly rateLimiter: Bottleneck;

  constructor() {
    this.http = buildClient();
    this.rateLimiter = getLimiter();
  }

  getCapabilities(): ApiCapabilities {
    // return this.rateLimiter.schedule(async () => {
    //   const response = await this.http.get('/events', {
    //     params: { page: 1, pageSize: 1, limit: 1 },
    //   });

    //   const root = asRecord(response.data);
    //   const pagination = asRecord(root.pagination ?? root.meta);
    //   const totalCount = Number(
    //     pagination.totalCount ?? pagination.total ?? root.totalCount ?? config.ingestion.expectedEvents
    //   );
    //   const totalPages = Number(pagination.totalPages ?? pagination.pages ?? 0);

    //   const nextCursor =
    //     (root.nextCursor as string | undefined) ??
    //     (pagination.nextCursor as string | undefined) ??
    //     (pagination.cursor as string | undefined);

      return {
        supportsPagination: false,
        supportsCursor: true,
        totalCount: config.ingestion.expectedEvents,
        maxPageSize: API_PAGE_LIMIT_MAX,
      };
   // });
  }

  async fetchPage(page: number, pageSize: number): Promise<FetchPageResponse> {
    return this.rateLimiter.schedule(async () => {
      await waitForCooldown();
      const effectivePageSize = normalizePageSize(pageSize);
      const response = await this.http.get('/events', {
        params: { page, pageSize: effectivePageSize, limit: effectivePageSize },
      });
      const root = asRecord(response.data);
      const pagination = asRecord(root.pagination ?? root.meta);
      const events = extractEvents(response.data);
      const totalCount = Number(
        pagination.totalCount ?? pagination.total ?? root.totalCount ?? config.ingestion.expectedEvents
      );
      const totalPages = Number(
        pagination.totalPages ?? pagination.pages ?? Math.ceil(totalCount / effectivePageSize)
      );
      const nextCursor =
        (root.nextCursor as string | undefined) ??
        (pagination.nextCursor as string | undefined) ??
        null;
      const hasMore =
        typeof root.hasMore === 'boolean'
          ? Boolean(root.hasMore)
          : page < totalPages || (nextCursor !== null && nextCursor !== undefined);

      logger.debug('DataSync API response', {
        endpoint: '/events',
        mode: 'page',
        status: response.status,
        page,
        pageSize: effectivePageSize,
        nextCursor: nextCursor
      });

      return {
        events,
        totalCount: Number.isFinite(totalCount) && totalCount > 0 ? totalCount : config.ingestion.expectedEvents,
        totalPages: Number.isFinite(totalPages) && totalPages > 0 ? totalPages : page,
        page,
        hasMore,
        nextCursor,
      };
    });
  }

  async fetchCursor(cursor: string | null, pageSize: number): Promise<FetchCursorResponse> {
    return this.rateLimiter.schedule(async () => {
      await waitForCooldown();
      const effectivePageSize = normalizePageSize(pageSize);
      const params: Record<string, string | number> = {
        limit: effectivePageSize,
        pageSize: effectivePageSize,
      };
      if (cursor) {
        params.cursor = cursor;
      }
      const response = await this.http.get('/events', { params });
      const root = asRecord(response.data);
      const pagination = asRecord(root.pagination ?? root.meta);

      const nextCursor =
        (root.nextCursor as string | undefined) ??
        (root.next_cursor as string | undefined) ??
        (pagination.nextCursor as string | undefined) ??
        null;

      const hasMore =
        typeof root.hasMore === 'boolean'
          ? Boolean(root.hasMore)
          : typeof nextCursor === 'string' && nextCursor.length > 0;

      const totalCount = Number(pagination.totalCount ?? pagination.total ?? root.totalCount ?? NaN);

      logger.debug('DataSync API response', {
        endpoint: '/events',
        mode: 'cursor',
        status: response.status,
        pageSize: effectivePageSize,
        hasCursor: Boolean(cursor),
      });

      return {
        events: extractEvents(response.data),
        hasMore,
        nextCursor,
        totalCount: Number.isFinite(totalCount) ? totalCount : undefined,
      };
    });
  }

  buildCursorFromTimestamp(timestampIso: string): string {
    const ts = Date.parse(timestampIso);
    if (!Number.isFinite(ts)) {
      throw new Error(`Invalid cursor timestamp: ${timestampIso}`);
    }

    const payload = JSON.stringify({ ts });
    return Buffer.from(payload, 'utf8').toString('base64');
  }
}
