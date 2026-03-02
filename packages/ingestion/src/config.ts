function readString(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (value === undefined || value === '') {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function readNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric env var ${name}: ${raw}`);
  }
  return parsed;
}

function readOptionalString(name: string): string | null {
  const value = process.env[name];
  if (value === undefined || value === '') return null;
  return value;
}

function clampParallelism(value: number): number {
  return Math.min(2, Math.max(1, Math.floor(value)));
}

export const config = {
  db: {
    url: readString('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/ingestion'),
  },
  temporal: {
    address: readString('TEMPORAL_ADDRESS', 'temporal:7233'),
    namespace: readString('TEMPORAL_NAMESPACE', 'default'),
    taskQueue: readString('TEMPORAL_TASK_QUEUE', 'datasync-ingestion'),
  },
  api: {
    baseUrl: readString(
      'API_BASE_URL',
      'http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1'
    ),
    apiKey: readString('TARGET_API_KEY', ''),
    pageSize: readNumber('API_PAGE_SIZE', 5000),
    rateLimitRequests: readNumber('API_RATE_LIMIT_REQUESTS', 5),
    rateLimitWindowSeconds: readNumber('API_RATE_LIMIT_WINDOW_SECONDS', 10),
    minTimeMs: readNumber('API_MIN_TIME_MS', 1500),
    maxRetries: readNumber('API_MAX_RETRIES', 6),
    requestTimeoutMs: readNumber('API_TIMEOUT_MS', 30000),
  },
  ingestion: {
    runId: readString('INGESTION_RUN_ID', 'datasync-ingestion-main'),
    workflowId: readString('INGESTION_WORKFLOW_ID', 'datasync-ingestion-workflow'),
    activityParallelism: clampParallelism(readNumber('INGESTION_ACTIVITY_PARALLELISM', 2)),
    workerConcurrency: clampParallelism(readNumber('INGESTION_WORKER_CONCURRENCY', 2)),
    maxSegmentRetries: readNumber('INGESTION_MAX_SEGMENT_RETRIES', 8),
    staleSegmentSeconds: readNumber('INGESTION_STALE_SEGMENT_SECONDS', 180),
    expectedEvents: readNumber('INGESTION_EXPECTED_EVENTS', 3000000),
    cursorShards: Math.max(1, Math.floor(readNumber('INGESTION_CURSOR_SHARDS', 2))),
    cursorWindowStart: readOptionalString('INGESTION_CURSOR_WINDOW_START'),
    cursorWindowEnd: readOptionalString('INGESTION_CURSOR_WINDOW_END'),
    testMaxApiCalls: readNumber('INGESTION_TEST_MAX_API_CALLS', 0),
    progressLogIntervalMs: readNumber('INGESTION_PROGRESS_LOG_INTERVAL_MS', 5000),
  },
};
