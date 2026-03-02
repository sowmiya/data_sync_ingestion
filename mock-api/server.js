const http = require('http');

const PORT = Number(process.env.PORT || 8080);
const TOTAL_EVENTS = Math.max(1, Number(process.env.MOCK_TOTAL_EVENTS || 50000));
const DEFAULT_PAGE_SIZE = Math.max(1, Number(process.env.MOCK_DEFAULT_PAGE_SIZE || 1000));
const RATE_LIMIT_RPS = Math.max(1, Number(process.env.MOCK_RATE_LIMIT_RPS || 200));

let requestsThisSecond = 0;
setInterval(() => {
  requestsThisSecond = 0;
}, 1000);

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parsePositiveInt(raw, fallback) {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.floor(parsed);
}

function createEvent(index) {
  const ts = new Date(Date.UTC(2024, 0, 1) + index * 1000).toISOString();
  return {
    id: `evt-${String(index + 1).padStart(8, '0')}`,
    type: index % 2 === 0 ? 'page_view' : 'button_click',
    source: 'mock-api',
    tenantId: `tenant-${(index % 10) + 1}`,
    userId: `user-${(index % 1000) + 1}`,
    sessionId: `session-${Math.floor(index / 20) + 1}`,
    timestamp: ts,
    properties: {
      index,
      synthetic: true,
    },
    metadata: {
      generator: 'datasync-mock',
    },
  };
}

function json(res, status, body, extraHeaders = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
    ...extraHeaders,
  });
  res.end(payload);
}

function rateHeaders() {
  const remaining = Math.max(0, RATE_LIMIT_RPS - requestsThisSecond);
  return {
    'X-RateLimit-Limit': String(RATE_LIMIT_RPS),
    'X-RateLimit-Remaining': String(remaining),
  };
}

const server = http.createServer((req, res) => {
  try {
    const url = new URL(req.url || '/', `http://${req.headers.host}`);

    if (url.pathname === '/health') {
      return json(res, 200, {
        ok: true,
        totalEvents: TOTAL_EVENTS,
      });
    }

    if (url.pathname !== '/api/v1/events') {
      return json(res, 404, {
        error: 'not_found',
      });
    }

    requestsThisSecond += 1;
    if (requestsThisSecond > RATE_LIMIT_RPS) {
      return json(
        res,
        429,
        { error: 'rate_limited', message: 'Too many requests' },
        {
          ...rateHeaders(),
          'Retry-After': '1',
        }
      );
    }

    const requestedLimit = parsePositiveInt(
      url.searchParams.get('limit') || url.searchParams.get('pageSize'),
      DEFAULT_PAGE_SIZE
    );
    const pageSize = clamp(requestedLimit, 1, 5000);

    const pageParam = url.searchParams.get('page');
    const cursorParam = url.searchParams.get('cursor');

    let startIndex = 0;
    let page = 1;

    if (pageParam) {
      page = parsePositiveInt(pageParam, 1);
      startIndex = (page - 1) * pageSize;
    } else if (cursorParam) {
      startIndex = Math.max(0, Number(cursorParam) || 0);
      page = Math.floor(startIndex / pageSize) + 1;
    }

    startIndex = clamp(startIndex, 0, TOTAL_EVENTS);
    const endIndex = clamp(startIndex + pageSize, 0, TOTAL_EVENTS);

    const events = [];
    for (let i = startIndex; i < endIndex; i += 1) {
      events.push(createEvent(i));
    }

    const totalPages = Math.max(1, Math.ceil(TOTAL_EVENTS / pageSize));
    const hasMore = endIndex < TOTAL_EVENTS;
    const nextCursor = hasMore ? String(endIndex) : null;

    return json(
      res,
      200,
      {
        data: events,
        hasMore,
        nextCursor,
        pagination: {
          page,
          pageSize,
          totalPages,
          totalCount: TOTAL_EVENTS,
          hasMore,
          nextCursor,
        },
      },
      rateHeaders()
    );
  } catch (error) {
    return json(res, 500, {
      error: 'internal_error',
      message: error instanceof Error ? error.message : String(error),
    });
  }
});

server.listen(PORT, () => {
  process.stdout.write(
    `${JSON.stringify({
      ts: new Date().toISOString(),
      level: 'info',
      msg: 'mock api listening',
      meta: { port: PORT, totalEvents: TOTAL_EVENTS, rateLimitRps: RATE_LIMIT_RPS },
    })}\n`
  );
});
