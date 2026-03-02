import { getPool } from '../db';
import { ApiEvent } from '../types';

interface StoredEvent {
  eventId: string;
  eventType: string;
  source: string | null;
  tenantId: string | null;
  userId: string | null;
  sessionId: string | null;
  timestamp: Date;
  properties: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
}

function parseTimestamp(value: unknown): Date {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    const millis = value > 1_000_000_000_000 ? value : value * 1000;
    const date = new Date(millis);
    if (!Number.isNaN(date.getTime())) return date;
  }

  if (typeof value === 'string') {
    const numeric = Number(value);
    if (Number.isFinite(numeric) && value.trim() !== '') {
      const millis = numeric > 1_000_000_000_000 ? numeric : numeric * 1000;
      const fromNumeric = new Date(millis);
      if (!Number.isNaN(fromNumeric.getTime())) return fromNumeric;
    }

    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }

  return new Date(0);
}

function mapEvent(event: ApiEvent): StoredEvent {
  return {
    eventId: event.id,
    eventType: event.type,
    source: event.source,
    tenantId: event.tenantId,
    userId: event.userId,
    sessionId: event.sessionId,
    timestamp: parseTimestamp(event.timestamp),
    properties: event.properties,
    metadata: event.metadata,
  };
}

function buildInsert(events: StoredEvent[]): { text: string; values: unknown[] } {
  const values: unknown[] = [];
  const placeholders: string[] = [];

  events.forEach((event, index) => {
    const offset = index * 9;
    placeholders.push(
      `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9})`
    );
    values.push(
      event.eventId,
      event.eventType,
      event.source,
      event.tenantId,
      event.userId,
      event.sessionId,
      event.timestamp,
      event.properties ? JSON.stringify(event.properties) : null,
      event.metadata ? JSON.stringify(event.metadata) : null
    );
  });

  return {
    text: `
      INSERT INTO events (
        event_id,
        event_type,
        source,
        tenant_id,
        user_id,
        session_id,
        timestamp,
        properties,
        metadata
      )
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (event_id, timestamp) DO NOTHING
    `,
    values,
  };
}

export class EventRepository {
  async storeBatch(events: ApiEvent[]): Promise<number> {
    if (events.length === 0) return 0;

    const mapped = events.map(mapEvent);
    const maxRowsPerStatement = 5000;
    let inserted = 0;

    for (let i = 0; i < mapped.length; i += maxRowsPerStatement) {
      const chunk = mapped.slice(i, i + maxRowsPerStatement);
      const query = buildInsert(chunk);
      const result = await getPool().query(query.text, query.values);
      inserted += result.rowCount ?? 0;
    }

    return inserted;
  }
}
