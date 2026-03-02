export type SegmentType = 'page' | 'cursor';

export interface ApiEvent {
  id: string;
  type: string;
  source: string | null;
  tenantId: string | null;
  userId: string | null;
  sessionId: string | null;
  timestamp: unknown;
  properties: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
}

export interface Segment {
  segmentId: string;
  segmentType: SegmentType;
  status: 'pending' | 'in_progress' | 'completed' | 'failed';
  retryCount: number;
  lastCursor: string | null;
}

export interface RunProgress {
  runId: string;
  status: string;
  totalSegments: number;
  completedSegments: number;
  failedSegments: number;
  pendingSegments: number;
  inProgressSegments: number;
  totalEvents: number;
  eventsPerSec: number;
}

export interface RunEvaluation {
  done: boolean;
  hasFailures: boolean;
  retryableFailed: number;
  exhaustedFailed: number;
  pending: number;
  inProgress: number;
}
