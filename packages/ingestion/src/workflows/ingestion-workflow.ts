import {
  defineQuery,
  proxyActivities,
  setHandler,
  sleep,
} from '@temporalio/workflow';
import type * as activities from '../activities';
import { RunProgress } from '../types';

export interface IngestionWorkflowInput {
  runId: string;
  activityParallelism: number;
}

export interface IngestionWorkflowResult {
  success: boolean;
  runId: string;
  reason?: string;
  progress: RunProgress;
}

export const progressQuery = defineQuery<RunProgress>('progress');

const managementActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    maximumAttempts: 3,
  },
});

const ingestionActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
  retry: {
    maximumAttempts: 1,
  },
});

export async function ingestionWorkflow(
  input: IngestionWorkflowInput
): Promise<IngestionWorkflowResult> {
  const { runId, activityParallelism } = input;
  let progress: RunProgress = {
    runId,
    status: 'starting',
    totalSegments: 0,
    completedSegments: 0,
    failedSegments: 0,
    pendingSegments: 0,
    inProgressSegments: 0,
    totalEvents: 0,
    eventsPerSec: 0,
  };

  setHandler(progressQuery, () => progress);

  await managementActivities.planRun({ runId });

  let previousTotalEvents = 0;
  let previousTs = Date.now();

  while (true) {
    const claimed = await managementActivities.claimSegments({
      runId,
      limit: activityParallelism,
    });

    if (claimed.segments.length > 0) {
      await Promise.all(
        claimed.segments.map((segment) => ingestionActivities.ingestSegment({ runId, segment }))
      );
    } else {
      await sleep('2 seconds');
    }

    progress = await managementActivities.getProgress({ runId });

    const now = Date.now();
    const elapsed = Math.max(1, now - previousTs) / 1000;
    const delta = Math.max(0, progress.totalEvents - previousTotalEvents);
    const rps = delta / elapsed;
    previousTs = now;
    previousTotalEvents = progress.totalEvents;

    await managementActivities.updateRunRps({
      runId,
      eventsPerSec: rps,
    });

    const evaluation = await managementActivities.evaluateRun({ runId });
    if (!evaluation.done) {
      continue;
    }

    if (evaluation.hasFailures) {
      const reason = `exhausted retries on ${evaluation.exhaustedFailed} segments`;
      await managementActivities.markRunFailed({ runId, errorMessage: reason });
      progress = await managementActivities.getProgress({ runId });
      return {
        success: false,
        runId,
        reason,
        progress,
      };
    }

    await managementActivities.markRunCompleted({ runId });
    progress = await managementActivities.getProgress({ runId });
    return {
      success: true,
      runId,
      progress,
    };
  }
}
