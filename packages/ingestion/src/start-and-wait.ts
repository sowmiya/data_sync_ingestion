import {
  Client,
  Connection,
  WorkflowExecutionAlreadyStartedError,
} from '@temporalio/client';
import { config } from './config';
import { logger } from './logger';
import { runMigrations } from './migrate';
import { RunRepository } from './repositories/run-repository';
import { sleep } from './db';
import {
  ingestionWorkflow,
  IngestionWorkflowInput,
  IngestionWorkflowResult,
} from './workflows';

async function startOrAttach(
  client: Client,
  input: IngestionWorkflowInput
): Promise<ReturnType<Client['workflow']['getHandle']>> {
  try {
    const handle = await client.workflow.start(ingestionWorkflow, {
      taskQueue: config.temporal.taskQueue,
      workflowId: config.ingestion.workflowId,
      args: [input],
    });
    logger.info('Workflow started', {
      workflowId: config.ingestion.workflowId,
      runId: handle.firstExecutionRunId,
      ingestionRunId: input.runId,
    });
    return handle;
  } catch (error) {
    if (error instanceof WorkflowExecutionAlreadyStartedError) {
      logger.warn('Workflow already running, attaching to existing execution', {
        workflowId: config.ingestion.workflowId,
      });
      return client.workflow.getHandle(config.ingestion.workflowId);
    }
    throw error;
  }
}

async function monitorProgress(runId: string): Promise<() => void> {
  const runs = new RunRepository();
  let active = true;

  const loop = async () => {
    while (active) {
      try {
        const progress = await runs.getProgress(runId);
        const health = await runs.getWorkerHealth(runId);
        logger.info('ingestion progress', {
          runId,
          status: progress.status,
          totalEvents: progress.totalEvents,
          segments: {
            total: progress.totalSegments,
            completed: progress.completedSegments,
            pending: progress.pendingSegments,
            inProgress: progress.inProgressSegments,
            failed: progress.failedSegments,
          },
          eventsPerSec: Number(progress.eventsPerSec.toFixed(2)),
          workers: health,
        });
      } catch (error) {
        logger.warn('Progress monitor read failed', {
          error: error instanceof Error ? error.message : String(error),
        });
      }

      await sleep(config.ingestion.progressLogIntervalMs);
    }
  };

  void loop();

  return () => {
    active = false;
  };
}

async function run(): Promise<void> {
  await runMigrations();

  const connection = await Connection.connect({
    address: config.temporal.address,
  });

  const client = new Client({
    connection,
    namespace: config.temporal.namespace,
  });

  const input: IngestionWorkflowInput = {
    runId: config.ingestion.runId,
    activityParallelism: config.ingestion.activityParallelism,
  };

  const handle = await startOrAttach(client, input);
  const stopMonitoring = await monitorProgress(config.ingestion.runId);

  try {
    const result = (await handle.result()) as IngestionWorkflowResult;
    if (!result.success) {
      logger.error('ingestion complete with failures', {
        runId: result.runId,
        reason: result.reason,
        totalEvents: result.progress.totalEvents,
      });
      process.exit(1);
    }

    logger.info('ingestion complete', {
      runId: result.runId,
      totalEvents: result.progress.totalEvents,
      eventsPerSec: Number(result.progress.eventsPerSec.toFixed(2)),
    });
  } finally {
    stopMonitoring();
  }
}

run().catch((error: Error) => {
  logger.error('Runner crashed', { error: error.message });
  process.exit(1);
});
