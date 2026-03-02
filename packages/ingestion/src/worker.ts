import { NativeConnection, Worker } from '@temporalio/worker';
import { config } from './config';
import { logger } from './logger';
import { runMigrations } from './migrate';
import * as activities from './activities';

async function run(): Promise<void> {
  await runMigrations();

  const connection = await NativeConnection.connect({
    address: config.temporal.address,
  });

  const worker = await Worker.create({
    connection,
    namespace: config.temporal.namespace,
    taskQueue: config.temporal.taskQueue,
    workflowsPath: require.resolve('./workflows'),
    activities,
    maxConcurrentActivityTaskExecutions: config.ingestion.workerConcurrency,
    maxConcurrentWorkflowTaskExecutions: 20,
  });

  logger.info('Temporal worker started', {
    address: config.temporal.address,
    namespace: config.temporal.namespace,
    taskQueue: config.temporal.taskQueue,
    activityConcurrency: config.ingestion.workerConcurrency,
  });

  await worker.run();
}

run().catch((error: Error) => {
  logger.error('Worker crashed', { error: error.message });
  process.exit(1);
});
