import { runMigrations } from './migrate';
import { logger } from './logger';

runMigrations()
  .then(() => {
    logger.info('Migration command completed');
    process.exit(0);
  })
  .catch((error: Error) => {
    logger.error('Migration command failed', { error: error.message });
    process.exit(1);
  });
