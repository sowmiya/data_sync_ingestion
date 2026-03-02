import fs from 'fs';
import path from 'path';
import { getPool } from './db';
import { logger } from './logger';

export async function runMigrations(): Promise<void> {
  const sqlPath = path.resolve(__dirname, '../sql/001_init.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await getPool().query(sql);
  logger.info('Migrations applied', { sqlPath });
}
