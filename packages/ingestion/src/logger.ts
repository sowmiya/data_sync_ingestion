type Level = 'info' | 'warn' | 'error' | 'debug';

function emit(level: Level, message: string, meta?: Record<string, unknown>): void {
  const payload: Record<string, unknown> = {
    ts: new Date().toISOString(),
    level,
    msg: message,
  };
  if (meta && Object.keys(meta).length > 0) {
    payload.meta = meta;
  }
  const line = JSON.stringify(payload);
  if (level === 'error') {
    process.stderr.write(`${line}\n`);
    return;
  }
  process.stdout.write(`${line}\n`);
}

export const logger = {
  info: (message: string, meta?: Record<string, unknown>) => emit('info', message, meta),
  warn: (message: string, meta?: Record<string, unknown>) => emit('warn', message, meta),
  error: (message: string, meta?: Record<string, unknown>) => emit('error', message, meta),
  debug: (message: string, meta?: Record<string, unknown>) => emit('debug', message, meta),
};
