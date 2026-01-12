import type { LogLevel, LogEntry } from '../types/index.js';

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

function getCurrentLogLevel(): LogLevel {
  const envLevel = process.env.LOG_LEVEL?.toLowerCase() as LogLevel | undefined;
  if (envLevel && LOG_LEVELS[envLevel] !== undefined) {
    return envLevel;
  }
  return 'info';
}

function formatLogEntry(entry: LogEntry): string {
  const { level, message, timestamp, data } = entry;
  const prefix = `[${timestamp}] [${level.toUpperCase()}]`;

  if (data && Object.keys(data).length > 0) {
    return `${prefix} ${message} ${JSON.stringify(data)}`;
  }
  return `${prefix} ${message}`;
}

function writeLog(level: LogLevel, message: string, data?: Record<string, unknown>): void {
  const currentLevel = getCurrentLogLevel();
  if (LOG_LEVELS[level] < LOG_LEVELS[currentLevel]) {
    return;
  }

  const entry: LogEntry = {
    level,
    message,
    timestamp: new Date().toISOString(),
    data,
  };

  console.error(formatLogEntry(entry));
}

export const logger = {
  debug(message: string, data?: Record<string, unknown>): void {
    writeLog('debug', message, data);
  },

  info(message: string, data?: Record<string, unknown>): void {
    writeLog('info', message, data);
  },

  warn(message: string, data?: Record<string, unknown>): void {
    writeLog('warn', message, data);
  },

  error(message: string, data?: Record<string, unknown>): void {
    writeLog('error', message, data);
  },
};

export function createLogger(prefix: string) {
  return {
    debug(message: string, data?: Record<string, unknown>): void {
      logger.debug(`[${prefix}] ${message}`, data);
    },
    info(message: string, data?: Record<string, unknown>): void {
      logger.info(`[${prefix}] ${message}`, data);
    },
    warn(message: string, data?: Record<string, unknown>): void {
      logger.warn(`[${prefix}] ${message}`, data);
    },
    error(message: string, data?: Record<string, unknown>): void {
      logger.error(`[${prefix}] ${message}`, data);
    },
  };
}
