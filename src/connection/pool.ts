import * as mysql from 'mysql2/promise';
import type { ConnectionConfig } from '../types/index.js';
import { handleDatabaseError } from '../utils/errors.js';
import { logger } from '../utils/logger.js';

const DEFAULT_POOL_OPTIONS = {
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  supportBigNumbers: true,
  bigNumberStrings: true,
};

export function createPool(config: ConnectionConfig): mysql.Pool {
  logger.debug('Creating connection pool', {
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
  });

  return mysql.createPool({
    ...config,
    ...DEFAULT_POOL_OPTIONS,
  });
}

export async function testConnection(pool: mysql.Pool): Promise<boolean> {
  try {
    const connection = await pool.getConnection();
    connection.release();
    logger.info('Connection test successful');
    return true;
  } catch (error) {
    logger.error('Connection test failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export async function executeQuery<T>(
  pool: mysql.Pool,
  sql: string,
  params: (string | number | boolean | null)[] = []
): Promise<T> {
  try {
    const [result] = await pool.query(sql, params);
    return result as T;
  } catch (error) {
    handleDatabaseError(error);
  }
}

export async function closePool(pool: mysql.Pool): Promise<void> {
  logger.info('Closing connection pool');
  await pool.end();
}
