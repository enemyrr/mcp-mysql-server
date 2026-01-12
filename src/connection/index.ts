import type { Pool } from 'mysql2/promise';
import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import type { ConnectionConfig, ConnectionArgs } from '../types/index.js';
import { loadConfig } from './config.js';
import { createPool, testConnection, executeQuery, closePool } from './pool.js';
import { handleDatabaseError } from '../utils/errors.js';
import { logger } from '../utils/logger.js';

export class ConnectionManager {
  private pool: Pool | null = null;
  private config: ConnectionConfig | null = null;

  getConfig(): ConnectionConfig | null {
    return this.config;
  }

  getDatabaseName(): string | null {
    return this.config?.database ?? null;
  }

  isConnected(): boolean {
    return this.pool !== null && this.config !== null;
  }

  async connect(args: ConnectionArgs): Promise<{ host: string; database: string; port?: number }> {
    this.config = await loadConfig(args);
    logger.info('Configuration loaded', {
      host: this.config.host,
      database: this.config.database,
    });

    try {
      this.pool = createPool(this.config);
      await testConnection(this.pool);

      logger.info('Successfully connected to database', {
        host: this.config.host,
        port: this.config.port,
        database: this.config.database,
      });

      return {
        host: this.config.host,
        database: this.config.database,
        port: this.config.port,
      };
    } catch (error) {
      if (this.pool) {
        await this.pool.end().catch(() => {});
        this.pool = null;
      }
      this.config = null;
      handleDatabaseError(error);
    }
  }

  private ensureConnected(): Pool {
    if (!this.config) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Not connected to a database. Use connect_db tool first.'
      );
    }

    if (!this.pool) {
      throw new McpError(ErrorCode.InternalError, 'Connection pool not initialized');
    }

    return this.pool;
  }

  async query<T>(sql: string, params: (string | number | boolean | null)[] = []): Promise<T> {
    const pool = this.ensureConnected();
    return executeQuery<T>(pool, sql, params);
  }

  async close(): Promise<void> {
    if (this.pool) {
      await closePool(this.pool);
      this.pool = null;
    }
    this.config = null;
    logger.info('Database connection closed');
  }

  async reconnect(): Promise<void> {
    if (!this.config) {
      throw new McpError(ErrorCode.InvalidRequest, 'Cannot reconnect: no previous configuration');
    }

    const savedConfig = { ...this.config };
    await this.close();

    this.config = savedConfig;
    this.pool = createPool(this.config);
    await testConnection(this.pool);

    logger.info('Successfully reconnected to database');
  }
}

