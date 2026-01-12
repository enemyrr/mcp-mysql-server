import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import path from 'path';
import type { ConnectionConfig, ConnectionArgs } from '../types/index.js';
import { logger } from '../utils/logger.js';

export function parseConnectionUrl(url: string): ConnectionConfig {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid connection URL format'
    );
  }

  if (!parsed.hostname) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid connection URL: host is required'
    );
  }

  if (!parsed.username) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid connection URL: username is required'
    );
  }

  const database = parsed.pathname?.slice(1);
  if (!database) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid connection URL: database name is required in path'
    );
  }

  return {
    host: parsed.hostname,
    port: parsed.port ? parseInt(parsed.port, 10) : undefined,
    user: decodeURIComponent(parsed.username),
    password: decodeURIComponent(parsed.password || ''),
    database,
    ssl: parsed.protocol === 'mysqls:' ? { rejectUnauthorized: true } : undefined,
  };
}

export async function loadWorkspaceConfig(workspace: string): Promise<ConnectionConfig | null> {
  try {
    const fs = await import('fs');
    const dotenv = await import('dotenv');

    // Resolve workspace path relative to current working directory
    const resolvedWorkspace = path.resolve(process.cwd(), workspace);
    logger.debug('Loading workspace config', { workspace: resolvedWorkspace });

    const envPaths = [
      path.join(resolvedWorkspace, '.env.local'),
      path.join(resolvedWorkspace, '.env'),
    ];

    for (const envPath of envPaths) {
      logger.debug('Checking for env file', { path: envPath });

      if (!fs.existsSync(envPath)) {
        logger.debug('Environment file not found', { path: envPath });
        continue;
      }

      logger.debug('Found environment file', { path: envPath });
      const workspaceEnv = dotenv.config({ path: envPath });

      if (workspaceEnv.error) {
        logger.warn('Error loading environment file', {
          path: envPath,
          error: workspaceEnv.error.message,
        });
        continue;
      }

      const { DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE } =
        workspaceEnv.parsed || {};

      if (DATABASE_URL) {
        logger.info('Using DATABASE_URL from environment file', { path: envPath });
        return parseConnectionUrl(DATABASE_URL);
      }

      if (DB_HOST && DB_USER && DB_PASSWORD && DB_DATABASE) {
        logger.info('Using individual credentials from environment file', { path: envPath });
        return {
          host: DB_HOST,
          port: DB_PORT ? parseInt(DB_PORT, 10) : undefined,
          user: DB_USER,
          password: DB_PASSWORD,
          database: DB_DATABASE,
        };
      }
    }

    logger.debug('No valid database configuration found in workspace', {
      workspace: resolvedWorkspace,
      checkedPaths: envPaths,
    });
    return null;
  } catch (error) {
    logger.error('Error loading workspace config', {
      error: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

export function hasDirectConfig(args: ConnectionArgs): boolean {
  return !!(args.host && args.user && args.password && args.database);
}

export function createDirectConfig(args: ConnectionArgs): ConnectionConfig {
  if (!hasDirectConfig(args)) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Missing required connection parameters (host, user, password, database)'
    );
  }

  return {
    host: args.host!,
    port: args.port,
    user: args.user!,
    password: args.password!,
    database: args.database!,
  };
}

export async function loadConfig(args: ConnectionArgs): Promise<ConnectionConfig> {
  logger.debug('Loading config', { hasUrl: !!args.url, hasWorkspace: !!args.workspace });

  // Try URL first
  if (args.url) {
    logger.info('Using URL configuration');
    return parseConnectionUrl(args.url);
  }

  // Try workspace config
  if (args.workspace) {
    logger.info('Attempting workspace configuration', { workspace: args.workspace });
    const config = await loadWorkspaceConfig(args.workspace);
    if (config) {
      logger.info('Successfully loaded workspace config');
      return config;
    }
    logger.warn('Failed to load workspace config, trying fallback');
  }

  // Try direct parameters
  if (hasDirectConfig(args)) {
    logger.info('Using direct configuration parameters');
    return createDirectConfig(args);
  }

  throw new McpError(
    ErrorCode.InvalidParams,
    'No valid configuration provided. Please provide either: a URL, workspace path with .env file, or direct connection parameters (host, user, password, database).'
  );
}
