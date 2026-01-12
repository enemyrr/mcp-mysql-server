import type { Server } from '@modelcontextprotocol/sdk/server/index.js';
import {
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  ListResourceTemplatesRequestSchema,
  ErrorCode,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import type { ConnectionManager } from './connection/index.js';
import { logger } from './utils/logger.js';

const RESOURCE_TEMPLATES = [
  {
    uriTemplate: 'mysql://tables/{table}',
    name: 'Table Details',
    description: 'Get detailed information about a specific table',
    mimeType: 'application/json',
  },
  {
    uriTemplate: 'mysql://tables/{table}/columns',
    name: 'Table Columns',
    description: 'Get column definitions for a specific table',
    mimeType: 'application/json',
  },
  {
    uriTemplate: 'mysql://tables/{table}/indexes',
    name: 'Table Indexes',
    description: 'Get index information for a specific table',
    mimeType: 'application/json',
  },
  {
    uriTemplate: 'mysql://tables/{table}/sample',
    name: 'Table Sample Data',
    description: 'Get sample rows from a specific table (limit 5)',
    mimeType: 'application/json',
  },
];

function parseResourceUri(uri: string): { type: string; table?: string; subResource?: string } {
  const url = new URL(uri);

  if (url.protocol !== 'mysql:') {
    throw new McpError(ErrorCode.InvalidRequest, `Invalid resource protocol: ${url.protocol}`);
  }

  const pathParts = url.pathname.replace(/^\/\//, '').split('/').filter(Boolean);

  if (pathParts.length === 0) {
    return { type: 'root' };
  }

  if (pathParts[0] === 'schema') {
    return { type: 'schema' };
  }

  if (pathParts[0] === 'tables') {
    if (pathParts.length === 1) {
      return { type: 'tables' };
    }
    if (pathParts.length === 2) {
      return { type: 'table', table: pathParts[1] };
    }
    if (pathParts.length === 3) {
      return { type: 'table-sub', table: pathParts[1], subResource: pathParts[2] };
    }
  }

  throw new McpError(ErrorCode.InvalidRequest, `Unknown resource path: ${url.pathname}`);
}

async function getSchemaOverview(connectionManager: ConnectionManager): Promise<object> {
  const database = connectionManager.getDatabaseName();

  const stats = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      COUNT(*) as table_count,
      SUM(data_length + index_length) as total_size_bytes
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ?
  `, [database]);

  const tables = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ?
    ORDER BY TABLE_NAME
  `, [database]);

  return {
    database,
    tableCount: stats[0]?.table_count || 0,
    totalSizeBytes: stats[0]?.total_size_bytes || 0,
    tables: tables.map((t) => ({
      name: t.TABLE_NAME,
      estimatedRows: t.TABLE_ROWS,
      dataSizeBytes: t.DATA_LENGTH,
      indexSizeBytes: t.INDEX_LENGTH,
    })),
  };
}

async function getTablesList(connectionManager: ConnectionManager): Promise<object[]> {
  const database = connectionManager.getDatabaseName();

  const tables = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      TABLE_NAME as name,
      TABLE_TYPE as type,
      ENGINE as engine,
      TABLE_ROWS as estimatedRows,
      AVG_ROW_LENGTH as avgRowLength,
      DATA_LENGTH as dataSize,
      INDEX_LENGTH as indexSize,
      CREATE_TIME as createdAt,
      UPDATE_TIME as updatedAt,
      TABLE_COMMENT as comment
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ?
    ORDER BY TABLE_NAME
  `, [database]);

  return tables;
}

async function getTableDetails(
  connectionManager: ConnectionManager,
  tableName: string
): Promise<object> {
  const database = connectionManager.getDatabaseName();

  const tableInfo = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      TABLE_NAME as name,
      TABLE_TYPE as type,
      ENGINE as engine,
      TABLE_ROWS as estimatedRows,
      AVG_ROW_LENGTH as avgRowLength,
      DATA_LENGTH as dataSize,
      INDEX_LENGTH as indexSize,
      CREATE_TIME as createdAt,
      UPDATE_TIME as updatedAt,
      TABLE_COMMENT as comment
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  `, [database, tableName]);

  if (tableInfo.length === 0) {
    throw new McpError(ErrorCode.InvalidRequest, `Table not found: ${tableName}`);
  }

  const columns = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      COLUMN_NAME as name,
      COLUMN_TYPE as type,
      IS_NULLABLE as nullable,
      COLUMN_KEY as \`key\`,
      COLUMN_DEFAULT as defaultValue,
      EXTRA as extra,
      COLUMN_COMMENT as comment
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
  `, [database, tableName]);

  const indexes = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      INDEX_NAME as name,
      NON_UNIQUE as nonUnique,
      COLUMN_NAME as columnName,
      SEQ_IN_INDEX as sequenceInIndex,
      INDEX_TYPE as type
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY INDEX_NAME, SEQ_IN_INDEX
  `, [database, tableName]);

  const indexMap = new Map<string, { name: string; unique: boolean; type: string; columns: string[] }>();
  for (const idx of indexes) {
    const name = idx.name as string;
    if (!indexMap.has(name)) {
      indexMap.set(name, {
        name,
        unique: idx.nonUnique === 0,
        type: idx.type as string,
        columns: [],
      });
    }
    indexMap.get(name)!.columns.push(idx.columnName as string);
  }

  const foreignKeys = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      CONSTRAINT_NAME as name,
      COLUMN_NAME as column,
      REFERENCED_TABLE_NAME as referencedTable,
      REFERENCED_COLUMN_NAME as referencedColumn
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
  `, [database, tableName]);

  return {
    ...tableInfo[0],
    columns,
    indexes: Array.from(indexMap.values()),
    foreignKeys,
  };
}

async function getTableColumns(
  connectionManager: ConnectionManager,
  tableName: string
): Promise<object[]> {
  const database = connectionManager.getDatabaseName();

  return connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      COLUMN_NAME as name,
      ORDINAL_POSITION as position,
      COLUMN_TYPE as type,
      DATA_TYPE as dataType,
      CHARACTER_MAXIMUM_LENGTH as maxLength,
      NUMERIC_PRECISION as numericPrecision,
      NUMERIC_SCALE as numericScale,
      IS_NULLABLE as nullable,
      COLUMN_KEY as \`key\`,
      COLUMN_DEFAULT as defaultValue,
      EXTRA as extra,
      COLUMN_COMMENT as comment
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
  `, [database, tableName]);
}

async function getTableIndexes(
  connectionManager: ConnectionManager,
  tableName: string
): Promise<object[]> {
  const database = connectionManager.getDatabaseName();

  const indexes = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT
      INDEX_NAME as name,
      NON_UNIQUE as nonUnique,
      COLUMN_NAME as columnName,
      SEQ_IN_INDEX as sequenceInIndex,
      CARDINALITY as cardinality,
      INDEX_TYPE as type,
      COMMENT as comment
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY INDEX_NAME, SEQ_IN_INDEX
  `, [database, tableName]);

  const indexMap = new Map<string, object>();
  for (const idx of indexes) {
    const name = idx.name as string;
    if (!indexMap.has(name)) {
      indexMap.set(name, {
        name,
        unique: idx.nonUnique === 0,
        type: idx.type,
        cardinality: idx.cardinality,
        comment: idx.comment,
        columns: [],
      });
    }
    (indexMap.get(name) as { columns: string[] }).columns.push(idx.columnName as string);
  }

  return Array.from(indexMap.values());
}

async function getTableSample(
  connectionManager: ConnectionManager,
  tableName: string
): Promise<object[]> {
  const escapedTable = `\`${tableName.replace(/`/g, '``')}\``;
  return connectionManager.query<Record<string, unknown>[]>(
    `SELECT * FROM ${escapedTable} LIMIT 5`
  );
}

export function registerResources(server: Server, connectionManager: ConnectionManager): void {
  server.setRequestHandler(ListResourceTemplatesRequestSchema, async () => ({
    resourceTemplates: RESOURCE_TEMPLATES,
  }));

  server.setRequestHandler(ListResourcesRequestSchema, async () => {
    if (!connectionManager.isConnected()) {
      return { resources: [] };
    }

    const database = connectionManager.getDatabaseName();

    return {
      resources: [
        {
          uri: 'mysql://schema',
          name: `Database Schema: ${database}`,
          description: 'Overview of the database schema including all tables',
          mimeType: 'application/json',
        },
        {
          uri: 'mysql://tables',
          name: 'All Tables',
          description: 'List of all tables in the database',
          mimeType: 'application/json',
        },
      ],
    };
  });

  server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    const { uri } = request.params;

    if (!connectionManager.isConnected()) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Not connected to a database. Use connect_db tool first.'
      );
    }

    logger.debug('Reading resource', { uri });

    const parsed = parseResourceUri(uri);
    let data: unknown;

    switch (parsed.type) {
      case 'schema':
        data = await getSchemaOverview(connectionManager);
        break;

      case 'tables':
        data = await getTablesList(connectionManager);
        break;

      case 'table':
        data = await getTableDetails(connectionManager, parsed.table!);
        break;

      case 'table-sub':
        switch (parsed.subResource) {
          case 'columns':
            data = await getTableColumns(connectionManager, parsed.table!);
            break;
          case 'indexes':
            data = await getTableIndexes(connectionManager, parsed.table!);
            break;
          case 'sample':
            data = await getTableSample(connectionManager, parsed.table!);
            break;
          default:
            throw new McpError(
              ErrorCode.InvalidRequest,
              `Unknown sub-resource: ${parsed.subResource}`
            );
        }
        break;

      default:
        throw new McpError(ErrorCode.InvalidRequest, `Unknown resource type: ${parsed.type}`);
    }

    return {
      contents: [
        {
          uri,
          mimeType: 'application/json',
          text: JSON.stringify(data, null, 2),
        },
      ],
    };
  });
}
