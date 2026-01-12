import type { Server } from '@modelcontextprotocol/sdk/server/index.js';
import {
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  ErrorCode,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import type { ConnectionManager } from './connection/index.js';
import { logger } from './utils/logger.js';

const PROMPT_DEFINITIONS = [
  {
    name: 'generate-select',
    description: 'Generate a SELECT query for a table with optional filters',
    arguments: [
      {
        name: 'table',
        description: 'The table name to query',
        required: true,
      },
      {
        name: 'columns',
        description: 'Comma-separated list of columns (default: all)',
        required: false,
      },
      {
        name: 'where',
        description: 'WHERE clause conditions (e.g., "status = active")',
        required: false,
      },
      {
        name: 'orderBy',
        description: 'ORDER BY column(s)',
        required: false,
      },
      {
        name: 'limit',
        description: 'Maximum number of rows to return',
        required: false,
      },
    ],
  },
  {
    name: 'generate-insert',
    description: 'Generate an INSERT statement template for a table',
    arguments: [
      {
        name: 'table',
        description: 'The table name to insert into',
        required: true,
      },
    ],
  },
  {
    name: 'generate-update',
    description: 'Generate an UPDATE statement template for a table',
    arguments: [
      {
        name: 'table',
        description: 'The table name to update',
        required: true,
      },
      {
        name: 'where',
        description: 'WHERE clause to identify rows to update',
        required: true,
      },
    ],
  },
  {
    name: 'explain-schema',
    description: 'Get a natural language explanation of a table structure',
    arguments: [
      {
        name: 'table',
        description: 'The table name to explain',
        required: true,
      },
    ],
  },
  {
    name: 'suggest-indexes',
    description: 'Analyze a table and suggest potential indexes for optimization',
    arguments: [
      {
        name: 'table',
        description: 'The table name to analyze',
        required: true,
      },
    ],
  },
];

async function getTableColumns(
  connectionManager: ConnectionManager,
  tableName: string
): Promise<{ name: string; type: string; nullable: string; key: string; default: unknown }[]> {
  const database = connectionManager.getDatabaseName();

  return connectionManager.query(`
    SELECT
      COLUMN_NAME as name,
      COLUMN_TYPE as type,
      IS_NULLABLE as nullable,
      COLUMN_KEY as \`key\`,
      COLUMN_DEFAULT as \`default\`
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
  `, [database, tableName]);
}

async function generateSelectPrompt(
  connectionManager: ConnectionManager,
  args: Record<string, string>
): Promise<{ role: 'user'; content: { type: 'text'; text: string } }[]> {
  const { table, columns, where, orderBy, limit } = args;

  if (!table) {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }

  const tableColumns = await getTableColumns(connectionManager, table);
  const columnList = tableColumns.map((c) => `  - ${c.name} (${c.type})`).join('\n');

  const columnSpec = columns || '*';
  let query = `SELECT ${columnSpec}\nFROM \`${table}\``;

  if (where) {
    query += `\nWHERE ${where}`;
  }
  if (orderBy) {
    query += `\nORDER BY ${orderBy}`;
  }
  if (limit) {
    query += `\nLIMIT ${limit}`;
  }

  const text = `Generate a SELECT query for the "${table}" table.

Available columns:
${columnList}

Base query template:
\`\`\`sql
${query}
\`\`\`

Please customize this query based on the user's needs. Consider:
- Which columns are actually needed
- Appropriate WHERE conditions for filtering
- Sorting requirements
- Whether to use JOINs if related tables exist
- Using prepared statement placeholders (?) for user input values`;

  return [{ role: 'user', content: { type: 'text', text } }];
}

async function generateInsertPrompt(
  connectionManager: ConnectionManager,
  args: Record<string, string>
): Promise<{ role: 'user'; content: { type: 'text'; text: string } }[]> {
  const { table } = args;

  if (!table) {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }

  const tableColumns = await getTableColumns(connectionManager, table);

  // Filter out auto-increment columns for INSERT
  const insertColumns = tableColumns.filter(
    (c) => !c.key.includes('PRI') || c.type.toLowerCase().includes('auto_increment') === false
  );

  const columnNames = insertColumns.map((c) => `\`${c.name}\``).join(', ');
  const placeholders = insertColumns.map(() => '?').join(', ');
  const columnDetails = insertColumns
    .map((c) => {
      let detail = `  - ${c.name}: ${c.type}`;
      if (c.nullable === 'NO') detail += ' (required)';
      if (c.default !== null) detail += ` (default: ${c.default})`;
      return detail;
    })
    .join('\n');

  const text = `Generate an INSERT statement for the "${table}" table.

Column details:
${columnDetails}

Template:
\`\`\`sql
INSERT INTO \`${table}\` (${columnNames})
VALUES (${placeholders})
\`\`\`

Notes:
- Use prepared statement placeholders (?) for values
- Ensure required fields are provided
- Consider default values for optional fields
- Auto-increment primary keys should be omitted`;

  return [{ role: 'user', content: { type: 'text', text } }];
}

async function generateUpdatePrompt(
  connectionManager: ConnectionManager,
  args: Record<string, string>
): Promise<{ role: 'user'; content: { type: 'text'; text: string } }[]> {
  const { table, where } = args;

  if (!table) {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }
  if (!where) {
    throw new McpError(ErrorCode.InvalidParams, 'WHERE clause is required for UPDATE');
  }

  const tableColumns = await getTableColumns(connectionManager, table);
  const columnDetails = tableColumns
    .map((c) => `  - ${c.name}: ${c.type}${c.key === 'PRI' ? ' (primary key)' : ''}`)
    .join('\n');

  const setClause = tableColumns
    .filter((c) => c.key !== 'PRI')
    .slice(0, 3)
    .map((c) => `  \`${c.name}\` = ?`)
    .join(',\n');

  const text = `Generate an UPDATE statement for the "${table}" table.

Column details:
${columnDetails}

Template:
\`\`\`sql
UPDATE \`${table}\`
SET
${setClause}
WHERE ${where}
\`\`\`

Important:
- Always include a WHERE clause to avoid updating all rows
- Use prepared statement placeholders (?) for values
- Primary key columns typically should not be updated
- Consider which columns actually need to be modified`;

  return [{ role: 'user', content: { type: 'text', text } }];
}

async function generateExplainSchemaPrompt(
  connectionManager: ConnectionManager,
  args: Record<string, string>
): Promise<{ role: 'user'; content: { type: 'text'; text: string } }[]> {
  const { table } = args;

  if (!table) {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }

  const database = connectionManager.getDatabaseName();

  // Get table info
  const tableInfo = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT TABLE_COMMENT as comment, ENGINE as engine, TABLE_ROWS as rows
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  `, [database, table]);

  // Get columns
  const columns = await getTableColumns(connectionManager, table);

  // Get indexes
  const indexes = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT DISTINCT INDEX_NAME as name, NON_UNIQUE as nonUnique
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  `, [database, table]);

  // Get foreign keys
  const foreignKeys = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT COLUMN_NAME as column, REFERENCED_TABLE_NAME as refTable, REFERENCED_COLUMN_NAME as refColumn
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
  `, [database, table]);

  const info = tableInfo[0] || {};
  const columnList = columns.map((c) => {
    let desc = `- **${c.name}** (${c.type})`;
    if (c.key === 'PRI') desc += ' - Primary Key';
    if (c.key === 'UNI') desc += ' - Unique';
    if (c.key === 'MUL') desc += ' - Indexed';
    if (c.nullable === 'NO') desc += ' - Required';
    return desc;
  }).join('\n');

  const indexList = indexes.map((i) =>
    `- ${i.name}${i.nonUnique === 0 ? ' (unique)' : ''}`
  ).join('\n');

  const fkList = foreignKeys.map((fk) =>
    `- ${fk.column} → ${fk.refTable}.${fk.refColumn}`
  ).join('\n');

  const text = `Explain the structure and purpose of the "${table}" table.

## Table Information
- Engine: ${info.engine || 'Unknown'}
- Estimated Rows: ${info.rows || 'Unknown'}
${info.comment ? `- Comment: ${info.comment}` : ''}

## Columns
${columnList}

## Indexes
${indexList || 'No additional indexes'}

## Foreign Keys
${fkList || 'No foreign keys'}

Please provide:
1. A brief description of what this table likely stores
2. Explanation of key columns and their purposes
3. How this table might relate to other tables
4. Any notable design patterns or potential issues`;

  return [{ role: 'user', content: { type: 'text', text } }];
}

async function generateSuggestIndexesPrompt(
  connectionManager: ConnectionManager,
  args: Record<string, string>
): Promise<{ role: 'user'; content: { type: 'text'; text: string } }[]> {
  const { table } = args;

  if (!table) {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }

  const database = connectionManager.getDatabaseName();

  // Get columns
  const columns = await getTableColumns(connectionManager, table);

  // Get existing indexes
  const indexes = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT INDEX_NAME as name, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns, NON_UNIQUE as nonUnique
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    GROUP BY INDEX_NAME, NON_UNIQUE
  `, [database, table]);

  // Get table row count estimate
  const stats = await connectionManager.query<Record<string, unknown>[]>(`
    SELECT TABLE_ROWS as rows
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  `, [database, table]);

  const columnList = columns.map((c) => `- ${c.name} (${c.type})`).join('\n');
  const indexList = indexes.map((i) =>
    `- ${i.name}: (${i.columns})${i.nonUnique === 0 ? ' UNIQUE' : ''}`
  ).join('\n');

  const rowCount = stats[0]?.rows || 'Unknown';

  const text = `Analyze the "${table}" table and suggest potential indexes for optimization.

## Table Statistics
- Estimated Rows: ${rowCount}

## Columns
${columnList}

## Existing Indexes
${indexList || 'Only primary key index'}

## Analysis Request
Please analyze this table structure and suggest:

1. **Missing indexes** that would benefit common query patterns:
   - Columns frequently used in WHERE clauses
   - Columns used in JOIN conditions
   - Columns used in ORDER BY

2. **Composite indexes** that could improve multi-column queries

3. **Covering indexes** for frequently accessed column combinations

4. **Index considerations**:
   - Selectivity (cardinality) of columns
   - Write performance impact
   - Storage requirements

Provide specific CREATE INDEX statements for your recommendations.`;

  return [{ role: 'user', content: { type: 'text', text } }];
}

export function registerPrompts(server: Server, connectionManager: ConnectionManager): void {
  server.setRequestHandler(ListPromptsRequestSchema, async () => ({
    prompts: PROMPT_DEFINITIONS,
  }));

  server.setRequestHandler(GetPromptRequestSchema, async (request) => {
    const { name, arguments: args = {} } = request.params;

    if (!connectionManager.isConnected()) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Not connected to a database. Use connect_db tool first.'
      );
    }

    logger.debug('Getting prompt', { name, args });

    let messages: { role: 'user'; content: { type: 'text'; text: string } }[];

    switch (name) {
      case 'generate-select':
        messages = await generateSelectPrompt(connectionManager, args);
        break;

      case 'generate-insert':
        messages = await generateInsertPrompt(connectionManager, args);
        break;

      case 'generate-update':
        messages = await generateUpdatePrompt(connectionManager, args);
        break;

      case 'explain-schema':
        messages = await generateExplainSchemaPrompt(connectionManager, args);
        break;

      case 'suggest-indexes':
        messages = await generateSuggestIndexesPrompt(connectionManager, args);
        break;

      default:
        throw new McpError(ErrorCode.InvalidRequest, `Unknown prompt: ${name}`);
    }

    return { messages };
  });
}
