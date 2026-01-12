import { z } from 'zod';
import type { Server } from '@modelcontextprotocol/sdk/server/index.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import type { ConnectionManager } from '../connection/index.js';
import { handleConnectDb } from './connection.js';
import { handleQuery, handleExecute } from './query.js';
import {
  handleListTables,
  handleDescribeTable,
  handleCreateTable,
  handleAddColumn,
  handleAlterColumn,
  handleDropColumn,
  handleDropTable,
  handleTruncateTable,
  handleListDatabases,
  handleGetIndexes,
  handleGetForeignKeys,
} from './schema.js';

const ConnectionArgsSchema = z.object({
  url: z.string().optional().describe('Database URL (mysql://user:pass@host:port/db)'),
  workspace: z.string().optional().describe('Project workspace path containing .env file'),
  host: z.string().optional().describe('Database host'),
  port: z.number().optional().describe('Database port (default: 3306)'),
  user: z.string().optional().describe('Database user'),
  password: z.string().optional().describe('Database password'),
  database: z.string().optional().describe('Database name'),
});

const QueryArgsSchema = z.object({
  sql: z.string().describe('The SQL query string to execute'),
  params: z
    .array(z.union([z.string(), z.number(), z.boolean(), z.null()]))
    .optional()
    .describe('Optional array of parameters to bind to query placeholders (?)'),
});

const SchemaFieldSchema = z.object({
  name: z.string().describe('Column name'),
  type: z.string().describe('MySQL column type (VARCHAR, INT, etc.)'),
  length: z.number().optional().describe('Column length for applicable types'),
  nullable: z.boolean().optional().describe('Whether the column allows NULL values'),
  default: z.union([z.string(), z.number(), z.null()]).optional().describe('Default value'),
  autoIncrement: z.boolean().optional().describe('Whether to auto-increment'),
  primary: z.boolean().optional().describe('Whether this is the primary key'),
});

const IndexDefinitionSchema = z.object({
  name: z.string().describe('Index name'),
  columns: z.array(z.string()).describe('Columns to include in index'),
  unique: z.boolean().optional().describe('Whether this is a unique index'),
});

const CreateTableArgsSchema = z.object({
  table: z.string().describe('Table name'),
  fields: z.array(SchemaFieldSchema).describe('Column definitions'),
  indexes: z.array(IndexDefinitionSchema).optional().describe('Index definitions'),
});

const TableNameSchema = z.object({
  table: z.string().describe('Table name'),
});

const AddColumnArgsSchema = z.object({
  table: z.string().describe('Table name'),
  field: SchemaFieldSchema.describe('Column definition'),
});

const AlterColumnArgsSchema = z.object({
  table: z.string().describe('Table name'),
  column: z.string().describe('Column name to modify'),
  type: z.string().optional().describe('New column type'),
  length: z.number().optional().describe('New column length'),
  nullable: z.boolean().optional().describe('Whether column allows NULL'),
  default: z.union([z.string(), z.number(), z.null()]).optional().describe('New default value'),
  newName: z.string().optional().describe('New column name (for renaming)'),
});

const DropColumnArgsSchema = z.object({
  table: z.string().describe('Table name'),
  column: z.string().describe('Column name to drop'),
});

const ConfirmableTableSchema = z.object({
  table: z.string().describe('Table name'),
  confirm: z.boolean().describe('Set to true to confirm the destructive operation'),
});

const TOOL_DEFINITIONS = [
  {
    name: 'connect_db',
    description: 'Connect to a MySQL database using URL, workspace config, or direct parameters',
    inputSchema: {
      type: 'object',
      properties: {
        url: { type: 'string', description: 'Database URL (mysql://user:pass@host:port/db)' },
        workspace: { type: 'string', description: 'Project workspace path containing .env file' },
        host: { type: 'string', description: 'Database host' },
        port: { type: 'number', description: 'Database port (default: 3306)' },
        user: { type: 'string', description: 'Database user' },
        password: { type: 'string', description: 'Database password' },
        database: { type: 'string', description: 'Database name' },
      },
    },
  },
  {
    name: 'query',
    description: 'Execute a SELECT query and return results',
    inputSchema: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'The SQL SELECT query to execute' },
        params: {
          type: 'array',
          items: { type: ['string', 'number', 'boolean', 'null'] },
          description: 'Optional parameters to bind to query placeholders (?)',
        },
      },
      required: ['sql'],
    },
  },
  {
    name: 'execute',
    description: 'Execute an INSERT, UPDATE, or DELETE query',
    inputSchema: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'The SQL query (INSERT, UPDATE, DELETE) to execute' },
        params: {
          type: 'array',
          items: { type: ['string', 'number', 'boolean', 'null'] },
          description: 'Optional parameters to bind to query placeholders (?)',
        },
      },
      required: ['sql'],
    },
  },
  {
    name: 'list_tables',
    description: 'List all tables in the connected database',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'list_databases',
    description: 'List all accessible databases on the server',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'describe_table',
    description: 'Get detailed structure of a table including columns, types, and constraints',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
      },
      required: ['table'],
    },
  },
  {
    name: 'create_table',
    description: 'Create a new table with specified columns and indexes',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        fields: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string', description: 'Column name' },
              type: { type: 'string', description: 'MySQL column type' },
              length: { type: 'number', description: 'Column length' },
              nullable: { type: 'boolean', description: 'Allow NULL values' },
              default: { type: ['string', 'number', 'null'], description: 'Default value' },
              autoIncrement: { type: 'boolean', description: 'Auto-increment' },
              primary: { type: 'boolean', description: 'Primary key' },
            },
            required: ['name', 'type'],
          },
          description: 'Column definitions',
        },
        indexes: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string', description: 'Index name' },
              columns: { type: 'array', items: { type: 'string' }, description: 'Indexed columns' },
              unique: { type: 'boolean', description: 'Unique index' },
            },
            required: ['name', 'columns'],
          },
          description: 'Index definitions',
        },
      },
      required: ['table', 'fields'],
    },
  },
  {
    name: 'add_column',
    description: 'Add a new column to an existing table',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        field: {
          type: 'object',
          properties: {
            name: { type: 'string', description: 'Column name' },
            type: { type: 'string', description: 'MySQL column type' },
            length: { type: 'number', description: 'Column length' },
            nullable: { type: 'boolean', description: 'Allow NULL values' },
            default: { type: ['string', 'number', 'null'], description: 'Default value' },
          },
          required: ['name', 'type'],
          description: 'Column definition',
        },
      },
      required: ['table', 'field'],
    },
  },
  {
    name: 'alter_column',
    description: 'Modify an existing column (type, nullable, default, or rename)',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        column: { type: 'string', description: 'Column name to modify' },
        type: { type: 'string', description: 'New column type' },
        length: { type: 'number', description: 'New column length' },
        nullable: { type: 'boolean', description: 'Allow NULL values' },
        default: { type: ['string', 'number', 'null'], description: 'New default value' },
        newName: { type: 'string', description: 'New column name (for renaming)' },
      },
      required: ['table', 'column'],
    },
  },
  {
    name: 'drop_column',
    description: 'Remove a column from a table',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        column: { type: 'string', description: 'Column name to drop' },
      },
      required: ['table', 'column'],
    },
  },
  {
    name: 'drop_table',
    description: 'Delete a table (requires confirmation)',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        confirm: { type: 'boolean', description: 'Set to true to confirm deletion' },
      },
      required: ['table', 'confirm'],
    },
  },
  {
    name: 'truncate_table',
    description: 'Remove all rows from a table (requires confirmation)',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
        confirm: { type: 'boolean', description: 'Set to true to confirm truncation' },
      },
      required: ['table', 'confirm'],
    },
  },
  {
    name: 'get_indexes',
    description: 'List all indexes on a table',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
      },
      required: ['table'],
    },
  },
  {
    name: 'get_foreign_keys',
    description: 'List all foreign key relationships for a table',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table name' },
      },
      required: ['table'],
    },
  },
];

export function registerTools(server: Server, connectionManager: ConnectionManager): void {
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOL_DEFINITIONS,
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    try {
      let result: { type: 'text'; text: string }[];

      switch (name) {
        case 'connect_db':
          result = await handleConnectDb(
            connectionManager,
            ConnectionArgsSchema.parse(args)
          );
          break;

        case 'query':
          result = await handleQuery(connectionManager, QueryArgsSchema.parse(args));
          break;

        case 'execute':
          result = await handleExecute(connectionManager, QueryArgsSchema.parse(args));
          break;

        case 'list_tables':
          result = await handleListTables(connectionManager);
          break;

        case 'list_databases':
          result = await handleListDatabases(connectionManager);
          break;

        case 'describe_table':
          result = await handleDescribeTable(connectionManager, TableNameSchema.parse(args));
          break;

        case 'create_table':
          result = await handleCreateTable(connectionManager, CreateTableArgsSchema.parse(args));
          break;

        case 'add_column':
          result = await handleAddColumn(connectionManager, AddColumnArgsSchema.parse(args));
          break;

        case 'alter_column':
          result = await handleAlterColumn(connectionManager, AlterColumnArgsSchema.parse(args));
          break;

        case 'drop_column':
          result = await handleDropColumn(connectionManager, DropColumnArgsSchema.parse(args));
          break;

        case 'drop_table':
          result = await handleDropTable(connectionManager, ConfirmableTableSchema.parse(args));
          break;

        case 'truncate_table':
          result = await handleTruncateTable(
            connectionManager,
            ConfirmableTableSchema.parse(args)
          );
          break;

        case 'get_indexes':
          result = await handleGetIndexes(connectionManager, TableNameSchema.parse(args));
          break;

        case 'get_foreign_keys':
          result = await handleGetForeignKeys(connectionManager, TableNameSchema.parse(args));
          break;

        default:
          throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
      }

      return { content: result };
    } catch (error) {
      if (error instanceof z.ZodError) {
        const messages = error.issues.map((issue) => issue.message).join(', ');
        throw new McpError(ErrorCode.InvalidParams, `Invalid parameters: ${messages}`);
      }
      throw error;
    }
  });
}

export { TOOL_DEFINITIONS };
