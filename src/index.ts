#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import * as mysql from 'mysql2/promise';
import { config } from 'dotenv';
import { parse as parseUrl } from 'url';
import path from 'path';

// Load environment variables
config();

// Type definitions
interface DatabaseConfig {
  host: string;
  port?: number;
  user: string;
  password: string;
  database: string;
}

interface SSLConfig {
  ca?: string;
  cert?: string;
  key?: string;
  rejectUnauthorized?: boolean;
}

interface ConnectionConfig extends DatabaseConfig {
  ssl?: SSLConfig;
  connectionTimeout?: number;
  connectRetry?: {
    maxAttempts: number;
    delay: number;
  };
}

interface SchemaField {
  name: string;
  type: string;
  length?: number;
  nullable?: boolean;
  default?: string | number | null;
  autoIncrement?: boolean;
  primary?: boolean;
}

interface IndexDefinition {
  name: string;
  columns: string[];
  unique?: boolean;
}

interface QueryResult {
  content: Array<{
    type: 'text';
    text: string;
  }>;
}

interface QueryArgs {
  sql: string;
  params?: Array<string | number | boolean | null>;
}

interface ConnectionArgs {
  url?: string;
  workspace?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
}

// Type guard for error objects
function isErrorWithMessage(error: unknown): error is { message: string } {
  return (
    typeof error === 'object' &&
    error !== null &&
    'message' in error &&
    typeof (error as Record<string, unknown>).message === 'string'
  );
}

// Helper to get error message
function getErrorMessage(error: unknown): string {
  if (isErrorWithMessage(error)) {
    return error.message;
  }
  return String(error);
}

class MySQLServer {
  private server: Server;
  private pool: mysql.Pool | null = null;
  private config: ConnectionConfig | null = null;
  private currentWorkspace: string | null = null;

  constructor() {
    this.server = new Server(
      {
        name: 'mysql-server',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupToolHandlers();
    this.setupErrorHandlers();
  }

  private setupErrorHandlers() {
    this.server.onerror = (error) => console.error('[MCP Error]', error);

    process.on('SIGINT', async () => {
      await this.cleanup();
      process.exit(0);
    });

    process.on('SIGTERM', async () => {
      await this.cleanup();
      process.exit(0);
    });
  }

  private async cleanup() {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
    await this.server.close();
  }

  private handleDatabaseError(error: unknown): never {
    // Handle MySQL-specific errors
    if (error instanceof Error) {
      const mysqlError = error as any;
      const code = mysqlError.code || '';
      const errno = mysqlError.errno || 0;

      // User input errors (Invalid Request)
      if (code === 'ER_PARSE_ERROR' || code === 'ER_EMPTY_QUERY') {
        throw new McpError(ErrorCode.InvalidParams, `Invalid SQL syntax: ${mysqlError.message}`);
      }

      // Authentication errors (Unauthorized)
      if (code === 'ER_ACCESS_DENIED_ERROR') {
        throw new McpError(ErrorCode.InvalidRequest, `Database authentication failed: Invalid credentials`);
      }

      // Database configuration errors (Internal Error)
      if (code === 'ER_BAD_DB_ERROR') {
        throw new McpError(ErrorCode.InternalError, `Database configuration error: Database does not exist`);
      }

      // Connection errors (Internal Error)
      if (code === 'ECONNREFUSED' || code === 'ETIMEDOUT' || code === 'ENOTFOUND') {
        throw new McpError(ErrorCode.InternalError, `Database connection error: ${code}`);
      }

      // Schema-related errors (Invalid Request)
      if (code === 'ER_NO_SUCH_TABLE') {
        throw new McpError(ErrorCode.InvalidParams, `Table does not exist: ${mysqlError.message}`);
      }

      // Data integrity errors (Invalid Request)
      if (code === 'ER_DUP_ENTRY') {
        throw new McpError(ErrorCode.InvalidParams, `Data integrity error: Duplicate entry`);
      }

      // Log unknown errors for debugging
      console.error('Unhandled MySQL error:', {
        code,
        errno,
        message: mysqlError.message,
        stack: mysqlError.stack
      });
    }

    // Generic error handling as fallback
    const message = getErrorMessage(error);
    throw new McpError(ErrorCode.InternalError, `Unexpected database error: ${message}`);
  }

  private validateSqlInput(sql: string, allowedTypes: string[]) {
    const type = sql.trim().split(' ')[0].toUpperCase();
    if (!allowedTypes.includes(type)) {
      throw new McpError(
        ErrorCode.InvalidParams,
        `Invalid SQL type. Allowed: ${allowedTypes.join(', ')}`
      );
    }
  }

  private async ensureConnection() {
    if (!this.config) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Database configuration not set. Use connect_db tool first.'
      );
    }

    if (!this.pool) {
      try {
        this.pool = mysql.createPool({
          ...this.config,
          waitForConnections: true,
          connectionLimit: 10,
          queueLimit: 0,
          enableKeepAlive: true,
          keepAliveInitialDelay: 0,
          supportBigNumbers: true,
          bigNumberStrings: true
        });

        // Test the connection
        const connection = await this.pool.getConnection();
        connection.release();
      } catch (error) {
        this.pool = null;
        this.handleDatabaseError(error);
      }
    }

    if (!this.pool) {
      throw new McpError(
        ErrorCode.InternalError,
        'Failed to establish database connection'
      );
    }

    return this.pool;
  }

  private async executeQuery<T>(sql: string, params: any[] = []): Promise<T> {
    const pool = await this.ensureConnection();
    try {
      const [result] = await pool.query(sql, params);
      return result as T;
    } catch (error) {
      this.handleDatabaseError(error);
    }
  }

  private hasDirectConfig(args: ConnectionArgs): boolean {
    return !!(args.host && args.user && args.password && args.database);
  }

  private createDirectConfig(args: ConnectionArgs): ConnectionConfig {
    if (!this.hasDirectConfig(args)) {
      throw new McpError(
        ErrorCode.InvalidParams,
        'Missing required connection parameters'
      );
    }

    return {
      host: args.host!,
      port: args.port,
      user: args.user!,
      password: args.password!,
      database: args.database!
    };
  }

  private async loadConfig(args: ConnectionArgs): Promise<ConnectionConfig> {
    console.error('Loading config with args:', args);

    if (args.url) {
      console.error('Using URL configuration');
      return this.parseConnectionUrl(args.url);
    }

    if (args.workspace) {
      console.error('Attempting workspace configuration from:', args.workspace);
      const config = await this.loadWorkspaceConfig(args.workspace);
      if (config) {
        console.error('Successfully loaded workspace config');
        return config;
      }
      console.error('Failed to load workspace config');
    }

    if (this.hasDirectConfig(args)) {
      console.error('Using direct configuration parameters');
      return this.createDirectConfig(args);
    }

    console.error('No valid configuration method found');
    throw new McpError(
      ErrorCode.InvalidParams,
      'No valid configuration provided. Please provide either a URL, workspace path, or connection parameters.'
    );
  }

  private async loadWorkspaceConfig(workspace: string): Promise<ConnectionConfig | null> {
    try {
      const fs = await import('fs');
      // Resolve workspace path relative to current working directory
      const resolvedWorkspace = path.resolve(process.cwd(), workspace);
      console.error(`Resolved workspace path: ${resolvedWorkspace}`);

      const envPaths = [
        path.join(resolvedWorkspace, '.env.local'),
        path.join(resolvedWorkspace, '.env')
      ];

      let loadedConfig: ConnectionConfig | null = null;

      for (const envPath of envPaths) {
        console.error(`Checking for env file at: ${envPath}`);
        
        if (!fs.existsSync(envPath)) {
          console.error(`Environment file not found at: ${envPath}`);
          continue;
        }

        console.error(`Found environment file at: ${envPath}`);
        const workspaceEnv = require('dotenv').config({ path: envPath });

        if (workspaceEnv.error) {
          console.error(`Error loading environment file ${envPath}:`, workspaceEnv.error);
          continue;
        }

        const { DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE } = workspaceEnv.parsed || {};

        if (DATABASE_URL) {
          console.error(`Found DATABASE_URL in ${envPath}`);
          loadedConfig = this.parseConnectionUrl(DATABASE_URL);
          break;
        }

        if (DB_HOST && DB_USER && DB_PASSWORD && DB_DATABASE) {
          console.error(`Found individual database credentials in ${envPath}`);
          loadedConfig = {
            host: DB_HOST,
            port: DB_PORT ? parseInt(DB_PORT, 10) : undefined,
            user: DB_USER,
            password: DB_PASSWORD,
            database: DB_DATABASE
          };
          break;
        }
      }

      if (!loadedConfig) {
        console.error('No valid database configuration found in workspace:', resolvedWorkspace);
        console.error('Checked paths:', envPaths);
      }

      return loadedConfig;
    } catch (error) {
      console.error('Error loading workspace config:', error);
      return null;
    }
  }

  private parseConnectionUrl(url: string): ConnectionConfig {
    const parsed = parseUrl(url);
    if (!parsed.host || !parsed.auth) {
      throw new McpError(
        ErrorCode.InvalidParams,
        'Invalid connection URL'
      );
    }

    const [user, password] = parsed.auth.split(':');
    const database = parsed.pathname?.slice(1);

    if (!database) {
      throw new McpError(
        ErrorCode.InvalidParams,
        'Database name must be specified in URL'
      );
    }

    return {
      host: parsed.hostname!,
      port: parsed.port ? parseInt(parsed.port, 10) : undefined,
      user,
      password: password || '',
      database,
      ssl: parsed.protocol === 'mysqls:' ? { rejectUnauthorized: true } : undefined
    };
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'connect_db',
          description: 'Connect to MySQL database using URL or config',
          inputSchema: {
            type: 'object',
            properties: {
              url: {
                type: 'string',
                description: 'Database URL (mysql://user:pass@host:port/db)',
                optional: true
              },
              workspace: {
                type: 'string',
                description: 'Project workspace path',
                optional: true
              },
              // Keep existing connection params as fallback
              host: { type: 'string', optional: true },
              port: { type: 'number', description: 'Database port (default: 3306)', optional: true },
              user: { type: 'string', optional: true },
              password: { type: 'string', optional: true },
              database: { type: 'string', optional: true }
            },
            // No required fields - will try different connection methods
          },
        },
        {
          name: 'query',
          description: 'Execute a SELECT query',
          inputSchema: {
            type: 'object',
            properties: {
              sql: {
                type: 'string',
                description: 'The SQL SELECT query string to execute.',
              },
              params: {
                type: 'array',
                items: {
                  type: 'string',
                  description: 'A parameter value (as string) to bind to the query.'
                },
                description: 'An optional array of parameters (as strings) to bind to the SQL query placeholders (e.g., ?).',
                optional: true,
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
              sql: {
                type: 'string',
                description: 'The SQL query string (INSERT, UPDATE, DELETE) to execute.',
              },
              params: {
                type: 'array',
                items: {
                  type: 'string',
                  description: 'A parameter value (as string) to bind to the query.'
                },
                description: 'An optional array of parameters (as strings) to bind to the SQL query placeholders (e.g., ?).',
                optional: true,
              },
            },
            required: ['sql'],
          },
        },
        {
          name: 'list_tables',
          description: 'List all tables in the database',
          inputSchema: {
            type: 'object',
            properties: {},
            required: [],
          },
        },
        {
          name: 'describe_table',
          description: 'Get table structure',
          inputSchema: {
            type: 'object',
            properties: {
              table: {
                type: 'string',
                description: 'Table name',
              },
            },
            required: ['table'],
          },
        },
        {
          name: 'create_table',
          description: 'Create a new table in the database',
          inputSchema: {
            type: 'object',
            properties: {
              table: {
                type: 'string',
                description: 'Table name',
              },
              fields: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    name: { type: 'string' },
                    type: { type: 'string' },
                    length: { type: 'number', optional: true },
                    nullable: { type: 'boolean', optional: true },
                    default: {
                      type: 'string',
                      description: 'Default value for the column (as string).',
                      optional: true
                    },
                    autoIncrement: { type: 'boolean', optional: true },
                    primary: { type: 'boolean', optional: true }
                  },
                  required: ['name', 'type']
                }
              },
              indexes: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    name: { type: 'string' },
                    columns: {
                      type: 'array',
                      items: { type: 'string' }
                    },
                    unique: { type: 'boolean', optional: true }
                  },
                  required: ['name', 'columns']
                },
                optional: true
              }
            },
            required: ['table', 'fields']
          }
        },
        {
          name: 'add_column',
          description: 'Add a new column to existing table',
          inputSchema: {
            type: 'object',
            properties: {
              table: { type: 'string' },
              field: {
                type: 'object',
                properties: {
                  name: { type: 'string' },
                  type: { type: 'string' },
                  length: { type: 'number', optional: true },
                  nullable: { type: 'boolean', optional: true },
                  default: {
                    type: 'string',
                    description: 'Default value for the column (as string).',
                    optional: true
                  }
                },
                required: ['name', 'type']
              }
            },
            required: ['table', 'field']
          }
        }
      ]
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      switch (request.params.name) {
        case 'connect_db':
          return await this.handleConnectDb(request.params.arguments as unknown as ConnectionArgs);
        case 'query':
          return await this.handleQuery(request.params.arguments as unknown as QueryArgs);
        case 'execute':
          return await this.handleExecute(request.params.arguments as unknown as QueryArgs);
        case 'list_tables':
          return await this.handleListTables();
        case 'describe_table':
          return await this.handleDescribeTable(request.params.arguments);
        case 'create_table':
          return await this.handleCreateTable(request.params.arguments);
        case 'add_column':
          return await this.handleAddColumn(request.params.arguments);
        default:
          throw new McpError(
            ErrorCode.MethodNotFound,
            `Unknown tool: ${request.params.name}`
          );
      }
    });
  }

  private async handleConnectDb(args: ConnectionArgs) {
    this.config = await this.loadConfig(args);

    try {
      await this.ensureConnection();
      return {
        content: [
          {
            type: 'text',
            text: `Successfully connected to database ${this.config.database} at ${this.config.host}${this.config.port ? ':' + this.config.port : ''}`
          }
        ]
      };
    } catch (error) {
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to connect to database: ${getErrorMessage(error)}`
      );
    }
  }

  private async handleQuery(args: QueryArgs): Promise<QueryResult> {
    this.validateSqlInput(args.sql, ['SELECT']);
    const rows = await this.executeQuery(args.sql, args.params || []);

    return {
      content: [{
        type: 'text',
        text: JSON.stringify(rows, null, 2)
      }]
    };
  }

  private async handleExecute(args: QueryArgs): Promise<QueryResult> {
    this.validateSqlInput(args.sql, ['INSERT', 'UPDATE', 'DELETE']);
    const result = await this.executeQuery(args.sql, args.params || []);

    return {
      content: [{
        type: 'text',
        text: JSON.stringify(result, null, 2)
      }]
    };
  }

  private async handleListTables() {
    const rows = await this.executeQuery('SHOW TABLES');
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(rows, null, 2),
        },
      ],
    };
  }

  private async handleDescribeTable(args: any) {
    if (!args.table) {
      throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
    }

    const rows = await this.executeQuery(
      `SELECT 
        COLUMN_NAME as Field,
        COLUMN_TYPE as Type,
        IS_NULLABLE as \`Null\`,
        COLUMN_KEY as \`Key\`,
        COLUMN_DEFAULT as \`Default\`,
        EXTRA as Extra,
        COLUMN_COMMENT as Comment
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION`,
      [this.config!.database, args.table]
    );

    const formattedRows = (rows as any[]).map(row => ({
      ...row,
      Null: row.Null === 'YES' ? 'YES' : 'NO'
    }));

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(formattedRows, null, 2),
        },
      ],
    };
  }

  private async handleCreateTable(args: any) {
    const fields = args.fields.map((field: SchemaField) => {
      let def = `\`${field.name}\` ${field.type.toUpperCase()}`;
      if (field.length) def += `(${field.length})`;
      if (field.nullable === false) def += ' NOT NULL';
      if (field.default !== undefined) {
        def += ` DEFAULT ${field.default === null ? 'NULL' : `'${field.default}'`}`;
      }
      if (field.autoIncrement) def += ' AUTO_INCREMENT';
      if (field.primary) def += ' PRIMARY KEY';
      return def;
    });

    const indexes = args.indexes?.map((idx: IndexDefinition) => {
      const type = idx.unique ? 'UNIQUE INDEX' : 'INDEX';
      return `${type} \`${idx.name}\` (\`${idx.columns.join('`, `')}\`)`;
    }) || [];

    const sql = `CREATE TABLE \`${args.table}\` (
      ${[...fields, ...indexes].join(',\n      ')}
    )`;

    await this.executeQuery(sql);
    return {
      content: [
        {
          type: 'text',
          text: `Table ${args.table} created successfully`
        }
      ]
    };
  }

  private async handleAddColumn(args: any) {
    if (!args.table || !args.field) {
      throw new McpError(ErrorCode.InvalidParams, 'Table name and field are required');
    }

    let sql = `ALTER TABLE \`${args.table}\` ADD COLUMN \`${args.field.name}\` ${args.field.type.toUpperCase()}`;
    if (args.field.length) sql += `(${args.field.length})`;
    if (args.field.nullable === false) sql += ' NOT NULL';
    if (args.field.default !== undefined) {
      sql += ` DEFAULT ${args.field.default === null ? 'NULL' : `'${args.field.default}'`}`;
    }

    await this.executeQuery(sql);
    return {
      content: [
        {
          type: 'text',
          text: `Column ${args.field.name} added to table ${args.table}`
        }
      ]
    };
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('MySQL MCP server running on stdio');
  }
}

const server = new MySQLServer();
server.run().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
