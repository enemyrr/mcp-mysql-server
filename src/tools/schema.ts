import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import type { ConnectionManager } from '../connection/index.js';
import type {
  SchemaField,
  IndexDefinition,
  CreateTableArgs,
  DescribeTableArgs,
  AddColumnArgs,
  AlterColumnArgs,
  DropColumnArgs,
  DropTableArgs,
  TruncateTableArgs,
  GetIndexesArgs,
  GetForeignKeysArgs,
} from '../types/index.js';
import { validateTableName, validateColumnName, escapeIdentifier } from '../utils/validation.js';

function buildColumnDefinition(field: SchemaField): string {
  validateColumnName(field.name);

  let def = `${escapeIdentifier(field.name)} ${field.type.toUpperCase()}`;

  if (field.length) {
    def += `(${field.length})`;
  }
  if (field.nullable === false) {
    def += ' NOT NULL';
  }
  if (field.default !== undefined) {
    if (field.default === null) {
      def += ' DEFAULT NULL';
    } else if (typeof field.default === 'number') {
      def += ` DEFAULT ${field.default}`;
    } else {
      const escaped = String(field.default).replace(/'/g, "''");
      def += ` DEFAULT '${escaped}'`;
    }
  }
  if (field.autoIncrement) {
    def += ' AUTO_INCREMENT';
  }
  if (field.primary) {
    def += ' PRIMARY KEY';
  }

  return def;
}

export async function handleListTables(
  connectionManager: ConnectionManager
): Promise<{ type: 'text'; text: string }[]> {
  const rows = await connectionManager.query('SHOW TABLES');

  return [
    {
      type: 'text',
      text: JSON.stringify(rows, null, 2),
    },
  ];
}

export async function handleDescribeTable(
  connectionManager: ConnectionManager,
  args: DescribeTableArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  const database = connectionManager.getDatabaseName();
  if (!database) {
    throw new McpError(ErrorCode.InvalidRequest, 'Not connected to a database');
  }

  const rows = await connectionManager.query<Record<string, unknown>[]>(
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
    [database, args.table]
  );

  const formattedRows = rows.map((row) => ({
    ...row,
    Null: row.Null === 'YES' ? 'YES' : 'NO',
  }));

  return [
    {
      type: 'text',
      text: JSON.stringify(formattedRows, null, 2),
    },
  ];
}

export async function handleCreateTable(
  connectionManager: ConnectionManager,
  args: CreateTableArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  if (!args.fields || args.fields.length === 0) {
    throw new McpError(ErrorCode.InvalidParams, 'At least one field is required');
  }

  const fields = args.fields.map(buildColumnDefinition);

  const indexes =
    args.indexes?.map((idx: IndexDefinition) => {
      const type = idx.unique ? 'UNIQUE INDEX' : 'INDEX';
      const columns = idx.columns.map(escapeIdentifier).join(', ');
      return `${type} ${escapeIdentifier(idx.name)} (${columns})`;
    }) || [];

  const sql = `CREATE TABLE ${escapeIdentifier(args.table)} (
    ${[...fields, ...indexes].join(',\n    ')}
  )`;

  await connectionManager.query(sql);

  return [
    {
      type: 'text',
      text: `Table "${args.table}" created successfully`,
    },
  ];
}

export async function handleAddColumn(
  connectionManager: ConnectionManager,
  args: AddColumnArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  if (!args.field) {
    throw new McpError(ErrorCode.InvalidParams, 'Field definition is required');
  }

  const columnDef = buildColumnDefinition(args.field);
  const sql = `ALTER TABLE ${escapeIdentifier(args.table)} ADD COLUMN ${columnDef}`;

  await connectionManager.query(sql);

  return [
    {
      type: 'text',
      text: `Column "${args.field.name}" added to table "${args.table}"`,
    },
  ];
}

export async function handleAlterColumn(
  connectionManager: ConnectionManager,
  args: AlterColumnArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);
  validateColumnName(args.column);

  if (!args.type && args.nullable === undefined && args.default === undefined && !args.newName) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'At least one modification (type, nullable, default, or newName) is required'
    );
  }

  // Build the MODIFY/CHANGE COLUMN statement
  let sql: string;
  const columnType = args.type?.toUpperCase() || 'VARCHAR(255)';
  let typeDef = columnType;

  if (args.length) {
    typeDef = `${columnType}(${args.length})`;
  }

  if (args.nullable === false) {
    typeDef += ' NOT NULL';
  }

  if (args.default !== undefined) {
    if (args.default === null) {
      typeDef += ' DEFAULT NULL';
    } else if (typeof args.default === 'number') {
      typeDef += ` DEFAULT ${args.default}`;
    } else {
      const escaped = String(args.default).replace(/'/g, "''");
      typeDef += ` DEFAULT '${escaped}'`;
    }
  }

  if (args.newName) {
    validateColumnName(args.newName);
    sql = `ALTER TABLE ${escapeIdentifier(args.table)} CHANGE COLUMN ${escapeIdentifier(args.column)} ${escapeIdentifier(args.newName)} ${typeDef}`;
  } else {
    sql = `ALTER TABLE ${escapeIdentifier(args.table)} MODIFY COLUMN ${escapeIdentifier(args.column)} ${typeDef}`;
  }

  await connectionManager.query(sql);

  const message = args.newName
    ? `Column "${args.column}" renamed to "${args.newName}" and modified in table "${args.table}"`
    : `Column "${args.column}" modified in table "${args.table}"`;

  return [
    {
      type: 'text',
      text: message,
    },
  ];
}

export async function handleDropColumn(
  connectionManager: ConnectionManager,
  args: DropColumnArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);
  validateColumnName(args.column);

  const sql = `ALTER TABLE ${escapeIdentifier(args.table)} DROP COLUMN ${escapeIdentifier(args.column)}`;
  await connectionManager.query(sql);

  return [
    {
      type: 'text',
      text: `Column "${args.column}" dropped from table "${args.table}"`,
    },
  ];
}

export async function handleDropTable(
  connectionManager: ConnectionManager,
  args: DropTableArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  if (!args.confirm) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Confirmation required: set confirm to true to drop the table'
    );
  }

  const sql = `DROP TABLE ${escapeIdentifier(args.table)}`;
  await connectionManager.query(sql);

  return [
    {
      type: 'text',
      text: `Table "${args.table}" dropped successfully`,
    },
  ];
}

export async function handleTruncateTable(
  connectionManager: ConnectionManager,
  args: TruncateTableArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  if (!args.confirm) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Confirmation required: set confirm to true to truncate the table'
    );
  }

  const sql = `TRUNCATE TABLE ${escapeIdentifier(args.table)}`;
  await connectionManager.query(sql);

  return [
    {
      type: 'text',
      text: `Table "${args.table}" truncated successfully`,
    },
  ];
}

export async function handleListDatabases(
  connectionManager: ConnectionManager
): Promise<{ type: 'text'; text: string }[]> {
  const rows = await connectionManager.query('SHOW DATABASES');

  return [
    {
      type: 'text',
      text: JSON.stringify(rows, null, 2),
    },
  ];
}

export async function handleGetIndexes(
  connectionManager: ConnectionManager,
  args: GetIndexesArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  const rows = await connectionManager.query(`SHOW INDEX FROM ${escapeIdentifier(args.table)}`);

  return [
    {
      type: 'text',
      text: JSON.stringify(rows, null, 2),
    },
  ];
}

export async function handleGetForeignKeys(
  connectionManager: ConnectionManager,
  args: GetForeignKeysArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateTableName(args.table);

  const database = connectionManager.getDatabaseName();
  if (!database) {
    throw new McpError(ErrorCode.InvalidRequest, 'Not connected to a database');
  }

  const rows = await connectionManager.query(
    `SELECT
      CONSTRAINT_NAME,
      COLUMN_NAME,
      REFERENCED_TABLE_NAME,
      REFERENCED_COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL`,
    [database, args.table]
  );

  return [
    {
      type: 'text',
      text: JSON.stringify(rows, null, 2),
    },
  ];
}
