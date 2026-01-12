import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';

export type SqlCommandType =
  | 'SELECT'
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'CREATE'
  | 'ALTER'
  | 'DROP'
  | 'TRUNCATE'
  | 'SHOW'
  | 'DESCRIBE';

export function validateSqlInput(sql: string, allowedTypes: SqlCommandType[]): void {
  const trimmed = sql.trim();
  if (!trimmed) {
    throw new McpError(ErrorCode.InvalidParams, 'SQL query cannot be empty');
  }

  const type = trimmed.split(/\s+/)[0].toUpperCase() as SqlCommandType;
  if (!allowedTypes.includes(type)) {
    throw new McpError(
      ErrorCode.InvalidParams,
      `Invalid SQL command type. Expected: ${allowedTypes.join(', ')}, got: ${type}`
    );
  }
}

export function validateTableName(name: string): void {
  if (!name || typeof name !== 'string') {
    throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(name)) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid table name. Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
    );
  }
}

export function validateColumnName(name: string): void {
  if (!name || typeof name !== 'string') {
    throw new McpError(ErrorCode.InvalidParams, 'Column name is required');
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new McpError(
      ErrorCode.InvalidParams,
      'Invalid column name. Must start with a letter or underscore and contain only alphanumeric characters and underscores.'
    );
  }
}

const VALID_MYSQL_TYPES = [
  'TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT',
  'DECIMAL', 'DEC', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL',
  'BIT', 'BOOLEAN', 'BOOL',
  'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR',
  'CHAR', 'VARCHAR', 'BINARY', 'VARBINARY',
  'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB',
  'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT',
  'ENUM', 'SET',
  'JSON',
  'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
];

export function validateColumnType(type: string): void {
  if (!type || typeof type !== 'string') {
    throw new McpError(ErrorCode.InvalidParams, 'Column type is required');
  }

  const baseType = type.toUpperCase().split('(')[0].trim();
  if (!VALID_MYSQL_TYPES.includes(baseType)) {
    throw new McpError(
      ErrorCode.InvalidParams,
      `Invalid MySQL column type: ${type}. Valid types include: ${VALID_MYSQL_TYPES.slice(0, 10).join(', ')}...`
    );
  }
}

export function escapeIdentifier(identifier: string): string {
  return `\`${identifier.replace(/`/g, '``')}\``;
}

export function validateRequired<T extends Record<string, unknown>>(
  obj: T,
  requiredFields: (keyof T)[]
): void {
  for (const field of requiredFields) {
    if (obj[field] === undefined || obj[field] === null) {
      throw new McpError(
        ErrorCode.InvalidParams,
        `Missing required field: ${String(field)}`
      );
    }
  }
}
