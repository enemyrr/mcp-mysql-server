import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import { logger } from './logger.js';

function isErrorWithMessage(error: unknown): error is { message: string } {
  return (
    typeof error === 'object' &&
    error !== null &&
    'message' in error &&
    typeof (error as Record<string, unknown>).message === 'string'
  );
}

function getErrorMessage(error: unknown): string {
  if (isErrorWithMessage(error)) {
    return error.message;
  }
  return String(error);
}

const MYSQL_ERROR_MAP: Record<string, { code: ErrorCode; prefix: string }> = {
  ER_PARSE_ERROR: { code: ErrorCode.InvalidParams, prefix: 'Invalid SQL syntax' },
  ER_EMPTY_QUERY: { code: ErrorCode.InvalidParams, prefix: 'Empty query' },
  ER_ACCESS_DENIED_ERROR: { code: ErrorCode.InvalidRequest, prefix: 'Database authentication failed' },
  ER_BAD_DB_ERROR: { code: ErrorCode.InternalError, prefix: 'Database does not exist' },
  ER_NO_SUCH_TABLE: { code: ErrorCode.InvalidParams, prefix: 'Table does not exist' },
  ER_TABLE_EXISTS_ERROR: { code: ErrorCode.InvalidParams, prefix: 'Table already exists' },
  ER_BAD_FIELD_ERROR: { code: ErrorCode.InvalidParams, prefix: 'Unknown column' },
  ER_DUP_FIELDNAME: { code: ErrorCode.InvalidParams, prefix: 'Duplicate column name' },
  ECONNREFUSED: { code: ErrorCode.InternalError, prefix: 'Connection refused' },
  ETIMEDOUT: { code: ErrorCode.InternalError, prefix: 'Connection timed out' },
  ENOTFOUND: { code: ErrorCode.InternalError, prefix: 'Host not found' },
  PROTOCOL_CONNECTION_LOST: { code: ErrorCode.InternalError, prefix: 'Connection lost' },
  ER_DUP_ENTRY: { code: ErrorCode.InvalidParams, prefix: 'Duplicate entry' },
  ER_DATA_TOO_LONG: { code: ErrorCode.InvalidParams, prefix: 'Data too long for column' },
  ER_TRUNCATED_WRONG_VALUE: { code: ErrorCode.InvalidParams, prefix: 'Invalid value' },
  ER_TABLEACCESS_DENIED_ERROR: { code: ErrorCode.InvalidRequest, prefix: 'Table access denied' },
  ER_DBACCESS_DENIED_ERROR: { code: ErrorCode.InvalidRequest, prefix: 'Database access denied' },
};

export function handleDatabaseError(error: unknown): never {
  if (error instanceof Error) {
    const mysqlError = error as { code?: string; message: string; stack?: string };
    const code = mysqlError.code || '';

    const mapping = MYSQL_ERROR_MAP[code];
    if (mapping) {
      throw new McpError(mapping.code, `${mapping.prefix}: ${mysqlError.message}`);
    }

    logger.error('Unhandled MySQL error', {
      code,
      message: mysqlError.message,
      stack: mysqlError.stack,
    });
  }

  const message = getErrorMessage(error);
  throw new McpError(ErrorCode.InternalError, `Database error: ${message}`);
}
