import type { ConnectionManager } from '../connection/index.js';
import type { QueryArgs } from '../types/index.js';
import { validateSqlInput } from '../utils/validation.js';

export async function handleQuery(
  connectionManager: ConnectionManager,
  args: QueryArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateSqlInput(args.sql, ['SELECT']);
  const rows = await connectionManager.query(args.sql, args.params || []);
  return [{ type: 'text', text: JSON.stringify(rows, null, 2) }];
}

export async function handleExecute(
  connectionManager: ConnectionManager,
  args: QueryArgs
): Promise<{ type: 'text'; text: string }[]> {
  validateSqlInput(args.sql, ['INSERT', 'UPDATE', 'DELETE']);
  const result = await connectionManager.query(args.sql, args.params || []);
  return [{ type: 'text', text: JSON.stringify(result, null, 2) }];
}
