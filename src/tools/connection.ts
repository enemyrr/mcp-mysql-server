import type { ConnectionManager } from '../connection/index.js';
import type { ConnectionArgs } from '../types/index.js';

export async function handleConnectDb(
  connectionManager: ConnectionManager,
  args: ConnectionArgs
): Promise<{ type: 'text'; text: string }[]> {
  const result = await connectionManager.connect(args);

  const portInfo = result.port ? `:${result.port}` : '';
  return [
    {
      type: 'text',
      text: `Successfully connected to database "${result.database}" at ${result.host}${portInfo}`,
    },
  ];
}
