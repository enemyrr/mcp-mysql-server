import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { ConnectionManager } from './connection/index.js';
import { registerTools } from './tools/index.js';
import { registerResources } from './resources.js';
import { registerPrompts } from './prompts.js';
import { logger } from './utils/logger.js';

export class MySQLServer {
  private server: Server;
  private connectionManager: ConnectionManager;

  constructor() {
    this.server = new Server(
      { name: 'mysql-server', version: '0.2.0' },
      { capabilities: { tools: {}, resources: {}, prompts: {} } }
    );
    this.connectionManager = new ConnectionManager();
    this.setupHandlers();
    this.setupErrorHandlers();
  }

  private setupHandlers(): void {
    registerTools(this.server, this.connectionManager);
    registerResources(this.server, this.connectionManager);
    registerPrompts(this.server, this.connectionManager);
    logger.info('All MCP handlers registered');
  }

  private setupErrorHandlers(): void {
    this.server.onerror = (error) => {
      logger.error('MCP Server error', {
        error: error instanceof Error ? error.message : String(error),
      });
    };

    const shutdown = async (signal: string) => {
      logger.info(`Received ${signal}, shutting down...`);
      await this.cleanup();
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    process.on('uncaughtException', (error) => {
      logger.error('Uncaught exception', { error: error.message, stack: error.stack });
      process.exit(1);
    });

    process.on('unhandledRejection', (reason) => {
      logger.error('Unhandled rejection', {
        reason: reason instanceof Error ? reason.message : String(reason),
      });
    });
  }

  private async cleanup(): Promise<void> {
    logger.info('Cleaning up resources...');

    try {
      await this.connectionManager.close();
    } catch (error) {
      logger.error('Error closing connection', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    try {
      await this.server.close();
    } catch (error) {
      logger.error('Error closing server', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    logger.info('Cleanup complete');
  }

  async run(): Promise<void> {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    logger.info('MySQL MCP server running on stdio');
  }
}
