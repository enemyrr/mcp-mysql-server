#!/usr/bin/env node
import { config } from 'dotenv';
import { MySQLServer } from './server.js';
import { logger } from './utils/logger.js';

config();

const server = new MySQLServer();

server.run().catch((error) => {
  logger.error('Fatal error starting server', {
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  });
  process.exit(1);
});
