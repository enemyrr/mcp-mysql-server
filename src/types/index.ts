export interface DatabaseConfig {
  host: string;
  port?: number;
  user: string;
  password: string;
  database: string;
}

export interface SSLConfig {
  ca?: string;
  cert?: string;
  key?: string;
  rejectUnauthorized?: boolean;
}

export interface ConnectionConfig extends DatabaseConfig {
  ssl?: SSLConfig;
  connectionTimeout?: number;
  connectRetry?: {
    maxAttempts: number;
    delay: number;
  };
}

// Schema definition types
export interface SchemaField {
  name: string;
  type: string;
  length?: number;
  nullable?: boolean;
  default?: string | number | null;
  autoIncrement?: boolean;
  primary?: boolean;
}

export interface IndexDefinition {
  name: string;
  columns: string[];
  unique?: boolean;
}

// Query types
export interface QueryResult {
  content: Array<{
    type: 'text';
    text: string;
  }>;
}

export interface QueryArgs {
  sql: string;
  params?: Array<string | number | boolean | null>;
}

// Connection argument types
export interface ConnectionArgs {
  url?: string;
  workspace?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
}

// Table operation types
export interface CreateTableArgs {
  table: string;
  fields: SchemaField[];
  indexes?: IndexDefinition[];
}

export interface DescribeTableArgs {
  table: string;
}

export interface AlterColumnArgs {
  table: string;
  column: string;
  type?: string;
  length?: number;
  nullable?: boolean;
  default?: string | number | null;
  newName?: string;
}

export interface DropColumnArgs {
  table: string;
  column: string;
}

export interface AddColumnArgs {
  table: string;
  field: SchemaField;
}

export interface DropTableArgs {
  table: string;
  confirm: boolean;
}

export interface TruncateTableArgs {
  table: string;
  confirm: boolean;
}

export interface GetIndexesArgs {
  table: string;
}

export interface GetForeignKeysArgs {
  table: string;
}

// Column information from INFORMATION_SCHEMA
export interface ColumnInfo {
  Field: string;
  Type: string;
  Null: string;
  Key: string;
  Default: string | null;
  Extra: string;
  Comment: string;
}

// Index information from INFORMATION_SCHEMA
export interface IndexInfo {
  Table: string;
  Non_unique: number;
  Key_name: string;
  Seq_in_index: number;
  Column_name: string;
  Collation: string | null;
  Cardinality: number | null;
  Sub_part: number | null;
  Packed: string | null;
  Null: string;
  Index_type: string;
  Comment: string;
}

// Foreign key information
export interface ForeignKeyInfo {
  CONSTRAINT_NAME: string;
  TABLE_NAME: string;
  COLUMN_NAME: string;
  REFERENCED_TABLE_NAME: string;
  REFERENCED_COLUMN_NAME: string;
}

// Logger types
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogEntry {
  level: LogLevel;
  message: string;
  timestamp: string;
  data?: Record<string, unknown>;
}
