# @enemyrr/mcp-mysql-server

[![smithery badge](https://smithery.ai/badge/@enemyrr/mcp-mysql-server)](https://smithery.ai/server/@enemyrr/mcp-mysql-server)

A Model Context Protocol server that provides MySQL database operations. This server enables AI models to interact with MySQL databases through a standardized interface.

<a href="https://glama.ai/mcp/servers/hcqqd3qi8q"><img width="380" height="200" src="https://glama.ai/mcp/servers/hcqqd3qi8q/badge" alt="MCP-MySQL Server MCP server" /></a>

## Installation & Setup for Cursor IDE

### Installing via Smithery

To install MySQL Database Server for Claude Desktop automatically via [Smithery](https://smithery.ai/server/@enemyrr/mcp-mysql-server):

```bash
npx -y @smithery/cli install @enemyrr/mcp-mysql-server --client claude
```

### Installing Manually
1. Clone and build the project:
```bash
git clone https://github.com/enemyrr/mcp-mysql-server.git
cd mcp-mysql-server
npm install
npm run build
```

2. Add the server in Cursor IDE settings:
   - Open Command Palette (Cmd/Ctrl + Shift + P)
   - Search for "MCP: Add Server"
   - Fill in the fields:
     - Name: `mysql`
     - Type: `command`
     - Command: `node /absolute/path/to/mcp-mysql-server/build/index.js`

> **Note**: Replace `/absolute/path/to/` with the actual path where you cloned and built the project.

## Database Configuration

You can configure the database connection in three ways:

1. **Database URL in .env** (Recommended):
```env
DATABASE_URL=mysql://user:password@host:3306/database
```

2. **Individual Parameters in .env**:
```env
DB_HOST=localhost
DB_USER=your_user
DB_PASSWORD=your_password
DB_DATABASE=your_database
```

3. **Direct Connection via Tool**:
```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "connect_db",
  arguments: {
    url: "mysql://user:password@host:3306/database"
    // OR
    workspace: "/path/to/your/project" // Will use project's .env
    // OR
    host: "localhost",
    user: "your_user",
    password: "your_password",
    database: "your_database"
  }
});
```

## Available Tools

### 1. connect_db
Connect to MySQL database using URL, workspace path, or direct credentials.

### 2. query
Execute SELECT queries with optional prepared statement parameters.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "query",
  arguments: {
    sql: "SELECT * FROM users WHERE id = ?",
    params: [1]
  }
});
```

### 3. execute
Execute INSERT, UPDATE, or DELETE queries with optional prepared statement parameters.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "execute",
  arguments: {
    sql: "INSERT INTO users (name, email) VALUES (?, ?)",
    params: ["John Doe", "john@example.com"]
  }
});
```

### 4. list_tables
List all tables in the connected database.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "list_tables"
});
```

### 5. describe_table
Get the structure of a specific table.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "describe_table",
  arguments: {
    table: "users"
  }
});
```

### 6. create_table
Create a new table with specified fields and indexes.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "create_table",
  arguments: {
    table: "users",
    fields: [
      {
        name: "id",
        type: "int",
        autoIncrement: true,
        primary: true
      },
      {
        name: "email",
        type: "varchar",
        length: 255,
        nullable: false
      }
    ],
    indexes: [
      {
        name: "email_idx",
        columns: ["email"],
        unique: true
      }
    ]
  }
});
```

### 7. add_column
Add a new column to an existing table.

```typescript
use_mcp_tool({
  server_name: "mysql",
  tool_name: "add_column",
  arguments: {
    table: "users",
    field: {
      name: "phone",
      type: "varchar",
      length: 20,
      nullable: true
    }
  }
});
```

## Features

- Multiple connection methods (URL, workspace, direct)
- Secure connection handling with automatic cleanup
- Prepared statement support for query parameters
- Schema management tools
- Comprehensive error handling and validation
- TypeScript support
- Automatic workspace detection

## Security

- Uses prepared statements to prevent SQL injection
- Supports secure password handling through environment variables
- Validates queries before execution
- Automatically closes connections when done

## Error Handling

The server provides detailed error messages for:
- Connection failures
- Invalid queries or parameters
- Missing configuration
- Database errors
- Schema validation errors

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request to https://github.com/enemyrr/mcp-mysql-server

## License

MIT
