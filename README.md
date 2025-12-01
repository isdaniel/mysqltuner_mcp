# MySQL Performance Tuning MCP
[![PyPI - Version](https://img.shields.io/pypi/v/mysqltuner-mcp)](https://pypi.org/project/mysqltuner-mcp/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/mysqltuner-mcp)](https://pypi.org/project/mysqltuner-mcp/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Pepy Total Downloads](https://img.shields.io/pepy/dt/mysqltuner-mcp)](https://pypi.org/project/mysqltuner-mcp/)
[![Docker Pulls](https://img.shields.io/docker/pulls/dog830228/mysqltuner_mcp)](https://hub.docker.com/r/dog830228/mysqltuner_mcp)

<a href="https://glama.ai/mcp/servers/@isdaniel/MySQL-Performance-Tuner-Mcp">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@isdaniel/MySQL-Performance-Tuner-Mcp/badge" />
</a>

A Model Context Protocol (MCP) server for MySQL performance tuning and analysis.

## Overview

`mysqltuner_mcp` provides AI-powered MySQL database performance analysis through the Model Context Protocol. It offers tools for query optimization, index recommendations, health monitoring

## Features

### Performance Analysis
- **Slow Query Detection**: Identify slow queries from performance_schema
- **Query Analysis**: Get detailed EXPLAIN plans with recommendations
- **Table Statistics**: Analyze table sizes, row counts, and fragmentation
- **Statement Analysis**: Analyze SQL statements for temp tables, sorting, and full scans

### Index Optimization
- **Index Recommendations**: AI-powered suggestions based on query patterns
- **Unused Index Finder**: Identify indexes that are never read
- **Duplicate Detection**: Find redundant and overlapping indexes
- **Index Statistics**: Cardinality, selectivity, and usage metrics

### Health Monitoring
- **Health Check**: Comprehensive database health assessment with scoring
- **Active Queries**: Real-time query monitoring
- **Wait Event Analysis**: Identify I/O and lock bottlenecks
- **Configuration Review**: Settings analysis with recommendations

### Storage Engine Analysis
- **Engine Statistics**: Analyze storage engine usage and distribution
- **Fragmentation Detection**: Find fragmented tables with OPTIMIZE recommendations
- **Auto-Increment Analysis**: Detect columns approaching overflow limits

### InnoDB Analysis
- **InnoDB Status**: Parse and analyze SHOW ENGINE INNODB STATUS
- **Buffer Pool Analysis**: Detailed buffer pool usage by schema and table
- **Transaction Analysis**: Monitor transactions, lock waits, and deadlocks

### Memory Analysis
- **Memory Calculations**: Calculate per-thread and global buffer usage
- **Memory by Host/User**: Breakdown memory usage by connection source
- **Table Cache Analysis**: Analyze table open cache efficiency

### Replication Monitoring
- **Master/Slave Status**: Monitor replication health and lag
- **Galera Cluster**: Full Galera cluster status for MariaDB/Percona
- **Group Replication**: MySQL Group Replication monitoring

### Security Analysis
- **Security Audit**: Check for anonymous users, weak passwords, dangerous privileges
- **User Privileges**: Analyze user privileges at all levels
- **Audit Log**: Check audit logging configuration

### Resources & Prompts
- Built-in best practices documentation
- Pre-configured prompts for common tuning tasks
- Index optimization guidelines
- Configuration optimization guide

## Installation

### From Source

```bash
git clone https://github.com/yourusername/mysqltuner_mcp.git
cd mysqltuner_mcp
pip install -e .
```

### Using pip (when published)

```bash
pip install mysqltuner_mcp
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MYSQL_URI` | MySQL connection URI (required) | - |
| `MYSQL_POOL_SIZE` | Connection pool size | `5` |
| `MYSQL_SSL` | Enable SSL/TLS connection | `false` |
| `MYSQL_SSL_CA` | Path to CA certificate file | - |
| `MYSQL_SSL_CERT` | Path to client certificate file | - |
| `MYSQL_SSL_KEY` | Path to client private key file | - |
| `MYSQL_SSL_VERIFY_CERT` | Verify server certificate | `true` |
| `MYSQL_SSL_VERIFY_IDENTITY` | Verify server hostname matches certificate | `false` |

### Connection URI Format

#### Environment Variables

```bash
export MYSQL_URI="mysql://user:password@host:3306/database"
export MYSQL_SSL=true
export MYSQL_SSL_CA="/path/to/ca.pem"  # Optional: CA certificate for verification
```

#### Connection URI Query Parameters

```bash
export MYSQL_URI="mysql://user:password@host:3306/database?ssl=true&ssl_ca=/path/to/ca.pem"
```

## Usage

### Running the Server

The server supports three transport modes: **stdio** (default), **SSE**, and **streamable-http**.

```bash
# As a module
python -m mysqltuner_mcp

# Using the entry point
mysqltuner-mcp

# Explicitly specifying stdio mode
python -m mysqltuner_mcp --mode stdio
```

#### SSE Mode (Server-Sent Events)

HTTP transport using Server-Sent Events, suitable for web-based MCP clients:

```bash
# Start SSE server on default port 8080
python -m mysqltuner_mcp --mode sse

# Specify custom host and port
python -m mysqltuner_mcp --mode sse --host 127.0.0.1 --port 3000

# Enable debug mode
python -m mysqltuner_mcp --mode sse --debug
```

**SSE Endpoints:**
- `http://<host>:<port>/sse` - SSE connection endpoint
- `http://<host>:<port>/messages/` - Message posting endpoint

#### Streamable HTTP Mode

Modern HTTP transport with session management:

```bash
# Start streamable HTTP server (stateful, with session tracking)
python -m mysqltuner_mcp --mode streamable-http

# Start in stateless mode (fresh transport per request)
python -m mysqltuner_mcp --mode streamable-http --stateless

# Specify custom host and port
python -m mysqltuner_mcp --mode streamable-http --host 127.0.0.1 --port 3000
```

**Streamable HTTP Endpoint:**
- `http://<host>:<port>/mcp` - Single endpoint for all MCP communication

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mode` | Server mode: `stdio`, `sse`, or `streamable-http` | `stdio` |
| `--host` | Host to bind to (HTTP modes only) | `0.0.0.0` |
| `--port` | Port to listen on (HTTP modes only) | `8080` or `PORT` env var |
| `--stateless` | Run in stateless mode (streamable-http only) | `false` |
| `--debug` | Enable debug logging | `false` |

### MCP Client Configuration

Add to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "mysqltuner_mcp": {
      "command": "python",
      "args": ["-m", "mysqltuner_mcp"],
      "env": {
        "MYSQL_URI": "mysql://root:your_password@localhost:3306/your_database"
      }
    }
  }
}
```

#### With SSL/TLS Enabled

```json
{
  "mcpServers": {
    "mysqltuner_mcp": {
      "command": "python",
      "args": ["-m", "mysqltuner_mcp"],
      "env": {
        "MYSQL_URI": "mysql://root:your_password@localhost:3306/your_database",
        "MYSQL_SSL": "true",
        "MYSQL_SSL_CA": "/path/to/ca.pem"
      }
    }
  }
}
```

## Available Tools

### Performance Tools

| Tool | Description |
|------|-------------|
| `get_slow_queries` | Retrieve slow queries from performance_schema with detailed statistics |
| `analyze_query` | Get EXPLAIN plan and analysis for a query with optimization recommendations |
| `get_table_stats` | Get table statistics including size, row counts, fragmentation, and indexes |

### Index Tools

| Tool | Description |
|------|-------------|
| `get_index_recommendations` | AI-powered index suggestions based on query patterns from performance_schema |
| `find_unused_indexes` | Find unused, duplicate, and redundant indexes with DROP statements |
| `get_index_stats` | Detailed index statistics including cardinality, selectivity, and usage metrics |

### Health Tools

| Tool | Description |
|------|-------------|
| `check_database_health` | Comprehensive health check with scoring (connections, buffer pool, queries, etc.) |
| `get_active_queries` | Monitor currently running queries and identify long-running/blocked queries |
| `review_settings` | Analyze MySQL configuration settings with best practice recommendations |
| `analyze_wait_events` | Identify wait event bottlenecks (I/O, locks, buffer, log waits) |

### Storage Engine Tools

| Tool | Description |
|------|-------------|
| `analyze_storage_engines` | Analyze storage engine usage, statistics, and recommendations |
| `get_fragmented_tables` | Find tables with significant fragmentation and wasted space |
| `analyze_auto_increment` | Check auto-increment columns for potential overflow issues |

### InnoDB Tools

| Tool | Description |
|------|-------------|
| `get_innodb_status` | Parse and analyze SHOW ENGINE INNODB STATUS output |
| `analyze_buffer_pool` | Detailed InnoDB buffer pool analysis by schema and table |
| `analyze_innodb_transactions` | Analyze InnoDB transactions, lock waits, and deadlocks |

### Memory Tools

| Tool | Description |
|------|-------------|
| `calculate_memory_usage` | Calculate MySQL memory usage (per-thread and global buffers) |
| `get_memory_by_host` | Get memory usage breakdown by host, user, or event |
| `get_table_memory_usage` | Analyze table cache and InnoDB buffer pool by table |

### Replication Tools

| Tool | Description |
|------|-------------|
| `get_replication_status` | Get master/slave replication status and health |
| `get_galera_status` | Get Galera cluster status (MariaDB/Percona XtraDB Cluster) |
| `get_group_replication_status` | Get MySQL Group Replication status |

### Security Tools

| Tool | Description |
|------|-------------|
| `analyze_security` | Comprehensive security analysis (users, passwords, SSL, privileges) |
| `analyze_user_privileges` | Analyze privileges for specific users or all users |
| `check_audit_log` | Check audit log configuration and status |

### Statement Analysis Tools

| Tool | Description |
|------|-------------|
| `analyze_statements` | Comprehensive SQL statement analysis from performance_schema |
| `get_statements_with_temp_tables` | Find statements creating temporary tables (memory and disk) |
| `get_statements_with_sorting` | Find statements with sorting operations and file sorts |
| `get_statements_with_full_scans` | Find statements performing full table scans |
| `get_statements_with_errors` | Find statements producing errors or warnings |

## Available Prompts

| Prompt | Description |
|--------|-------------|
| `optimize_slow_query` | Analyze and optimize a slow query |
| `health_check` | Perform comprehensive health assessment |
| `index_review` | Review indexes for a database |
| `performance_audit` | Full performance audit |

## Requirements

- Python 3.10+
- MySQL 5.7+ or MySQL 8.0+
- `performance_schema` enabled (for full functionality)

### MySQL Permissions

The MySQL user needs the following privileges:

```sql
GRANT SELECT ON performance_schema.* TO 'your_user'@'%';
GRANT SELECT ON information_schema.* TO 'your_user'@'%';
GRANT PROCESS ON *.* TO 'your_user'@'%';
-- For EXPLAIN on user databases:
GRANT SELECT ON your_database.* TO 'your_user'@'%';
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/yourusername/mysqltuner_mcp.git
cd mysqltuner_mcp
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e .
```
