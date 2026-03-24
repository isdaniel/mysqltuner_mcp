"""
MySQL Tuner MCP Server.

Main server implementation that integrates all tools with the MCP protocol.
Provides prompts and resources for MySQL performance tuning.
Supports stdio, SSE, and streamable-http MCP server modes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from mcp.server import Server
from mcp.types import (
    CompleteResult,
    Completion,
    GetPromptResult,
    Prompt,
    PromptArgument,
    PromptMessage,
    PromptReference,
    Resource,
    ResourceTemplateReference,
    TextContent,
    Tool,
)

# HTTP-related imports (imported conditionally)
try:
    import uvicorn
    from mcp.server.sse import SseServerTransport
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.middleware.cors import CORSMiddleware
    from starlette.requests import Request
    from starlette.routing import Mount, Route
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False

from .services import DbConnPool, SqlDriver
from .tools import (
    # Health tools
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
    SettingsReviewToolHandler,
    WaitEventsToolHandler,
    # Performance tools
    AnalyzeQueryToolHandler,
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
    # Index tools
    IndexRecommendationsToolHandler,
    IndexStatsToolHandler,
    UnusedIndexesToolHandler,
    # InnoDB tools
    InnoDBStatusToolHandler,
    InnoDBBufferPoolToolHandler,
    InnoDBTransactionsToolHandler,
    # Statement analysis tools
    StatementAnalysisToolHandler,
    StatementsTempTablesToolHandler,
    StatementsSortingToolHandler,
    StatementsFullScansToolHandler,
    StatementErrorsToolHandler,
    LongQueryTypeCollationIssuesToolHandler,
    # Memory tools
    MemoryCalculationsToolHandler,
    MemoryByHostToolHandler,
    TableMemoryUsageToolHandler,
    # Storage engine tools
    StorageEngineAnalysisToolHandler,
    FragmentedTablesToolHandler,
    AutoIncrementAnalysisToolHandler,
    # Replication tools
    ReplicationStatusToolHandler,
    GaleraClusterToolHandler,
    GroupReplicationToolHandler,
    # Security tools
    SecurityAnalysisToolHandler,
    UserPrivilegesToolHandler,
    AuditLogToolHandler,
    # Diagnostic tools
    ConnectionAnalysisToolHandler,
    TableLockAnalysisToolHandler,
    TempTableAnalysisToolHandler,
    PerfSchemaConfigToolHandler,
    OptimizerConfigToolHandler,
    # Schema & binlog tools
    SchemaProfilingToolHandler,
    BinlogAnalysisToolHandler,
    GlobalStatusSnapshotToolHandler,
    # Base class
    ToolHandler,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mysqltuner_mcp")


@dataclass
class ServerConfig:
    """Server configuration loaded from environment."""

    mysql_uri: str
    pool_size: int = 5
    ssl_enabled: bool = False
    ssl_ca: str | None = None
    ssl_cert: str | None = None
    ssl_key: str | None = None
    ssl_verify_cert: bool = True
    ssl_verify_identity: bool = False

    @classmethod
    def from_env(cls) -> "ServerConfig":
        """
        Load configuration from environment variables.

        Environment Variables:
            MYSQL_URI: MySQL connection URI (required)
                Format: mysql://user:password@host:port/database
                Can include SSL params: mysql://user:pass@host/db?ssl=true&ssl_ca=/path/to/ca.pem
            MYSQL_POOL_SIZE: Connection pool size (default: 5)
            MYSQL_SSL: Enable SSL/TLS (true/false, default: false)
            MYSQL_SSL_CA: Path to CA certificate file
            MYSQL_SSL_CERT: Path to client certificate file
            MYSQL_SSL_KEY: Path to client private key file
            MYSQL_SSL_VERIFY_CERT: Verify server certificate (true/false, default: true)
            MYSQL_SSL_VERIFY_IDENTITY: Verify server hostname (true/false, default: false)
        """
        uri = os.getenv("MYSQL_URI")

        if not uri:
            raise ValueError(
                "MYSQL_URI environment variable is required. "
                "Format: mysql://user:password@host:port/database"
            )

        def get_bool_env(name: str, default: bool = False) -> bool:
            """Parse a boolean environment variable."""
            val = os.getenv(name, "").lower()
            if not val:
                return default
            return val in ("true", "1", "yes", "on")

        return cls(
            mysql_uri=uri,
            pool_size=int(os.getenv("MYSQL_POOL_SIZE", "5")),
            ssl_enabled=get_bool_env("MYSQL_SSL"),
            ssl_ca=os.getenv("MYSQL_SSL_CA"),
            ssl_cert=os.getenv("MYSQL_SSL_CERT"),
            ssl_key=os.getenv("MYSQL_SSL_KEY"),
            ssl_verify_cert=get_bool_env("MYSQL_SSL_VERIFY_CERT", default=True),
            ssl_verify_identity=get_bool_env("MYSQL_SSL_VERIFY_IDENTITY"),
        )


class MySQLTunerServer:
    """MySQL Tuner MCP Server implementation."""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.server = Server("mysqltuner_mcp")
        self.db_pool: DbConnPool | None = None
        self.sql_driver: SqlDriver | None = None
        self.tools: dict[str, ToolHandler] = {}

        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Set up MCP protocol handlers."""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """Return list of available tools."""
            return [handler.get_tool_definition() for handler in self.tools.values()]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any] | None) -> list[TextContent]:
            """Execute a tool by name."""
            if name not in self.tools:
                return [TextContent(type="text", text=f"Unknown tool: {name}")]

            handler = self.tools[name]
            args = arguments or {}

            try:
                result = await handler.run_tool(args)
                return list(result)
            except Exception as e:
                logger.exception(f"Error executing tool {name}")
                return [TextContent(type="text", text=f"Error: {e!s}")]

        @self.server.list_prompts()
        async def list_prompts() -> list[Prompt]:
            """Return list of available prompts."""
            return self._get_prompts()

        @self.server.get_prompt()
        async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
            """Get a prompt by name."""
            return await self._get_prompt_result(name, arguments or {})

        @self.server.list_resources()
        async def list_resources() -> list[Resource]:
            """Return list of available resources."""
            return self._get_resources()

        @self.server.read_resource()
        async def read_resource(uri: str) -> str:
            """Read a resource by URI."""
            return await self._read_resource(uri)

        @self.server.completion()
        async def handle_completion(
            ref: PromptReference | ResourceTemplateReference,
            argument: dict[str, str],
            context: dict[str, str] | None = None
        ) -> CompleteResult:
            """Handle completion requests for prompts and resource templates."""
            return await self._get_completions(ref, argument, context)

    async def _get_completions(
        self,
        ref: PromptReference | ResourceTemplateReference,
        argument: dict[str, str],
        context: dict[str, str] | None = None
    ) -> CompleteResult:
        """
        Provide completions for prompt arguments.

        Args:
            ref: Reference to prompt or resource template
            argument: The argument being completed (name and current value)
            context: Optional additional context

        Returns:
            CompleteResult with suggested completions
        """
        completions: list[str] = []

        # Handle prompt completions
        if isinstance(ref, PromptReference):
            prompt_name = ref.name
            arg_name = argument.get("name", "")
            arg_value = argument.get("value", "")

            if prompt_name == "optimize_slow_query":
                if arg_name == "table_name":
                    # Suggest common table-related completions
                    completions = ["users", "orders", "products", "sessions", "logs"]
                    if arg_value:
                        completions = [c for c in completions if c.startswith(arg_value.lower())]

            elif prompt_name == "health_check":
                if arg_name == "focus_area":
                    completions = ["memory", "connections", "queries", "all", "innodb", "replication"]
                    if arg_value:
                        completions = [c for c in completions if c.startswith(arg_value.lower())]

            elif prompt_name == "index_review":
                if arg_name == "schema_name":
                    # Could potentially query for actual schemas, for now provide common suggestions
                    completions = ["public", "app", "analytics", "logs"]
                    if arg_value:
                        completions = [c for c in completions if c.startswith(arg_value.lower())]

        # Handle resource template completions (if any resource templates are added in the future)
        elif isinstance(ref, ResourceTemplateReference):
            # Currently no resource templates defined, return empty
            pass

        return CompleteResult(
            completion=Completion(
                values=completions[:100],  # Limit to 100 suggestions
                total=len(completions),
                hasMore=len(completions) > 100
            )
        )

    async def initialize(self) -> None:
        """Initialize database connection and tools."""
        logger.info("Initializing MySQL connection pool...")

        # Build SSL kwargs from config (environment variables take precedence)
        ssl_kwargs = {}
        if self.config.ssl_enabled:
            ssl_kwargs["ssl_enabled"] = True
        if self.config.ssl_ca:
            ssl_kwargs["ssl_ca"] = self.config.ssl_ca
        if self.config.ssl_cert:
            ssl_kwargs["ssl_cert"] = self.config.ssl_cert
        if self.config.ssl_key:
            ssl_kwargs["ssl_key"] = self.config.ssl_key
        if self.config.ssl_verify_cert is not None:
            ssl_kwargs["ssl_verify_cert"] = self.config.ssl_verify_cert
        if self.config.ssl_verify_identity is not None:
            ssl_kwargs["ssl_verify_identity"] = self.config.ssl_verify_identity

        self.db_pool = DbConnPool.from_uri(
            self.config.mysql_uri,
            minsize=1,
            maxsize=self.config.pool_size,
            **ssl_kwargs
        )

        await self.db_pool.initialize()
        self.sql_driver = SqlDriver(self.db_pool)

        # Register tools
        self._register_tools()

        logger.info(f"Registered {len(self.tools)} tools")

    def _register_tools(self) -> None:
        """Register all tool handlers."""
        if not self.sql_driver:
            raise RuntimeError("SQL driver not initialized")

        tool_classes: list[type[ToolHandler]] = [
            # Performance tools
            GetSlowQueriesToolHandler,
            AnalyzeQueryToolHandler,
            TableStatsToolHandler,
            # Index tools
            IndexRecommendationsToolHandler,
            UnusedIndexesToolHandler,
            IndexStatsToolHandler,
            # Health tools
            DatabaseHealthToolHandler,
            ActiveQueriesToolHandler,
            SettingsReviewToolHandler,
            WaitEventsToolHandler,
            # InnoDB analysis tools
            InnoDBStatusToolHandler,
            InnoDBBufferPoolToolHandler,
            InnoDBTransactionsToolHandler,
            # Statement analysis tools
            StatementAnalysisToolHandler,
            StatementsTempTablesToolHandler,
            StatementsSortingToolHandler,
            StatementsFullScansToolHandler,
            StatementErrorsToolHandler,
            LongQueryTypeCollationIssuesToolHandler,
            # Memory calculation tools
            MemoryCalculationsToolHandler,
            MemoryByHostToolHandler,
            TableMemoryUsageToolHandler,
            # Storage engine tools
            StorageEngineAnalysisToolHandler,
            FragmentedTablesToolHandler,
            AutoIncrementAnalysisToolHandler,
            # Replication tools
            ReplicationStatusToolHandler,
            GaleraClusterToolHandler,
            GroupReplicationToolHandler,
            # Security tools
            SecurityAnalysisToolHandler,
            UserPrivilegesToolHandler,
            AuditLogToolHandler,
            # Diagnostic tools
            ConnectionAnalysisToolHandler,
            TableLockAnalysisToolHandler,
            TempTableAnalysisToolHandler,
            PerfSchemaConfigToolHandler,
            OptimizerConfigToolHandler,
            # Schema & binlog tools
            SchemaProfilingToolHandler,
            BinlogAnalysisToolHandler,
            GlobalStatusSnapshotToolHandler,
        ]

        for tool_class in tool_classes:
            handler = tool_class(self.sql_driver)
            self.tools[handler.name] = handler
            logger.debug(f"Registered tool: {handler.name}")

    def _get_prompts(self) -> list[Prompt]:
        """Get available prompts for MySQL tuning."""
        return [
            Prompt(
                name="optimize_slow_query",
                description="Analyze and optimize a slow SQL query",
                arguments=[
                    PromptArgument(
                        name="query",
                        description="The SQL query to optimize",
                        required=True
                    ),
                    PromptArgument(
                        name="table_name",
                        description="Primary table involved (optional)",
                        required=False
                    )
                ]
            ),
            Prompt(
                name="health_check",
                description="Perform a comprehensive MySQL health check",
                arguments=[
                    PromptArgument(
                        name="focus_area",
                        description="Specific area to focus on (memory, connections, queries)",
                        required=False
                    )
                ]
            ),
            Prompt(
                name="index_review",
                description="Review indexes for a database schema",
                arguments=[
                    PromptArgument(
                        name="schema_name",
                        description="Database schema to review",
                        required=False
                    )
                ]
            ),
            Prompt(
                name="performance_audit",
                description="Full performance audit of MySQL server",
                arguments=[]
            ),
            Prompt(
                name="connection_tuning",
                description="Analyze and tune MySQL connection settings",
                arguments=[]
            ),
            Prompt(
                name="innodb_deep_dive",
                description="Deep-dive analysis of InnoDB internals",
                arguments=[]
            ),
            Prompt(
                name="lock_contention_diagnosis",
                description="Diagnose lock contention and blocking issues",
                arguments=[]
            ),
            Prompt(
                name="capacity_planning",
                description="Capacity planning analysis for MySQL server",
                arguments=[
                    PromptArgument(
                        name="growth_period",
                        description="Planning period (e.g., '6months', '1year')",
                        required=False
                    )
                ]
            ),
        ]

    async def _get_prompt_result(self, name: str, arguments: dict[str, str]) -> GetPromptResult:
        """Generate prompt result based on name and arguments."""

        if name == "optimize_slow_query":
            query = arguments.get("query", "")
            table = arguments.get("table_name", "")

            context = f"""You are a MySQL performance expert. Analyze and optimize the following query:

Query: {query}
{"Table: " + table if table else ""}

Please:
1. Use the analyze_query tool to get the execution plan
2. Use the get_index_recommendations tool to suggest indexes
3. Identify potential performance issues
4. Provide optimization recommendations
5. Suggest rewritten queries if applicable"""

            return GetPromptResult(
                description="Optimize a slow MySQL query",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "health_check":
            focus = arguments.get("focus_area", "all")

            context = f"""You are a MySQL DBA expert. Perform a comprehensive health check.

Focus area: {focus}

Please:
1. Use the check_database_health tool to get overall health metrics
2. Use the review_settings tool to check configuration
3. Use the get_active_queries tool if there are performance issues
4. Use the analyze_wait_events tool to identify bottlenecks
5. Provide prioritized recommendations for improvement"""

            return GetPromptResult(
                description="MySQL health check",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "index_review":
            schema = arguments.get("schema_name", "")

            context = f"""You are a MySQL index optimization expert. Review indexes for the database.

{"Schema: " + schema if schema else "Analyze the current database"}

Please:
1. Use the find_unused_indexes tool to identify unused indexes
2. Use the get_index_recommendations tool to suggest new indexes
3. Use the get_index_stats tool to analyze existing indexes
4. Identify redundant and duplicate indexes
5. Provide index optimization recommendations with CREATE/DROP statements"""

            return GetPromptResult(
                description="MySQL index review",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "performance_audit":
            context = """You are a MySQL performance expert conducting a full audit.
Read the mysql://tuner/perf-tuning-workflow resource first for the recommended methodology.

Please perform a comprehensive performance audit using this sequence:

Phase 1 - Prerequisites:
1. Use check_perf_schema_config to verify performance_schema is properly configured

Phase 2 - Overall Health:
2. Use check_database_health for overall health scoring
3. Use review_settings to analyze configuration against best practices
4. Use calculate_memory_usage to check memory allocation vs physical RAM

Phase 3 - Query Analysis:
5. Use get_slow_queries to find the most expensive queries
6. Use get_statements_with_full_scans for queries doing full table scans
7. Use get_statements_with_temp_tables for disk temp table spills
8. Use get_statements_with_sorting for expensive sorts

Phase 4 - Index & Schema:
9. Use find_unused_indexes to find wasted indexes
10. Use get_index_recommendations to suggest missing indexes
11. Use profile_schema_sizes to understand data distribution

Phase 5 - InnoDB & Locks:
12. Use analyze_buffer_pool for buffer pool efficiency
13. Use analyze_wait_events to identify bottlenecks
14. Use analyze_table_locks for lock contention
15. Use get_innodb_status for InnoDB internals

Phase 6 - Connections & Resources:
16. Use analyze_connections for connection state analysis
17. Use analyze_temp_tables for temp table optimization
18. Use get_global_status_snapshot for throughput baseline

Phase 7 - Security & Replication:
19. Use analyze_security for security posture
20. Use get_replication_status if applicable

Provide a detailed report with:
- Executive summary with health score
- Critical issues requiring immediate attention
- Top 5 most impactful optimizations
- Configuration change recommendations with specific values
- Index optimization DDL (CREATE/DROP statements)
- Capacity planning notes
- Action items prioritized by impact and effort"""

            return GetPromptResult(
                description="Full MySQL performance audit",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "connection_tuning":
            context = """You are a MySQL connection tuning expert.

Please perform a comprehensive connection analysis:

1. Use analyze_connections to get connection state breakdown (by state, then by user)
2. Use check_database_health for connection-related health metrics
3. Use review_settings with category 'connections' to review connection settings
4. Use calculate_memory_usage to understand per-connection memory impact
5. Use get_global_status_snapshot with category 'connections' for connection counters

Analyze and recommend:
- Optimal max_connections setting based on actual peak usage
- wait_timeout and interactive_timeout tuning
- thread_cache_size optimization
- Connection pool recommendations for the application tier
- Whether connection pooling middleware (ProxySQL, MySQL Router) is needed
- Memory impact of current connection settings
- Solutions for any sleeping connection bloat"""

            return GetPromptResult(
                description="MySQL connection tuning analysis",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "innodb_deep_dive":
            context = """You are an InnoDB internals expert.

Please perform a deep-dive analysis of InnoDB:

1. Use get_innodb_status to parse SHOW ENGINE INNODB STATUS
2. Use analyze_buffer_pool for buffer pool page analysis
3. Use analyze_innodb_transactions for active transaction analysis
4. Use review_settings with category 'innodb' for InnoDB configuration
5. Use analyze_wait_events with category 'io' for I/O wait analysis
6. Use analyze_wait_events with category 'lock' for lock wait analysis
7. Use calculate_memory_usage for memory allocation details

Analyze and recommend:
- Buffer pool sizing (is it too small/large?)
- Buffer pool hit ratio optimization
- InnoDB log file sizing (redo log)
- innodb_flush_log_at_trx_commit trade-offs
- I/O capacity settings (innodb_io_capacity, innodb_io_capacity_max)
- Adaptive hash index effectiveness
- Purge lag and history list length
- Any deadlock patterns detected"""

            return GetPromptResult(
                description="InnoDB deep-dive analysis",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "lock_contention_diagnosis":
            context = """You are a MySQL lock contention specialist.

Please diagnose lock contention issues:

1. Use analyze_table_locks to get lock wait statistics and metadata locks
2. Use analyze_innodb_transactions to find long-running transactions
3. Use get_active_queries to see currently running/blocked queries
4. Use analyze_wait_events with category 'lock' for lock-related waits
5. Use get_innodb_status to check for deadlocks
6. Use analyze_connections to find idle transactions holding locks

Diagnose and recommend:
- Identify the root cause of lock contention (DDL vs DML, long transactions, etc.)
- Find tables with the highest lock contention
- Detect any deadlock patterns and suggest resolution
- Recommend transaction isolation level changes if applicable
- Suggest application-level changes (shorter transactions, lock ordering)
- Review lock_wait_timeout and innodb_lock_wait_timeout settings
- Identify metadata lock issues blocking DDL operations"""

            return GetPromptResult(
                description="Lock contention diagnosis",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        elif name == "capacity_planning":
            period = arguments.get("growth_period", "6 months")

            context = f"""You are a MySQL capacity planning expert.
Planning period: {period}

Please perform capacity planning analysis:

1. Use profile_schema_sizes to understand current data sizes and distribution
2. Use get_global_status_snapshot for current throughput baseline
3. Use calculate_memory_usage for current memory utilization
4. Use check_database_health for current server health
5. Use analyze_connections for connection usage patterns
6. Use analyze_auto_increment to check auto-increment headroom
7. Use analyze_binlog for replication and binlog disk usage

Analyze and recommend:
- Current storage usage and projected growth over {period}
- Memory requirements based on data growth projections
- Connection capacity analysis (current vs projected peak)
- Auto-increment column headroom for high-growth tables
- Binlog disk space projections
- Read/write ratio and trend analysis
- Recommendations for scaling strategy (vertical vs horizontal)
- When hardware upgrades or sharding may be needed"""

            return GetPromptResult(
                description="MySQL capacity planning analysis",
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(type="text", text=context)
                    )
                ]
            )

        return GetPromptResult(
            description="Unknown prompt",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=f"Unknown prompt: {name}")
                )
            ]
        )

    def _get_resources(self) -> list[Resource]:
        """Get available resources."""
        return [
            Resource(
                uri="mysql://tuner/best-practices",
                name="MySQL Best Practices",
                description="Best practices for MySQL performance tuning",
                mimeType="text/markdown"
            ),
            Resource(
                uri="mysql://tuner/index-guidelines",
                name="Index Guidelines",
                description="Guidelines for MySQL index optimization",
                mimeType="text/markdown"
            ),
            Resource(
                uri="mysql://tuner/configuration-guide",
                name="Configuration Guide",
                description="MySQL configuration optimization guide",
                mimeType="text/markdown"
            ),
            Resource(
                uri="mysql://tuner/perf-tuning-workflow",
                name="Performance Tuning Workflow",
                description="Step-by-step workflow for AI-driven MySQL performance tuning using this MCP server",
                mimeType="text/markdown"
            ),
            Resource(
                uri="mysql://tuner/tool-reference",
                name="Tool Quick Reference",
                description="Quick reference card for all 39 MCP tools with usage patterns",
                mimeType="text/markdown"
            ),
        ]

    async def _read_resource(self, uri: str) -> str:
        """Read resource content by URI."""

        if uri == "mysql://tuner/best-practices":
            return """# MySQL Performance Best Practices

## Query Optimization
- Always use indexes for WHERE, JOIN, and ORDER BY clauses
- Avoid SELECT * - specify only needed columns
- Use EXPLAIN to analyze query execution plans
- Avoid functions on indexed columns in WHERE clauses
- Use prepared statements for repeated queries

## Index Strategy
- Create composite indexes for multi-column queries
- Place most selective columns first in composite indexes
- Remove unused and duplicate indexes
- Monitor index cardinality and selectivity
- Consider covering indexes for frequent queries

## Configuration
- Set innodb_buffer_pool_size to 70-80% of available RAM
- Enable slow query log with appropriate threshold
- Configure thread_cache_size appropriately
- Use innodb_flush_log_at_trx_commit=1 for ACID compliance
- Set appropriate max_connections based on workload

## Monitoring
- Monitor buffer pool hit ratio (aim for >95%)
- Track slow query count and patterns
- Watch for lock contention and long-running transactions
- Monitor connection usage and thread states
- Check for full table scans and temporary table creation"""

        elif uri == "mysql://tuner/index-guidelines":
            return """# MySQL Index Optimization Guidelines

## When to Create Indexes
- Columns frequently used in WHERE clauses
- Columns used in JOIN conditions
- Columns used in ORDER BY and GROUP BY
- High-cardinality columns (many unique values)

## Index Types
- **B-Tree**: Default, best for range queries and equality
- **Hash**: Only for memory tables, equality only
- **Full-Text**: For text searching
- **Spatial**: For geographic data

## Composite Index Rules
1. Most selective column first (highest cardinality)
2. Equality conditions before range conditions
3. Match the query's column order
4. Leftmost prefix rule applies

## Index Anti-Patterns
- Too many indexes (slows writes)
- Indexes on low-cardinality columns
- Unused indexes consuming space
- Redundant/overlapping indexes
- Missing indexes causing full table scans

## Maintenance
- Regularly analyze tables for statistics
- Remove unused indexes
- Monitor index size and fragmentation
- Rebuild indexes after large data changes"""

        elif uri == "mysql://tuner/configuration-guide":
            return """# MySQL Configuration Optimization Guide

## Memory Settings

### InnoDB Buffer Pool
```ini
innodb_buffer_pool_size = <70-80% of RAM>
innodb_buffer_pool_instances = <1 per GB of buffer pool>
```

### Sort and Join Buffers
```ini
sort_buffer_size = 256K to 2M
join_buffer_size = 256K to 1M
read_buffer_size = 128K to 256K
```

### Temporary Tables
```ini
tmp_table_size = 64M to 256M
max_heap_table_size = 64M to 256M
```

## InnoDB Settings

### Transaction Durability
```ini
innodb_flush_log_at_trx_commit = 1  # Full ACID
innodb_flush_log_at_trx_commit = 2  # Crash-safe with better performance
```

### I/O Configuration
```ini
innodb_io_capacity = 200 to 2000
innodb_io_capacity_max = 2000 to 4000
innodb_flush_method = O_DIRECT  # For Linux
```

## Connection Settings
```ini
max_connections = 100 to 500
thread_cache_size = 8 to 16
wait_timeout = 28800
interactive_timeout = 28800
```

## Logging
```ini
slow_query_log = ON
long_query_time = 1 to 2
log_queries_not_using_indexes = ON
```

## Performance Schema
```ini
performance_schema = ON  # Required for detailed monitoring
```"""

        elif uri == "mysql://tuner/perf-tuning-workflow":
            return """# MySQL Performance Tuning Workflow Guide

## How to Use This MCP Server for Performance Tuning

This guide teaches AI assistants the recommended methodology for MySQL performance
tuning using the mysqltuner_mcp MCP server's 39 tools.

---

## Quick Start: Which Prompt to Use?

| Scenario | Prompt to Use |
|----------|--------------|
| General performance issues | `performance_audit` |
| Specific slow query | `optimize_slow_query` |
| Index optimization | `index_review` |
| General health check | `health_check` |
| Connection issues (too many connections, timeouts) | `connection_tuning` |
| InnoDB buffer pool / redo log tuning | `innodb_deep_dive` |
| Lock waits, deadlocks, blocking queries | `lock_contention_diagnosis` |
| Planning for growth / resource sizing | `capacity_planning` |

---

## Step-by-Step Tuning Methodology

### Step 1: Verify Prerequisites
**Always start here.** Many tools depend on performance_schema.

```
Tool: check_perf_schema_config
```

If performance_schema is OFF, most diagnostic tools will return limited data.
Recommend enabling it before proceeding.

### Step 2: Get the Big Picture
Run these tools first to understand overall server health:

```
Tools (run in this order):
1. check_database_health        -> Overall health score
2. get_global_status_snapshot    -> Throughput baseline (QPS, read/write ratio)
3. review_settings               -> Configuration review
4. calculate_memory_usage        -> Memory allocation check
```

The health score gives you a quick triage:
- **90-100**: Healthy, focus on fine-tuning
- **70-89**: Warning, address specific issues
- **50-69**: Degraded, multiple issues need attention
- **0-49**: Critical, immediate action required

### Step 3: Analyze Queries (The #1 Impact Area)
Bad queries are almost always the top performance bottleneck.

```
Tools:
1. get_slow_queries              -> Top slow queries by total time
2. analyze_statements            -> Statement analysis from sys schema
3. get_statements_with_full_scans -> Queries doing full table scans
4. get_statements_with_temp_tables -> Queries spilling to disk
5. get_statements_with_sorting    -> Queries with expensive sorts
6. get_statements_with_errors     -> Queries generating errors
```

For each problematic query found:
```
Tools:
1. analyze_query (with the SQL)   -> Get EXPLAIN plan
2. get_index_recommendations      -> Suggest missing indexes
3. analyze_long_queries_for_type_collation_issues -> Check for implicit conversions
```

### Step 4: Optimize Indexes
Indexes are the second-highest impact area.

```
Tools:
1. find_unused_indexes           -> Remove wasted indexes (saves write I/O)
2. get_index_recommendations     -> Add missing indexes
3. get_index_stats               -> Check index selectivity
4. get_table_stats               -> Table-level statistics
```

**Key principle**: Remove unused indexes FIRST, then add new ones.
Each unnecessary index slows down every INSERT/UPDATE/DELETE.

### Step 5: Tune InnoDB
InnoDB is the default storage engine and the core of MySQL performance.

```
Tools:
1. analyze_buffer_pool           -> Buffer pool hit ratio and sizing
2. get_innodb_status             -> InnoDB internals (log, deadlocks)
3. analyze_innodb_transactions   -> Long-running transactions
```

**Key metrics**:
- Buffer pool hit ratio should be >99% for OLTP workloads
- If hit ratio < 95%, increase innodb_buffer_pool_size
- History list length > 1000 indicates purge lag

### Step 6: Check Connections and Locks
Connection and lock issues cause intermittent performance problems.

```
Tools:
1. analyze_connections           -> Connection state breakdown
2. analyze_table_locks           -> Lock contention analysis
3. get_active_queries            -> Currently running queries
4. analyze_wait_events           -> Wait event bottlenecks
```

### Step 7: Review Storage and Capacity
Understand data distribution and plan for growth.

```
Tools:
1. profile_schema_sizes          -> Database and table sizes
2. analyze_storage_engines       -> Engine distribution
3. get_fragmented_tables         -> Tables needing OPTIMIZE
4. analyze_auto_increment        -> Auto-increment headroom
5. analyze_binlog                -> Binary log sizing
```

### Step 8: Security Review
Performance and security go hand-in-hand.

```
Tools:
1. analyze_security              -> Security score and checks
2. analyze_user_privileges       -> Privilege analysis
3. check_audit_log               -> Audit logging status
```

---

## Common Problem Patterns and Tool Combinations

### "The server is slow" (General Slowness)
1. `check_database_health` -> Get health score
2. `get_slow_queries` -> Find top slow queries
3. `analyze_wait_events` -> Find bottleneck type (I/O, locks, CPU)
4. Branch based on findings:
   - I/O bottleneck -> `analyze_buffer_pool`, `review_settings` (innodb)
   - Lock bottleneck -> `analyze_table_locks`, `analyze_innodb_transactions`
   - CPU/query bottleneck -> `analyze_query` per slow query

### "Too many connections" Error
1. `analyze_connections` -> Breakdown by state and user
2. `calculate_memory_usage` -> Memory per connection
3. `review_settings` (connections) -> Current limits
4. Usually caused by sleeping connections or missing connection pooling

### "Disk space running out"
1. `profile_schema_sizes` -> Find large databases/tables
2. `get_fragmented_tables` -> Find reclaimable space
3. `analyze_binlog` -> Check binlog accumulation
4. `find_unused_indexes` -> Indexes wasting space

### "Deadlocks occurring"
1. `get_innodb_status` -> Parse deadlock info
2. `analyze_innodb_transactions` -> Active transactions
3. `analyze_table_locks` -> Lock patterns
4. `get_active_queries` -> Concurrent queries

### "Replication lag"
1. `get_replication_status` -> Current lag metrics
2. `get_slow_queries` -> Queries causing lag on replica
3. `analyze_binlog` -> Binlog throughput
4. `review_settings` (replication) -> Replication configuration

---

## Tuning Priority Order (Impact vs Effort)

1. **Query optimization** (Highest impact, variable effort)
   - Tools: get_slow_queries, analyze_query, get_index_recommendations
2. **Index optimization** (High impact, low effort)
   - Tools: find_unused_indexes, get_index_recommendations
3. **Buffer pool sizing** (High impact, low effort)
   - Tools: analyze_buffer_pool, calculate_memory_usage
4. **Connection tuning** (Medium impact, low effort)
   - Tools: analyze_connections, review_settings
5. **Temp table optimization** (Medium impact, medium effort)
   - Tools: analyze_temp_tables, get_statements_with_temp_tables
6. **Lock contention** (Variable impact, medium effort)
   - Tools: analyze_table_locks, analyze_innodb_transactions
7. **Storage optimization** (Low-medium impact, medium effort)
   - Tools: get_fragmented_tables, profile_schema_sizes
"""

        elif uri == "mysql://tuner/tool-reference":
            return """# MySQL Tuner MCP Server - Tool Quick Reference

## 39 Available Tools by Category

### Performance Analysis (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `get_slow_queries` | Top slow queries from performance_schema | Find worst queries |
| `analyze_query` | EXPLAIN plan analysis (JSON/tree/traditional) | Deep-dive one query |
| `get_table_stats` | Table and index metadata from information_schema | Table-level stats |

### Index Tools (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `get_index_recommendations` | Suggest missing indexes from query patterns | Add new indexes |
| `find_unused_indexes` | Find zero-read, duplicate, redundant indexes | Remove waste |
| `get_index_stats` | Index cardinality, selectivity, I/O stats | Evaluate indexes |

### Health Monitoring (4 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `check_database_health` | Health score (0-100) with breakdown | Quick triage |
| `get_active_queries` | Currently running queries from PROCESSLIST | Live monitoring |
| `review_settings` | Configuration review vs best practices | Config audit |
| `analyze_wait_events` | Wait event analysis from performance_schema | Find bottlenecks |

### InnoDB Analysis (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `get_innodb_status` | Parse SHOW ENGINE INNODB STATUS | InnoDB internals |
| `analyze_buffer_pool` | Buffer pool pages, hit ratio, per-table breakdown | Memory tuning |
| `analyze_innodb_transactions` | Active transactions, lock waits | Transaction issues |

### Statement Analysis (6 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `analyze_statements` | Top statements from sys.statement_analysis | Statement ranking |
| `get_statements_with_temp_tables` | Queries creating disk temp tables | Temp table spills |
| `get_statements_with_sorting` | Queries with expensive sorts | Sort optimization |
| `get_statements_with_full_scans` | Queries doing full table scans | Missing indexes |
| `get_statements_with_errors` | Queries generating errors/warnings | Error patterns |
| `analyze_long_queries_for_type_collation_issues` | Implicit type/collation mismatches | Hidden perf bugs |

### Memory Analysis (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `calculate_memory_usage` | Global + per-thread memory calculation | Memory sizing |
| `get_memory_by_host` | Memory usage by client host | Per-host analysis |
| `get_table_memory_usage` | Table cache and buffer pool per-table | Table memory |

### Storage Engine (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `analyze_storage_engines` | Engine distribution, non-InnoDB detection | Engine audit |
| `get_fragmented_tables` | Tables with high DATA_FREE ratio | Space reclaim |
| `analyze_auto_increment` | Auto-increment overflow risk detection | Capacity risk |

### Replication (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `get_replication_status` | Master/replica status | Replication health |
| `get_galera_status` | Galera cluster status (wsrep) | Galera monitoring |
| `get_group_replication_status` | MySQL Group Replication | GR monitoring |

### Security (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `analyze_security` | Security score with 8 checks | Security audit |
| `analyze_user_privileges` | Per-user privilege breakdown | Privilege review |
| `check_audit_log` | Audit plugin enablement | Compliance check |

### Diagnostic (5 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `analyze_connections` | Connection states, per-user/host breakdown | Connection issues |
| `analyze_table_locks` | Table lock contention, metadata locks | Lock diagnosis |
| `analyze_temp_tables` | Temp table disk spill analysis | Temp table tuning |
| `check_perf_schema_config` | Performance Schema enablement check | Prerequisites |
| `review_optimizer_config` | Optimizer switches and cost model | Query plan tuning |

### Schema & Binlog (3 tools)
| Tool | Description | Key Use Case |
|------|------------|-------------|
| `profile_schema_sizes` | Database/table size profiling | Capacity planning |
| `analyze_binlog` | Binary log config and throughput | Binlog management |
| `get_global_status_snapshot` | Curated global status counters | Baseline metrics |

---

## Tool Usage Tips

1. **Always start with `check_perf_schema_config`** to verify prerequisites
2. **Use `check_database_health` for quick triage** - the health score tells you severity
3. **Call `get_global_status_snapshot` twice with a delay** to compute delta rates
4. **Run `find_unused_indexes` before `get_index_recommendations`** - remove waste first
5. **Use `analyze_query` with format='json'** for the most detailed EXPLAIN output
6. **Check `analyze_connections` when seeing connection errors** - breakdown by state reveals idle bloat
7. **Use `review_optimizer_config`** when query plans are suboptimal despite good indexes
"""

        return f"Unknown resource: {uri}"

    async def shutdown(self) -> None:
        """Cleanup resources on shutdown."""
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database connection pool closed")

    @asynccontextmanager
    async def run_context(self) -> AsyncIterator[None]:
        """Context manager for server lifecycle."""
        await self.initialize()
        try:
            yield
        finally:
            await self.shutdown()

    async def run_stdio(self) -> None:
        """Run server using stdio transport."""
        from mcp.server.stdio import stdio_server

        async with self.run_context():
            logger.info("Starting MySQL Tuner MCP Server (stdio mode)")
            async with stdio_server() as (read_stream, write_stream):
                await self.server.run(
                    read_stream,
                    write_stream,
                    self.server.create_initialization_options()
                )


def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> "Starlette":
    """
    Create a Starlette application that can serve the provided mcp server with SSE.

    Args:
        mcp_server: The MCP server instance
        debug: Whether to enable debug mode

    Returns:
        Starlette application instance
    """
    if not HTTP_AVAILABLE:
        raise RuntimeError("HTTP dependencies not available. Install with: pip install starlette uvicorn")

    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )


def create_streamable_http_app(
    mcp_server: Server, *, debug: bool = False, stateless: bool = False
) -> "Starlette":
    """
    Create a Starlette application with StreamableHTTPSessionManager.
    Implements the MCP Streamable HTTP protocol with a single /mcp endpoint.

    Args:
        mcp_server: The MCP server instance
        debug: Whether to enable debug mode
        stateless: If True, creates a fresh transport for each request with no session tracking

    Returns:
        Starlette application instance
    """
    if not HTTP_AVAILABLE:
        raise RuntimeError("HTTP dependencies not available. Install with: pip install starlette uvicorn")

    # Create the session manager
    session_manager = StreamableHTTPSessionManager(
        app=mcp_server,
        event_store=None,  # No event store for now (no resumability)
        json_response=False,
        stateless=stateless,
    )

    class StreamableHTTPRoute:
        """ASGI app wrapper for the streamable HTTP handler"""
        async def __call__(self, scope, receive, send):
            await session_manager.handle_request(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app: "Starlette") -> AsyncIterator[None]:
        """Context manager for session manager lifecycle."""
        async with session_manager.run():
            logger.info("Streamable HTTP session manager started!")
            try:
                yield
            finally:
                logger.info("Streamable HTTP session manager shutting down...")

    # Create Starlette app with a single endpoint
    starlette_app = Starlette(
        debug=debug,
        routes=[
            Route("/mcp", endpoint=StreamableHTTPRoute()),
        ],
        lifespan=lifespan,
    )

    # Add CORS middleware
    starlette_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["mcp-session-id", "mcp-protocol-version"],
        max_age=86400,
    )

    return starlette_app


async def run_server(
    mysql_server: MySQLTunerServer,
    mode: str,
    host: str = "0.0.0.0",
    port: int = 8080,
    debug: bool = False,
    stateless: bool = False
) -> None:
    """
    Unified server runner that supports stdio, SSE, and streamable-http modes.

    Args:
        mysql_server: The MySQLTunerServer instance (already initialized)
        mode: Server mode ("stdio", "sse", or "streamable-http")
        host: Host to bind to (HTTP modes only)
        port: Port to listen on (HTTP modes only)
        debug: Whether to enable debug mode
        stateless: Whether to use stateless mode (streamable-http only)
    """
    if mode == "stdio":
        from mcp.server.stdio import stdio_server

        logger.info("Starting MySQL Tuner MCP Server (stdio mode)")
        async with stdio_server() as (read_stream, write_stream):
            await mysql_server.server.run(
                read_stream,
                write_stream,
                mysql_server.server.create_initialization_options()
            )

    elif mode == "sse":
        if not HTTP_AVAILABLE:
            raise RuntimeError(
                "SSE mode requires additional dependencies. "
                "Install with: pip install starlette uvicorn"
            )

        logger.info(f"Starting MySQL Tuner MCP Server (SSE mode) on {host}:{port}...")
        logger.info(f"Endpoints: http://{host}:{port}/sse, http://{host}:{port}/messages/")

        # Create Starlette app with SSE transport
        starlette_app = create_starlette_app(mysql_server.server, debug=debug)

        # Configure uvicorn
        config = uvicorn.Config(
            app=starlette_app,
            host=host,
            port=port,
            log_level="debug" if debug else "info"
        )

        # Run the server
        server = uvicorn.Server(config)
        await server.serve()

    elif mode == "streamable-http":
        if not HTTP_AVAILABLE:
            raise RuntimeError(
                "Streamable HTTP mode requires additional dependencies. "
                "Install with: pip install starlette uvicorn"
            )

        mode_desc = "stateless" if stateless else "stateful"
        logger.info(f"Starting MySQL Tuner MCP Server (Streamable HTTP, {mode_desc}) on {host}:{port}...")
        logger.info(f"Endpoint: http://{host}:{port}/mcp")

        # Create Starlette app with Streamable HTTP transport
        starlette_app = create_streamable_http_app(mysql_server.server, debug=debug, stateless=stateless)

        # Configure uvicorn
        config = uvicorn.Config(
            app=starlette_app,
            host=host,
            port=port,
            log_level="debug" if debug else "info"
        )

        # Run the server (session manager lifecycle is handled by lifespan)
        server = uvicorn.Server(config)
        await server.serve()

    else:
        raise ValueError(f"Unknown mode: {mode}")


async def main() -> None:
    """
    Main entry point for the mysqltuner_mcp server.
    Supports stdio, SSE, and streamable-http modes based on command line arguments.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='mysqltuner_mcp: MySQL MCP Performance Tuning Server - supports stdio, SSE, and streamable-http modes'
    )
    parser.add_argument(
        '--mode',
        choices=['stdio', 'sse', 'streamable-http'],
        default='stdio',
        help='Server mode: stdio (default), sse, or streamable-http'
    )
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Host to bind to (HTTP modes only, default: 0.0.0.0)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=None,
        help='Port to listen on (HTTP modes only, default: from PORT env var or 8080)'
    )
    parser.add_argument(
        '--stateless',
        action='store_true',
        help='Run in stateless mode (streamable-http only, creates fresh transport per request)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )

    args = parser.parse_args()

    # Get port from environment variable or command line argument, or default to 8080
    port = args.port if args.port is not None else int(os.environ.get("PORT", 8080))

    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        # Load configuration from environment
        config = ServerConfig.from_env()
        mysql_server = MySQLTunerServer(config)

        # Initialize the server (database connection, tools registration)
        await mysql_server.initialize()

        logger.info(f"Starting mysqltuner_mcp server in {args.mode} mode...")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Registered tools: {list(mysql_server.tools.keys())}")

        # Run the server in the specified mode
        await run_server(mysql_server, args.mode, args.host, port, args.debug, args.stateless)

    except Exception as e:
        logger.exception(f"Failed to start server: {str(e)}")
        raise
    finally:
        if 'mysql_server' in locals():
            await mysql_server.shutdown()


def _configure_event_loop_policy() -> None:
    """
    Configure asyncio event loop policy for Windows SSL compatibility.

    On Windows, the default ProactorEventLoop doesn't properly support SSL
    handshakes with aiomysql. This is a known issue:
    https://github.com/aio-libs/aiomysql/issues/978

    When SSL is enabled on Windows, we need to use SelectorEventLoop instead.
    """
    if sys.platform != "win32":
        return

    ssl_enabled = os.getenv("MYSQL_SSL", "").lower() in ("true", "1", "yes", "on")
    ssl_in_uri = "ssl=true" in os.getenv("MYSQL_URI", "").lower() or "ssl_enabled=true" in os.getenv("MYSQL_URI", "").lower()

    if ssl_enabled or ssl_in_uri:
        logger.info("Windows detected with SSL enabled - using SelectorEventLoop for aiomysql compatibility")
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def run() -> None:
    """Synchronous entry point for scripts."""
    _configure_event_loop_policy()
    asyncio.run(main())


if __name__ == "__main__":
    run()
