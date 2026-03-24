"""
Diagnostic and deep-dive tool handlers for MySQL performance tuning.

Includes tools for:
- Connection state analysis (sleep vs active, aborted, per-user)
- Table lock analysis (metadata locks, lock waits)
- Temporary table and disk spill analysis
- Performance Schema configuration check
- Optimizer switch and configuration review
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class ConnectionAnalysisToolHandler(ToolHandler):
    """Tool handler for detailed connection state analysis."""

    name = "analyze_connections"
    title = "Connection Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze MySQL connection states and patterns for performance tuning.

Provides:
- Breakdown of connection states (Sleep, Query, Locked, etc.)
- Connections per user and per host
- Aborted clients and connections metrics
- Connection churn rate (connections/sec)
- Thread pool utilization (if available)
- Max used connections high watermark

Useful for:
- Diagnosing connection pool exhaustion
- Finding idle connections wasting resources
- Identifying connection storms
- Capacity planning for max_connections"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "group_by": {
                        "type": "string",
                        "description": "Group connections by user, host, state, or database",
                        "enum": ["user", "host", "state", "database"],
                        "default": "state"
                    },
                    "include_sleeping": {
                        "type": "boolean",
                        "description": "Include sleeping connections in detail",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            group_by = arguments.get("group_by", "state")
            include_sleeping = arguments.get("include_sleeping", True)

            status = await self.sql_driver.get_server_status()
            variables = await self.sql_driver.get_server_variables()

            output: dict[str, Any] = {
                "connection_overview": {},
                "breakdown": [],
                "aborted_stats": {},
                "recommendations": []
            }

            # Overview metrics
            max_conn = int(variables.get("max_connections", 151))
            threads_connected = int(status.get("Threads_connected", 0))
            threads_running = int(status.get("Threads_running", 0))
            threads_cached = int(status.get("Threads_cached", 0))
            max_used = int(status.get("Max_used_connections", 0))
            total_connections = int(status.get("Connections", 0))
            uptime = int(status.get("Uptime", 1))

            output["connection_overview"] = {
                "max_connections": max_conn,
                "current_connections": threads_connected,
                "running_threads": threads_running,
                "cached_threads": threads_cached,
                "max_used_connections": max_used,
                "max_used_pct": round(max_used / max_conn * 100, 2) if max_conn else 0,
                "current_usage_pct": round(threads_connected / max_conn * 100, 2) if max_conn else 0,
                "connections_per_sec": round(total_connections / uptime, 2) if uptime else 0,
                "total_connections_since_start": total_connections
            }

            # Aborted connections
            aborted_clients = int(status.get("Aborted_clients", 0))
            aborted_connects = int(status.get("Aborted_connects", 0))

            output["aborted_stats"] = {
                "aborted_clients": aborted_clients,
                "aborted_connects": aborted_connects,
                "aborted_client_pct": round(
                    aborted_clients / total_connections * 100, 4
                ) if total_connections else 0,
                "aborted_connect_pct": round(
                    aborted_connects / total_connections * 100, 4
                ) if total_connections else 0
            }

            # Connection breakdown
            column_map = {
                "user": "USER",
                "host": "HOST",
                "state": "COMMAND",
                "database": "DB"
            }
            group_col = column_map.get(group_by, "COMMAND")

            base_query = f"""
                SELECT
                    {group_col} as group_key,
                    COUNT(*) as connection_count,
                    SUM(CASE WHEN COMMAND = 'Sleep' THEN 1 ELSE 0 END) as sleeping,
                    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active,
                    MAX(TIME) as max_time_sec,
                    AVG(TIME) as avg_time_sec
                FROM information_schema.PROCESSLIST
            """

            if not include_sleeping:
                base_query += " WHERE COMMAND != 'Sleep'"

            base_query += f" GROUP BY {group_col} ORDER BY connection_count DESC LIMIT 50"

            results = await self.sql_driver.execute_query(base_query)

            for row in results:
                output["breakdown"].append({
                    "group": row["group_key"] or "(none)",
                    "total": row["connection_count"],
                    "sleeping": row["sleeping"],
                    "active": row["active"],
                    "max_time_sec": row["max_time_sec"],
                    "avg_time_sec": round(float(row["avg_time_sec"] or 0), 2)
                })

            # Recommendations
            if threads_connected / max_conn > 0.8:
                output["recommendations"].append(
                    f"Connection usage at {threads_connected}/{max_conn} ({threads_connected / max_conn * 100:.0f}%). "
                    "Consider increasing max_connections or using connection pooling."
                )

            if max_used / max_conn > 0.9:
                output["recommendations"].append(
                    f"Max used connections ({max_used}) is very close to max_connections ({max_conn}). "
                    "Server may have rejected connections."
                )

            if aborted_connects > total_connections * 0.01:
                output["recommendations"].append(
                    f"High aborted connections ({aborted_connects}). "
                    "Check authentication failures, network issues, or max_connections limit."
                )

            if aborted_clients > total_connections * 0.05:
                output["recommendations"].append(
                    f"High aborted clients ({aborted_clients}). "
                    "Applications may not be closing connections properly."
                )

            sleeping_count = sum(b["sleeping"] for b in output["breakdown"])
            if sleeping_count > threads_connected * 0.8 and threads_connected > 10:
                output["recommendations"].append(
                    f"{sleeping_count} of {threads_connected} connections are sleeping. "
                    "Consider reducing wait_timeout or using connection pooling."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class TableLockAnalysisToolHandler(ToolHandler):
    """Tool handler for analyzing table lock contention."""

    name = "analyze_table_locks"
    title = "Table Lock Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze MySQL table lock contention and metadata lock issues.

Provides:
- Table lock wait statistics
- Metadata lock holders and waiters
- Table lock vs row lock ratio
- Lock wait timeout metrics
- Tables with highest lock contention

Useful for:
- Diagnosing DDL/DML lock conflicts
- Finding tables causing lock waits
- Identifying metadata lock bottlenecks
- Tuning lock_wait_timeout"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Filter by specific schema name",
                        "default": ""
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_filter = arguments.get("schema", "")

            status = await self.sql_driver.get_server_status()
            variables = await self.sql_driver.get_server_variables()

            output: dict[str, Any] = {
                "lock_overview": {},
                "table_lock_waits": [],
                "metadata_locks": [],
                "recommendations": []
            }

            # Global lock statistics
            table_locks_waited = int(status.get("Table_locks_waited", 0))
            table_locks_immediate = int(status.get("Table_locks_immediate", 0))
            total_locks = table_locks_waited + table_locks_immediate
            lock_wait_pct = (
                round(table_locks_waited / total_locks * 100, 4) if total_locks else 0
            )

            innodb_row_lock_waits = int(status.get("Innodb_row_lock_waits", 0))
            innodb_row_lock_time_avg = int(status.get("Innodb_row_lock_time_avg", 0))
            innodb_row_lock_time = int(status.get("Innodb_row_lock_time", 0))
            innodb_row_lock_current_waits = int(
                status.get("Innodb_row_lock_current_waits", 0)
            )

            output["lock_overview"] = {
                "table_locks_immediate": table_locks_immediate,
                "table_locks_waited": table_locks_waited,
                "table_lock_wait_pct": lock_wait_pct,
                "innodb_row_lock_waits": innodb_row_lock_waits,
                "innodb_row_lock_time_avg_ms": innodb_row_lock_time_avg,
                "innodb_row_lock_time_total_ms": innodb_row_lock_time,
                "innodb_row_lock_current_waits": innodb_row_lock_current_waits,
                "lock_wait_timeout": variables.get("lock_wait_timeout", "unknown"),
                "innodb_lock_wait_timeout": variables.get(
                    "innodb_lock_wait_timeout", "unknown"
                ),
            }

            # Tables with highest lock waits from performance_schema
            lock_query = """
                SELECT
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    COUNT_READ as read_locks,
                    COUNT_WRITE as write_locks,
                    COUNT_READ_NORMAL as read_normal,
                    COUNT_WRITE_ALLOW_WRITE as write_allow_write,
                    SUM_TIMER_WAIT / 1000000000 as total_wait_ms,
                    SUM_TIMER_READ / 1000000000 as read_wait_ms,
                    SUM_TIMER_WRITE / 1000000000 as write_wait_ms
                FROM performance_schema.table_lock_waits_summary_by_table
                WHERE COUNT_STAR > 0
            """

            params: list[Any] = []
            if schema_filter:
                lock_query += " AND OBJECT_SCHEMA = %s"
                params.append(schema_filter)

            system_schemas = (
                "'mysql','information_schema','performance_schema','sys'"
            )
            lock_query += (
                f" AND OBJECT_SCHEMA NOT IN ({system_schemas})"
                " ORDER BY total_wait_ms DESC LIMIT 20"
            )

            try:
                lock_results = await self.sql_driver.execute_query(
                    lock_query, params if params else None
                )
                for row in lock_results:
                    output["table_lock_waits"].append({
                        "schema": row["OBJECT_SCHEMA"],
                        "table": row["OBJECT_NAME"],
                        "read_locks": row["read_locks"],
                        "write_locks": row["write_locks"],
                        "total_wait_ms": round(float(row["total_wait_ms"] or 0), 2),
                        "read_wait_ms": round(float(row["read_wait_ms"] or 0), 2),
                        "write_wait_ms": round(float(row["write_wait_ms"] or 0), 2),
                    })
            except Exception:
                output["table_lock_waits_error"] = (
                    "Could not query performance_schema.table_lock_waits_summary_by_table"
                )

            # Current metadata locks
            mdl_query = """
                SELECT
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    OBJECT_TYPE,
                    LOCK_TYPE,
                    LOCK_DURATION,
                    LOCK_STATUS,
                    OWNER_THREAD_ID
                FROM performance_schema.metadata_locks
                WHERE OBJECT_SCHEMA NOT IN ('mysql', 'information_schema',
                                            'performance_schema', 'sys')
                ORDER BY LOCK_STATUS, OBJECT_SCHEMA, OBJECT_NAME
                LIMIT 50
            """

            try:
                mdl_results = await self.sql_driver.execute_query(mdl_query)
                for row in mdl_results:
                    output["metadata_locks"].append({
                        "schema": row["OBJECT_SCHEMA"],
                        "object": row["OBJECT_NAME"],
                        "object_type": row["OBJECT_TYPE"],
                        "lock_type": row["LOCK_TYPE"],
                        "duration": row["LOCK_DURATION"],
                        "status": row["LOCK_STATUS"],
                        "owner_thread": row["OWNER_THREAD_ID"],
                    })
            except Exception:
                output["metadata_locks_note"] = (
                    "metadata_locks table not available (requires setup_instruments)"
                )

            # Recommendations
            if lock_wait_pct > 1:
                output["recommendations"].append(
                    f"Table lock contention at {lock_wait_pct:.2f}%. "
                    "Consider converting MyISAM tables to InnoDB for row-level locking."
                )

            if innodb_row_lock_time_avg > 1000:
                output["recommendations"].append(
                    f"Average InnoDB row lock wait is {innodb_row_lock_time_avg}ms. "
                    "Review transactions for lock escalation patterns."
                )

            if innodb_row_lock_current_waits > 0:
                output["recommendations"].append(
                    f"{innodb_row_lock_current_waits} current InnoDB row lock waits. "
                    "Check for long-running transactions holding locks."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class TempTableAnalysisToolHandler(ToolHandler):
    """Tool handler for temporary table and disk spill analysis."""

    name = "analyze_temp_tables"
    title = "Temp Table Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze MySQL temporary table usage and disk spills.

Provides:
- In-memory vs on-disk temp table ratios
- Top queries creating disk temp tables
- tmp_table_size and max_heap_table_size analysis
- Internal tmp table engine configuration
- Recommendations for reducing disk temp tables

Useful for:
- Identifying queries that spill to disk
- Tuning tmp_table_size and max_heap_table_size
- Reducing I/O from disk-based temp tables"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top queries to return",
                        "default": 15
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            top_n = arguments.get("top_n", 15)

            status = await self.sql_driver.get_server_status()
            variables = await self.sql_driver.get_server_variables()

            output: dict[str, Any] = {
                "overview": {},
                "configuration": {},
                "top_disk_temp_queries": [],
                "recommendations": []
            }

            # Global temp table stats
            created_tmp_tables = int(status.get("Created_tmp_tables", 0))
            created_tmp_disk_tables = int(status.get("Created_tmp_disk_tables", 0))
            created_tmp_files = int(status.get("Created_tmp_files", 0))
            disk_pct = (
                round(created_tmp_disk_tables / created_tmp_tables * 100, 2)
                if created_tmp_tables else 0
            )

            output["overview"] = {
                "total_tmp_tables_created": created_tmp_tables,
                "disk_tmp_tables": created_tmp_disk_tables,
                "disk_tmp_pct": disk_pct,
                "tmp_files_created": created_tmp_files,
                "memory_tmp_tables": created_tmp_tables - created_tmp_disk_tables,
            }

            # Configuration
            tmp_table_size = int(variables.get("tmp_table_size", 0))
            max_heap_table_size = int(variables.get("max_heap_table_size", 0))
            effective_tmp_size = min(tmp_table_size, max_heap_table_size)
            internal_tmp_mem_engine = variables.get(
                "internal_tmp_mem_storage_engine", "TempTable"
            )

            output["configuration"] = {
                "tmp_table_size": tmp_table_size,
                "tmp_table_size_mb": round(tmp_table_size / 1024 / 1024, 2),
                "max_heap_table_size": max_heap_table_size,
                "max_heap_table_size_mb": round(
                    max_heap_table_size / 1024 / 1024, 2
                ),
                "effective_limit_mb": round(effective_tmp_size / 1024 / 1024, 2),
                "internal_tmp_mem_storage_engine": internal_tmp_mem_engine,
                "tmpdir": variables.get("tmpdir", "unknown"),
            }

            # Top queries creating disk temp tables
            query = """
                SELECT
                    DIGEST_TEXT,
                    COUNT_STAR as exec_count,
                    SUM_CREATED_TMP_TABLES as tmp_tables,
                    SUM_CREATED_TMP_DISK_TABLES as disk_tmp_tables,
                    ROUND(SUM_CREATED_TMP_DISK_TABLES / NULLIF(SUM_CREATED_TMP_TABLES, 0) * 100, 2) as disk_pct,
                    ROUND(SUM_TIMER_WAIT / 1000000000000, 4) as total_time_sec,
                    SCHEMA_NAME
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SUM_CREATED_TMP_DISK_TABLES > 0
                ORDER BY SUM_CREATED_TMP_DISK_TABLES DESC
                LIMIT %s
            """

            try:
                results = await self.sql_driver.execute_query(query, [top_n])
                for row in results:
                    digest = row.get("DIGEST_TEXT") or ""
                    output["top_disk_temp_queries"].append({
                        "query_digest": digest[:300],
                        "exec_count": row["exec_count"],
                        "tmp_tables": row["tmp_tables"],
                        "disk_tmp_tables": row["disk_tmp_tables"],
                        "disk_pct": float(row["disk_pct"] or 0),
                        "total_time_sec": float(row["total_time_sec"] or 0),
                        "schema": row.get("SCHEMA_NAME", ""),
                    })
            except Exception:
                output["top_disk_temp_queries_note"] = (
                    "Could not query performance_schema for temp table details"
                )

            # Recommendations
            if disk_pct > 25:
                output["recommendations"].append(
                    f"Disk temp table ratio is high ({disk_pct:.1f}%). "
                    "This causes extra I/O. Consider increasing tmp_table_size/max_heap_table_size "
                    "or optimizing queries that use GROUP BY, ORDER BY, or DISTINCT on large result sets."
                )

            if tmp_table_size != max_heap_table_size:
                output["recommendations"].append(
                    f"tmp_table_size ({tmp_table_size // 1024 // 1024}MB) and "
                    f"max_heap_table_size ({max_heap_table_size // 1024 // 1024}MB) differ. "
                    "The effective limit is the smaller value. Set them to the same value."
                )

            if effective_tmp_size < 64 * 1024 * 1024:
                output["recommendations"].append(
                    f"Effective tmp table limit is only {effective_tmp_size // 1024 // 1024}MB. "
                    "Consider increasing to at least 64MB for production workloads."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class PerfSchemaConfigToolHandler(ToolHandler):
    """Tool handler for checking Performance Schema configuration."""

    name = "check_perf_schema_config"
    title = "Performance Schema Config"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Check Performance Schema configuration and enablement status.

Provides:
- Whether performance_schema is ON or OFF
- Enabled instruments (statement, wait, stage, memory, etc.)
- Enabled consumers (events, summaries, history)
- Memory usage by performance_schema
- Setup diagnostics for tuning tools that depend on it

Essential first step: many tuning tools require performance_schema to be properly
configured. This tool helps verify the prerequisites are met."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "verbose": {
                        "type": "boolean",
                        "description": "Show all instruments (not just summary counts)",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            verbose = arguments.get("verbose", False)

            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            output: dict[str, Any] = {
                "performance_schema_enabled": False,
                "instruments_summary": {},
                "consumers_summary": {},
                "memory_usage": {},
                "tool_readiness": {},
                "recommendations": []
            }

            ps_enabled = variables.get("performance_schema", "OFF").upper() == "ON"
            output["performance_schema_enabled"] = ps_enabled

            if not ps_enabled:
                output["recommendations"].append(
                    "performance_schema is OFF. Most tuning tools require it. "
                    "Enable with: SET GLOBAL performance_schema = ON (requires restart)."
                )
                output["tool_readiness"] = {
                    "slow_query_analysis": False,
                    "wait_event_analysis": False,
                    "statement_analysis": False,
                    "index_usage_stats": False,
                    "memory_analysis": False,
                }
                return self.format_json_result(output)

            # Instrument summary by category
            instrument_query = """
                SELECT
                    SUBSTRING_INDEX(NAME, '/', 2) as category,
                    COUNT(*) as total,
                    SUM(CASE WHEN ENABLED = 'YES' THEN 1 ELSE 0 END) as enabled,
                    SUM(CASE WHEN TIMED = 'YES' THEN 1 ELSE 0 END) as timed
                FROM performance_schema.setup_instruments
                GROUP BY SUBSTRING_INDEX(NAME, '/', 2)
                ORDER BY category
            """

            try:
                instruments = await self.sql_driver.execute_query(instrument_query)
                for row in instruments:
                    output["instruments_summary"][row["category"]] = {
                        "total": row["total"],
                        "enabled": row["enabled"],
                        "timed": row["timed"],
                        "enabled_pct": round(
                            row["enabled"] / row["total"] * 100, 1
                        ) if row["total"] else 0,
                    }
            except Exception:
                output["instruments_summary_note"] = "Could not query setup_instruments"

            # Consumer summary
            consumer_query = """
                SELECT NAME, ENABLED
                FROM performance_schema.setup_consumers
                ORDER BY NAME
            """

            try:
                consumers = await self.sql_driver.execute_query(consumer_query)
                for row in consumers:
                    output["consumers_summary"][row["NAME"]] = row["ENABLED"]
            except Exception:
                output["consumers_summary_note"] = "Could not query setup_consumers"

            # Memory usage
            ps_memory = int(
                status.get("Performance_schema_memory", 0)
            )
            output["memory_usage"] = {
                "total_bytes": ps_memory,
                "total_mb": round(ps_memory / 1024 / 1024, 2),
            }

            # Check readiness for specific tools
            consumers = output.get("consumers_summary", {})
            instruments = output.get("instruments_summary", {})

            stmt_enabled = instruments.get("statement/sql", {}).get("enabled", 0) > 0
            wait_enabled = instruments.get("wait/io", {}).get("enabled", 0) > 0
            stage_enabled = instruments.get("stage/sql", {}).get("enabled", 0) > 0
            memory_enabled = instruments.get("memory/sql", {}).get("enabled", 0) > 0

            output["tool_readiness"] = {
                "slow_query_analysis": stmt_enabled,
                "wait_event_analysis": wait_enabled,
                "statement_analysis": stmt_enabled,
                "stage_analysis": stage_enabled,
                "index_usage_stats": wait_enabled,
                "memory_analysis": memory_enabled,
                "events_statements_history": consumers.get(
                    "events_statements_history", "NO"
                ) == "YES",
                "events_waits_history": consumers.get(
                    "events_waits_history", "NO"
                ) == "YES",
            }

            # Recommendations
            if not stmt_enabled:
                output["recommendations"].append(
                    "Statement instruments are disabled. Enable with: "
                    "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' "
                    "WHERE NAME LIKE 'statement/%'"
                )

            if not wait_enabled:
                output["recommendations"].append(
                    "Wait instruments are disabled. Enable with: "
                    "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' "
                    "WHERE NAME LIKE 'wait/%'"
                )

            if consumers.get("events_statements_current", "NO") != "YES":
                output["recommendations"].append(
                    "events_statements_current consumer is disabled. Enable with: "
                    "UPDATE performance_schema.setup_consumers SET ENABLED='YES' "
                    "WHERE NAME = 'events_statements_current'"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class OptimizerConfigToolHandler(ToolHandler):
    """Tool handler for reviewing MySQL optimizer configuration."""

    name = "review_optimizer_config"
    title = "Optimizer Config Review"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Review MySQL query optimizer configuration and switches.

Provides:
- optimizer_switch flags and their status
- Cost model parameters (if MySQL 8.0+)
- Join/sort/scan strategy configuration
- Optimizer trace readiness
- Recommendations for optimizer tuning

Useful for:
- Understanding why the optimizer chose a specific plan
- Tuning optimizer behavior for specific workloads
- Enabling/disabling specific optimization strategies
- Diagnosing suboptimal query plans"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_cost_model": {
                        "type": "boolean",
                        "description": "Include cost model parameters (MySQL 8.0+)",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_cost = arguments.get("include_cost_model", True)

            variables = await self.sql_driver.get_server_variables()

            output: dict[str, Any] = {
                "optimizer_switches": {},
                "key_settings": {},
                "cost_model": {},
                "recommendations": []
            }

            # Parse optimizer_switch
            optimizer_switch = variables.get("optimizer_switch", "")
            if optimizer_switch:
                switches = {}
                for item in optimizer_switch.split(","):
                    parts = item.split("=")
                    if len(parts) == 2:
                        switches[parts[0].strip()] = parts[1].strip()
                output["optimizer_switches"] = switches

            # Key optimizer settings
            output["key_settings"] = {
                "optimizer_search_depth": variables.get(
                    "optimizer_search_depth", "unknown"
                ),
                "optimizer_prune_level": variables.get(
                    "optimizer_prune_level", "unknown"
                ),
                "optimizer_trace": variables.get("optimizer_trace", "unknown"),
                "optimizer_trace_max_mem_size": variables.get(
                    "optimizer_trace_max_mem_size", "unknown"
                ),
                "eq_range_index_dive_limit": variables.get(
                    "eq_range_index_dive_limit", "unknown"
                ),
                "range_optimizer_max_mem_size": variables.get(
                    "range_optimizer_max_mem_size", "unknown"
                ),
                "max_join_size": variables.get("max_join_size", "unknown"),
                "join_buffer_size": variables.get("join_buffer_size", "unknown"),
                "sort_buffer_size": variables.get("sort_buffer_size", "unknown"),
                "read_rnd_buffer_size": variables.get(
                    "read_rnd_buffer_size", "unknown"
                ),
            }

            # Cost model (MySQL 8.0+)
            if include_cost:
                try:
                    cost_query = """
                        SELECT cost_name, cost_value, default_value
                        FROM mysql.server_cost
                    """
                    cost_results = await self.sql_driver.execute_query(cost_query)
                    server_costs = {}
                    for row in cost_results:
                        server_costs[row["cost_name"]] = {
                            "value": row["cost_value"],
                            "default": row["default_value"],
                        }
                    output["cost_model"]["server_cost"] = server_costs

                    engine_cost_query = """
                        SELECT engine_name, cost_name, cost_value, default_value
                        FROM mysql.engine_cost
                    """
                    engine_results = await self.sql_driver.execute_query(
                        engine_cost_query
                    )
                    engine_costs = {}
                    for row in engine_results:
                        key = f"{row['engine_name']}.{row['cost_name']}"
                        engine_costs[key] = {
                            "value": row["cost_value"],
                            "default": row["default_value"],
                        }
                    output["cost_model"]["engine_cost"] = engine_costs
                except Exception:
                    output["cost_model"]["note"] = (
                        "Cost model tables not available (requires MySQL 8.0+)"
                    )

            # Recommendations based on switches
            switches = output.get("optimizer_switches", {})

            if switches.get("use_invisible_indexes") == "on":
                output["recommendations"].append(
                    "use_invisible_indexes is ON globally. This overrides invisible index hints "
                    "and should generally be OFF except for testing."
                )

            if switches.get("derived_merge") == "off":
                output["recommendations"].append(
                    "derived_merge is OFF. This may cause subqueries in FROM clause "
                    "to materialize unnecessarily. Consider enabling it."
                )

            if switches.get("index_merge") == "off":
                output["recommendations"].append(
                    "index_merge is OFF. The optimizer cannot merge multiple indexes "
                    "for a single table scan. Consider enabling for complex WHERE clauses."
                )

            if switches.get("mrr") == "off":
                output["recommendations"].append(
                    "Multi-Range Read (mrr) is OFF. Enabling it can improve range scan "
                    "performance on secondary indexes by reducing random disk access."
                )

            if switches.get("batched_key_access") == "off":
                output["recommendations"].append(
                    "Batched Key Access (BKA) is OFF. For join-heavy workloads, "
                    "enabling BKA with MRR can significantly improve join performance."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
