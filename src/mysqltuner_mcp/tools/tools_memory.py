"""
Memory calculation tool handlers for MySQL.

Includes tools for analyzing MySQL memory usage:
- Per-thread buffer calculations
- Server buffer calculations
- Maximum memory estimation
- Memory usage recommendations

Based on MySQLTuner's calculations() function for memory analysis.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class MemoryCalculationsToolHandler(ToolHandler):
    """Tool handler for MySQL memory calculations."""

    name = "calculate_memory_usage"
    title = "Memory Calculator"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Calculate MySQL memory usage and provide recommendations.

Analyzes:
- Per-thread memory buffers (read_buffer, sort_buffer, join_buffer, etc.)
- Global server buffers (key_buffer, innodb_buffer_pool, etc.)
- Maximum potential memory usage
- Current memory utilization

Based on MySQLTuner's memory calculation methodology.
Helps identify memory-related configuration issues."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "physical_memory_gb": {
                        "type": "number",
                        "description": "Physical memory in GB (for comparison). "
                                     "If not provided, uses system detection if available."
                    },
                    "detailed": {
                        "type": "boolean",
                        "description": "Include detailed breakdown",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            physical_memory_gb = arguments.get("physical_memory_gb")
            detailed = arguments.get("detailed", True)

            output = {
                "server_buffers": {},
                "per_thread_buffers": {},
                "memory_summary": {},
                "recommendations": [],
                "issues": []
            }

            # Get relevant variables
            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            # --- Server (Global) Buffers ---
            # These are allocated once for the entire server

            # Key buffer (MyISAM)
            key_buffer_size = int(variables.get("key_buffer_size", 0))

            # Query cache (deprecated in 8.0, removed in 8.0.3+)
            query_cache_size = int(variables.get("query_cache_size", 0))

            # InnoDB buffers
            innodb_buffer_pool_size = int(
                variables.get("innodb_buffer_pool_size", 0)
            )
            innodb_log_buffer_size = int(
                variables.get("innodb_log_buffer_size", 0)
            )
            innodb_additional_mem_pool_size = int(
                variables.get("innodb_additional_mem_pool_size", 0)
            )

            # Table definition cache
            table_definition_cache = int(
                variables.get("table_definition_cache", 0)
            )
            table_open_cache = int(variables.get("table_open_cache", 0))

            # Binary log cache
            binlog_cache_size = int(variables.get("binlog_cache_size", 0))

            output["server_buffers"] = {
                "key_buffer_size": self._format_bytes(key_buffer_size),
                "key_buffer_size_bytes": key_buffer_size,
                "query_cache_size": self._format_bytes(query_cache_size),
                "query_cache_size_bytes": query_cache_size,
                "innodb_buffer_pool_size": self._format_bytes(
                    innodb_buffer_pool_size
                ),
                "innodb_buffer_pool_size_bytes": innodb_buffer_pool_size,
                "innodb_log_buffer_size": self._format_bytes(innodb_log_buffer_size),
                "innodb_log_buffer_size_bytes": innodb_log_buffer_size,
                "innodb_additional_mem_pool_size": self._format_bytes(
                    innodb_additional_mem_pool_size
                ),
                "table_definition_cache": table_definition_cache,
                "table_open_cache": table_open_cache,
                "binlog_cache_size": self._format_bytes(binlog_cache_size),
                "binlog_cache_size_bytes": binlog_cache_size
            }

            # Total global buffers
            total_global_buffers = (
                key_buffer_size
                + query_cache_size
                + innodb_buffer_pool_size
                + innodb_log_buffer_size
                + innodb_additional_mem_pool_size
            )
            output["server_buffers"]["total"] = self._format_bytes(total_global_buffers)
            output["server_buffers"]["total_bytes"] = total_global_buffers

            # --- Per-Thread Buffers ---
            # These are allocated for each connection

            read_buffer_size = int(variables.get("read_buffer_size", 0))
            read_rnd_buffer_size = int(variables.get("read_rnd_buffer_size", 0))
            sort_buffer_size = int(variables.get("sort_buffer_size", 0))
            join_buffer_size = int(variables.get("join_buffer_size", 0))
            thread_stack = int(variables.get("thread_stack", 0))
            binlog_stmt_cache_size = int(
                variables.get("binlog_stmt_cache_size", 0)
            )
            net_buffer_length = int(variables.get("net_buffer_length", 0))
            tmp_table_size = int(variables.get("tmp_table_size", 0))
            max_heap_table_size = int(variables.get("max_heap_table_size", 0))

            # Per-thread temp table is limited by smaller of tmp_table_size and
            # max_heap_table_size
            effective_tmp_table_size = min(tmp_table_size, max_heap_table_size)

            output["per_thread_buffers"] = {
                "read_buffer_size": self._format_bytes(read_buffer_size),
                "read_buffer_size_bytes": read_buffer_size,
                "read_rnd_buffer_size": self._format_bytes(read_rnd_buffer_size),
                "read_rnd_buffer_size_bytes": read_rnd_buffer_size,
                "sort_buffer_size": self._format_bytes(sort_buffer_size),
                "sort_buffer_size_bytes": sort_buffer_size,
                "join_buffer_size": self._format_bytes(join_buffer_size),
                "join_buffer_size_bytes": join_buffer_size,
                "thread_stack": self._format_bytes(thread_stack),
                "thread_stack_bytes": thread_stack,
                "binlog_stmt_cache_size": self._format_bytes(binlog_stmt_cache_size),
                "binlog_stmt_cache_size_bytes": binlog_stmt_cache_size,
                "net_buffer_length": self._format_bytes(net_buffer_length),
                "net_buffer_length_bytes": net_buffer_length,
                "tmp_table_size": self._format_bytes(tmp_table_size),
                "tmp_table_size_bytes": tmp_table_size,
                "max_heap_table_size": self._format_bytes(max_heap_table_size),
                "max_heap_table_size_bytes": max_heap_table_size,
                "effective_tmp_table_size": self._format_bytes(
                    effective_tmp_table_size
                )
            }

            # Total per-thread buffer size (worst case per connection)
            per_thread_total = (
                read_buffer_size
                + read_rnd_buffer_size
                + sort_buffer_size
                + join_buffer_size
                + thread_stack
                + binlog_stmt_cache_size
                + net_buffer_length
            )
            output["per_thread_buffers"]["total_per_thread"] = self._format_bytes(
                per_thread_total
            )
            output["per_thread_buffers"]["total_per_thread_bytes"] = per_thread_total

            # --- Connection limits ---
            max_connections = int(variables.get("max_connections", 151))
            current_connections = int(status.get("Threads_connected", 0))
            max_used_connections = int(status.get("Max_used_connections", 0))

            output["memory_summary"]["max_connections"] = max_connections
            output["memory_summary"]["current_connections"] = current_connections
            output["memory_summary"]["max_used_connections"] = max_used_connections

            # --- Memory calculations ---

            # Maximum possible memory (worst case: all connections using max buffers)
            max_total_memory = (
                total_global_buffers + (per_thread_total * max_connections)
            )

            # Peak memory (based on historical max connections)
            peak_memory = (
                total_global_buffers + (per_thread_total * max_used_connections)
            )

            # Current memory estimate
            current_memory = (
                total_global_buffers + (per_thread_total * current_connections)
            )

            output["memory_summary"]["max_total_memory"] = self._format_bytes(
                max_total_memory
            )
            output["memory_summary"]["max_total_memory_bytes"] = max_total_memory
            output["memory_summary"]["max_total_memory_gb"] = round(
                max_total_memory / 1024 / 1024 / 1024, 2
            )

            output["memory_summary"]["peak_memory"] = self._format_bytes(peak_memory)
            output["memory_summary"]["peak_memory_bytes"] = peak_memory
            output["memory_summary"]["peak_memory_gb"] = round(
                peak_memory / 1024 / 1024 / 1024, 2
            )

            output["memory_summary"]["current_memory"] = self._format_bytes(
                current_memory
            )
            output["memory_summary"]["current_memory_bytes"] = current_memory
            output["memory_summary"]["current_memory_gb"] = round(
                current_memory / 1024 / 1024 / 1024, 2
            )

            output["memory_summary"]["global_buffers"] = self._format_bytes(
                total_global_buffers
            )
            output["memory_summary"]["global_buffers_bytes"] = total_global_buffers
            output["memory_summary"]["global_buffers_gb"] = round(
                total_global_buffers / 1024 / 1024 / 1024, 2
            )

            # Compare to physical memory if provided
            if physical_memory_gb:
                physical_memory_bytes = physical_memory_gb * 1024 * 1024 * 1024
                output["memory_summary"]["physical_memory_gb"] = physical_memory_gb

                max_memory_pct = (max_total_memory / physical_memory_bytes) * 100
                current_memory_pct = (current_memory / physical_memory_bytes) * 100

                output["memory_summary"]["max_memory_pct_of_physical"] = round(
                    max_memory_pct, 2
                )
                output["memory_summary"]["current_memory_pct_of_physical"] = round(
                    current_memory_pct, 2
                )

                # Recommendations based on physical memory
                if max_memory_pct > 100:
                    output["issues"].append(
                        f"Max possible MySQL memory ({self._format_bytes(max_total_memory)}) "
                        f"exceeds physical memory ({physical_memory_gb}GB)"
                    )
                    output["recommendations"].append(
                        "Reduce max_connections or per-thread buffer sizes to "
                        "prevent potential OOM"
                    )
                elif max_memory_pct > 85:
                    output["issues"].append(
                        f"Max MySQL memory is {max_memory_pct:.1f}% of physical memory"
                    )
                    output["recommendations"].append(
                        "Consider reducing max_connections to leave headroom "
                        "for OS and other processes"
                    )

            if detailed:
                await self._add_detailed_analysis(variables, status, output)

            # General recommendations
            self._generate_recommendations(variables, status, output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable string."""
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val / 1024:.2f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val / 1024 / 1024:.2f} MB"
        else:
            return f"{bytes_val / 1024 / 1024 / 1024:.2f} GB"

    async def _add_detailed_analysis(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Add detailed memory analysis."""

        # Key buffer usage (MyISAM)
        key_buffer_size = int(variables.get("key_buffer_size", 0))
        key_blocks_used = int(status.get("Key_blocks_used", 0))
        key_blocks_unused = int(status.get("Key_blocks_unused", 0))
        key_block_size = int(variables.get("key_cache_block_size", 1024))

        if key_buffer_size > 0:
            key_cache_used = key_blocks_used * key_block_size
            key_usage_pct = (key_cache_used / key_buffer_size) * 100

            output["detailed"] = output.get("detailed", {})
            output["detailed"]["key_buffer"] = {
                "size": self._format_bytes(key_buffer_size),
                "used": self._format_bytes(key_cache_used),
                "usage_pct": round(key_usage_pct, 2),
                "blocks_used": key_blocks_used,
                "blocks_unused": key_blocks_unused
            }

            if key_usage_pct < 10 and key_buffer_size > 64 * 1024 * 1024:
                output["recommendations"].append(
                    f"Key buffer utilization is only {key_usage_pct:.1f}%. "
                    "Consider reducing key_buffer_size if not using MyISAM."
                )

        # Query cache usage (if enabled)
        query_cache_size = int(variables.get("query_cache_size", 0))
        if query_cache_size > 0:
            qc_free_memory = int(status.get("Qcache_free_memory", 0))
            qc_hits = int(status.get("Qcache_hits", 0))
            qc_inserts = int(status.get("Qcache_inserts", 1))

            output["detailed"] = output.get("detailed", {})
            output["detailed"]["query_cache"] = {
                "size": self._format_bytes(query_cache_size),
                "free_memory": self._format_bytes(qc_free_memory),
                "usage_pct": round(
                    (query_cache_size - qc_free_memory) / query_cache_size * 100, 2
                ),
                "hit_ratio": round(qc_hits / max(qc_hits + qc_inserts, 1) * 100, 2)
            }

        # InnoDB buffer pool details
        innodb_bp_size = int(variables.get("innodb_buffer_pool_size", 0))
        if innodb_bp_size > 0:
            bp_pages_total = int(status.get("Innodb_buffer_pool_pages_total", 1))
            bp_pages_free = int(status.get("Innodb_buffer_pool_pages_free", 0))
            bp_pages_data = int(status.get("Innodb_buffer_pool_pages_data", 0))
            bp_pages_dirty = int(status.get("Innodb_buffer_pool_pages_dirty", 0))

            read_requests = int(status.get("Innodb_buffer_pool_read_requests", 0))
            reads = int(status.get("Innodb_buffer_pool_reads", 0))
            hit_ratio = (
                (read_requests - reads) / max(read_requests, 1) * 100
            )

            output["detailed"] = output.get("detailed", {})
            output["detailed"]["innodb_buffer_pool"] = {
                "size": self._format_bytes(innodb_bp_size),
                "pages_total": bp_pages_total,
                "pages_free": bp_pages_free,
                "pages_data": bp_pages_data,
                "pages_dirty": bp_pages_dirty,
                "usage_pct": round(
                    (bp_pages_total - bp_pages_free) / bp_pages_total * 100, 2
                ),
                "hit_ratio_pct": round(hit_ratio, 4)
            }

    def _generate_recommendations(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Generate memory-related recommendations."""

        # Check sort buffer
        sort_buffer = int(variables.get("sort_buffer_size", 0))
        if sort_buffer > 4 * 1024 * 1024:  # > 4MB
            output["recommendations"].append(
                f"sort_buffer_size ({self._format_bytes(sort_buffer)}) is large. "
                "This is allocated per-sort, consider reducing if memory is tight."
            )

        # Check join buffer
        join_buffer = int(variables.get("join_buffer_size", 0))
        if join_buffer > 4 * 1024 * 1024:  # > 4MB
            output["recommendations"].append(
                f"join_buffer_size ({self._format_bytes(join_buffer)}) is large. "
                "Multiple can be allocated per query for nested joins."
            )

        # Check tmp_table_size vs max_heap_table_size mismatch
        tmp_table = int(variables.get("tmp_table_size", 0))
        max_heap = int(variables.get("max_heap_table_size", 0))
        if tmp_table != max_heap:
            output["recommendations"].append(
                f"tmp_table_size ({self._format_bytes(tmp_table)}) and "
                f"max_heap_table_size ({self._format_bytes(max_heap)}) differ. "
                "The smaller value is used. Consider aligning them."
            )

        # Check for unused query cache in MySQL 5.7
        qc_type = variables.get("query_cache_type", "OFF")
        qc_size = int(variables.get("query_cache_size", 0))
        if qc_type == "OFF" and qc_size > 0:
            output["recommendations"].append(
                "query_cache_type is OFF but query_cache_size is allocated. "
                "Set query_cache_size=0 to free memory."
            )

        # Check connection-related memory
        max_conn = int(variables.get("max_connections", 151))
        if max_conn > 500:
            output["recommendations"].append(
                f"max_connections is high ({max_conn}). "
                "Consider using connection pooling to reduce memory overhead."
            )

        # Thread cache
        thread_cache_size = int(variables.get("thread_cache_size", 0))
        threads_created = int(status.get("Threads_created", 0))
        connections = int(status.get("Connections", 1))

        if connections > 0:
            thread_cache_miss_pct = (threads_created / connections) * 100
            if thread_cache_miss_pct > 10 and thread_cache_size < 64:
                output["recommendations"].append(
                    f"Thread cache miss rate is {thread_cache_miss_pct:.1f}%. "
                    "Consider increasing thread_cache_size."
                )


class MemoryByHostToolHandler(ToolHandler):
    """Tool handler for memory usage by host/user."""

    name = "get_memory_by_host"
    title = "Memory by Host"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get memory usage breakdown by host or user.

Uses sys schema or performance_schema to show:
- Memory allocated per host
- Memory allocated per user
- Memory by event/operation type

Requires performance_schema memory instrumentation enabled."""

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
                        "description": "Group memory by",
                        "enum": ["host", "user", "event_name"],
                        "default": "host"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results to return",
                        "default": 25
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            group_by = arguments.get("group_by", "host")
            limit = arguments.get("limit", 25)

            output = {
                "memory_usage": [],
                "total_allocated": {},
                "recommendations": []
            }

            # Check if memory instrumentation is available
            try:
                if group_by == "host":
                    query = f"""
                        SELECT
                            host,
                            current_count_used,
                            current_allocated as current_bytes,
                            current_avg_alloc,
                            current_max_alloc,
                            total_allocated
                        FROM sys.memory_by_host_by_current_bytes
                        WHERE host IS NOT NULL
                        ORDER BY current_allocated DESC
                        LIMIT {limit}
                    """
                elif group_by == "user":
                    query = f"""
                        SELECT
                            user,
                            current_count_used,
                            current_allocated as current_bytes,
                            current_avg_alloc,
                            current_max_alloc,
                            total_allocated
                        FROM sys.memory_by_user_by_current_bytes
                        WHERE user IS NOT NULL
                        ORDER BY current_allocated DESC
                        LIMIT {limit}
                    """
                else:  # event_name
                    query = f"""
                        SELECT
                            event_name,
                            current_count,
                            current_alloc as current_bytes,
                            current_avg_alloc,
                            high_count,
                            high_alloc
                        FROM sys.memory_global_by_current_bytes
                        ORDER BY current_alloc DESC
                        LIMIT {limit}
                    """

                results = await self.sql_driver.execute_query(query)

                for row in results:
                    entry = {}
                    if group_by == "host":
                        entry["host"] = row.get("host")
                    elif group_by == "user":
                        entry["user"] = row.get("user")
                    else:
                        entry["event_name"] = row.get("event_name")

                    entry["current_bytes"] = row.get("current_bytes")
                    entry["current_alloc_readable"] = str(
                        row.get("current_alloc") or row.get("current_allocated")
                    )
                    entry["current_count"] = (
                        row.get("current_count_used") or row.get("current_count")
                    )

                    output["memory_usage"].append(entry)

                # Get total
                total_query = """
                    SELECT SUM(current_alloc) as total
                    FROM sys.memory_global_by_current_bytes
                """
                total_result = await self.sql_driver.execute_scalar(total_query)
                output["total_allocated"]["bytes"] = total_result or 0
                output["total_allocated"]["readable"] = self._format_bytes(
                    total_result or 0
                )

            except Exception as e:
                # Memory instrumentation may not be enabled
                output["error"] = str(e)
                output["recommendations"].append(
                    "Enable memory instrumentation in performance_schema: "
                    "UPDATE performance_schema.setup_instruments "
                    "SET ENABLED='YES' WHERE NAME LIKE 'memory/%'"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable string."""
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val / 1024:.2f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val / 1024 / 1024:.2f} MB"
        else:
            return f"{bytes_val / 1024 / 1024 / 1024:.2f} GB"


class TableMemoryUsageToolHandler(ToolHandler):
    """Tool handler for table memory and cache analysis."""

    name = "get_table_memory_usage"
    title = "Table Memory Usage"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze memory usage for user tables and caches.

Shows:
- Table cache usage and hit rates
- Table definition cache efficiency
- Open tables vs table_open_cache
- InnoDB buffer pool by table

Note: Buffer pool breakdown by table only shows user/custom tables
and excludes MySQL system tables (mysql, information_schema, performance_schema, sys).

Helps optimize table caching parameters."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_buffer_pool": {
                        "type": "boolean",
                        "description": "Include InnoDB buffer pool by table",
                        "default": True
                    },
                    "top_n_tables": {
                        "type": "integer",
                        "description": "Number of top tables to show",
                        "default": 20
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_bp = arguments.get("include_buffer_pool", True)
            top_n = arguments.get("top_n_tables", 20)

            output = {
                "table_cache": {},
                "definition_cache": {},
                "buffer_pool_by_table": [],
                "recommendations": []
            }

            # Get relevant variables and status
            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            # Table cache analysis
            table_open_cache = int(variables.get("table_open_cache", 0))
            open_tables = int(status.get("Open_tables", 0))
            opened_tables = int(status.get("Opened_tables", 0))
            table_open_cache_instances = int(
                variables.get("table_open_cache_instances", 1)
            )

            output["table_cache"] = {
                "table_open_cache": table_open_cache,
                "table_open_cache_instances": table_open_cache_instances,
                "open_tables": open_tables,
                "opened_tables": opened_tables,
                "cache_usage_pct": round(
                    open_tables / max(table_open_cache, 1) * 100, 2
                ),
                "cache_misses": opened_tables - open_tables
            }

            # Calculate table cache hit ratio
            if opened_tables > 0:
                # Low ratio means cache misses
                miss_ratio = (opened_tables - open_tables) / opened_tables * 100
                if miss_ratio > 10:
                    output["recommendations"].append(
                        f"Table cache miss ratio is {miss_ratio:.1f}%. "
                        "Consider increasing table_open_cache."
                    )

            # Table definition cache
            table_definition_cache = int(
                variables.get("table_definition_cache", 0)
            )
            open_table_definitions = int(
                status.get("Open_table_definitions", 0)
            )
            opened_table_definitions = int(
                status.get("Opened_table_definitions", 0)
            )

            output["definition_cache"] = {
                "table_definition_cache": table_definition_cache,
                "open_definitions": open_table_definitions,
                "opened_definitions": opened_table_definitions,
                "cache_usage_pct": round(
                    open_table_definitions / max(table_definition_cache, 1) * 100, 2
                )
            }

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # InnoDB buffer pool by table
            if include_bp:
                try:
                    bp_query = f"""
                        SELECT
                            object_schema,
                            object_name,
                            allocated,
                            data,
                            pages,
                            pages_hashed,
                            pages_old,
                            rows_cached
                        FROM sys.innodb_buffer_stats_by_table
                        WHERE object_schema NOT IN {system_schemas}
                        ORDER BY allocated DESC
                        LIMIT {top_n}
                    """
                    bp_results = await self.sql_driver.execute_query(bp_query)

                    for row in bp_results:
                        output["buffer_pool_by_table"].append({
                            "schema": row.get("object_schema"),
                            "table": row.get("object_name"),
                            "allocated": str(row.get("allocated")),
                            "data": str(row.get("data")),
                            "pages": row.get("pages"),
                            "rows_cached": row.get("rows_cached")
                        })
                except Exception:
                    # sys schema may not be available
                    output["buffer_pool_by_table"] = []
                    output["recommendations"].append(
                        "Install sys schema for detailed buffer pool breakdown"
                    )

            # Additional recommendations
            if open_tables >= table_open_cache * 0.9:
                output["recommendations"].append(
                    f"Table cache is {output['table_cache']['cache_usage_pct']:.1f}% full. "
                    "Consider increasing table_open_cache."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
