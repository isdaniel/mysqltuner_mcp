"""
Schema profiling and binary log analysis tool handlers for MySQL.

Includes tools for:
- Schema size and growth profiling
- Binary log throughput and configuration
- Global status snapshot comparison
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class SchemaProfilingToolHandler(ToolHandler):
    """Tool handler for database/schema size profiling."""

    name = "profile_schema_sizes"
    title = "Schema Size Profiler"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Profile MySQL schema and table sizes for capacity planning.

Provides:
- Database sizes ranked by total size
- Largest tables across all databases
- Data vs index size ratios
- Row counts and average row length
- Per-schema breakdown

Useful for:
- Capacity planning and storage forecasting
- Identifying unexpectedly large tables
- Finding tables with poor data/index ratios
- Database housekeeping and archival planning"""

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
                        "description": "Filter to a specific schema (empty for all)",
                        "default": ""
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top tables to return",
                        "default": 20
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_filter = arguments.get("schema", "")
            top_n = arguments.get("top_n", 20)

            output: dict[str, Any] = {
                "database_sizes": [],
                "largest_tables": [],
                "summary": {},
                "recommendations": []
            }

            system_schemas = (
                "'mysql','information_schema','performance_schema','sys'"
            )

            # Database-level sizes
            db_query = f"""
                SELECT
                    TABLE_SCHEMA as db_name,
                    COUNT(*) as table_count,
                    COALESCE(SUM(DATA_LENGTH), 0) as data_size,
                    COALESCE(SUM(INDEX_LENGTH), 0) as index_size,
                    COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) as total_size,
                    COALESCE(SUM(DATA_FREE), 0) as free_space,
                    COALESCE(SUM(TABLE_ROWS), 0) as total_rows
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA NOT IN ({system_schemas})
            """

            params: list[Any] = []
            if schema_filter:
                db_query += " AND TABLE_SCHEMA = %s"
                params.append(schema_filter)

            db_query += " GROUP BY TABLE_SCHEMA ORDER BY total_size DESC"

            db_results = await self.sql_driver.execute_query(
                db_query, params if params else None
            )

            total_data = 0
            total_index = 0
            total_free = 0

            for row in db_results:
                data = int(row["data_size"] or 0)
                index = int(row["index_size"] or 0)
                total = int(row["total_size"] or 0)
                free = int(row["free_space"] or 0)
                total_data += data
                total_index += index
                total_free += free

                output["database_sizes"].append({
                    "database": row["db_name"],
                    "table_count": row["table_count"],
                    "data_size_mb": round(data / 1024 / 1024, 2),
                    "index_size_mb": round(index / 1024 / 1024, 2),
                    "total_size_mb": round(total / 1024 / 1024, 2),
                    "free_space_mb": round(free / 1024 / 1024, 2),
                    "total_rows": row["total_rows"],
                    "index_to_data_ratio": round(
                        index / data, 3
                    ) if data else 0,
                })

            # Top N largest tables
            table_query = f"""
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    ENGINE,
                    TABLE_ROWS,
                    AVG_ROW_LENGTH,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    DATA_LENGTH + INDEX_LENGTH as total_size,
                    DATA_FREE
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA NOT IN ({system_schemas})
                  AND TABLE_TYPE = 'BASE TABLE'
            """

            params2: list[Any] = []
            if schema_filter:
                table_query += " AND TABLE_SCHEMA = %s"
                params2.append(schema_filter)

            table_query += " ORDER BY total_size DESC LIMIT %s"
            params2.append(top_n)

            table_results = await self.sql_driver.execute_query(
                table_query, params2
            )

            for row in table_results:
                data = int(row["DATA_LENGTH"] or 0)
                index = int(row["INDEX_LENGTH"] or 0)
                output["largest_tables"].append({
                    "schema": row["TABLE_SCHEMA"],
                    "table": row["TABLE_NAME"],
                    "engine": row["ENGINE"],
                    "rows": row["TABLE_ROWS"],
                    "avg_row_length": row["AVG_ROW_LENGTH"],
                    "data_size_mb": round(data / 1024 / 1024, 2),
                    "index_size_mb": round(index / 1024 / 1024, 2),
                    "total_size_mb": round(
                        (data + index) / 1024 / 1024, 2
                    ),
                    "free_space_mb": round(
                        int(row["DATA_FREE"] or 0) / 1024 / 1024, 2
                    ),
                })

            # Summary
            output["summary"] = {
                "total_databases": len(db_results),
                "total_data_size_mb": round(total_data / 1024 / 1024, 2),
                "total_index_size_mb": round(total_index / 1024 / 1024, 2),
                "total_size_mb": round(
                    (total_data + total_index) / 1024 / 1024, 2
                ),
                "total_free_space_mb": round(total_free / 1024 / 1024, 2),
                "overall_index_to_data_ratio": round(
                    total_index / total_data, 3
                ) if total_data else 0,
            }

            # Recommendations
            if total_free > (total_data + total_index) * 0.2:
                output["recommendations"].append(
                    f"Free space ({total_free // 1024 // 1024}MB) is more than 20% of total size. "
                    "Consider running OPTIMIZE TABLE on fragmented tables."
                )

            for t in output["largest_tables"][:5]:
                if t["free_space_mb"] > t["total_size_mb"] * 0.3 and t["total_size_mb"] > 100:
                    output["recommendations"].append(
                        f"Table {t['schema']}.{t['table']} has significant free space "
                        f"({t['free_space_mb']}MB / {t['total_size_mb']}MB). "
                        "Consider OPTIMIZE TABLE to reclaim space."
                    )

            if total_index > total_data * 1.5 and total_data > 0:
                output["recommendations"].append(
                    "Total index size exceeds 1.5x data size. "
                    "Review indexes for redundancy - use find_unused_indexes tool."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class BinlogAnalysisToolHandler(ToolHandler):
    """Tool handler for binary log analysis."""

    name = "analyze_binlog"
    title = "Binary Log Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze MySQL binary log configuration and throughput.

Provides:
- Binary log enablement and format
- Current binlog file sizes
- Binlog throughput rate (bytes/sec)
- Disk space used by binlogs
- Expire/purge configuration
- GTID mode status

Useful for:
- Monitoring replication readiness
- Estimating binlog disk usage
- Tuning binlog retention
- Point-in-time recovery planning"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            variables = await self.sql_driver.get_server_variables()
            status = await self.sql_driver.get_server_status()

            output: dict[str, Any] = {
                "binlog_enabled": False,
                "configuration": {},
                "binlog_files": [],
                "throughput": {},
                "recommendations": []
            }

            log_bin = variables.get("log_bin", "OFF")
            binlog_enabled = log_bin.upper() not in ("OFF", "0")
            output["binlog_enabled"] = binlog_enabled

            output["configuration"] = {
                "log_bin": log_bin,
                "binlog_format": variables.get("binlog_format", "unknown"),
                "binlog_row_image": variables.get("binlog_row_image", "unknown"),
                "sync_binlog": variables.get("sync_binlog", "unknown"),
                "binlog_cache_size": variables.get("binlog_cache_size", "unknown"),
                "max_binlog_size": variables.get("max_binlog_size", "unknown"),
                "binlog_expire_logs_seconds": variables.get(
                    "binlog_expire_logs_seconds",
                    variables.get("expire_logs_days", "unknown")
                ),
                "gtid_mode": variables.get("gtid_mode", "OFF"),
                "enforce_gtid_consistency": variables.get(
                    "enforce_gtid_consistency", "OFF"
                ),
            }

            if not binlog_enabled:
                output["recommendations"].append(
                    "Binary logging is disabled. Enable it for replication "
                    "and point-in-time recovery (PITR)."
                )
                return self.format_json_result(output)

            # Get binary log files
            try:
                # Try MySQL 8.4+ / MariaDB syntax first
                try:
                    binlog_results = await self.sql_driver.execute_query(
                        "SHOW BINARY LOGS"
                    )
                except Exception:
                    binlog_results = []

                total_size = 0
                for row in binlog_results:
                    # Column names vary between MySQL versions
                    file_name = row.get("Log_name", row.get("log_name", ""))
                    file_size = int(
                        row.get("File_size", row.get("file_size", 0))
                    )
                    total_size += file_size
                    output["binlog_files"].append({
                        "file": file_name,
                        "size_mb": round(file_size / 1024 / 1024, 2),
                    })

                output["throughput"] = {
                    "total_binlog_files": len(binlog_results),
                    "total_binlog_size_mb": round(total_size / 1024 / 1024, 2),
                }
            except Exception:
                output["binlog_files_note"] = (
                    "Could not retrieve binary log file list"
                )

            # Binlog cache stats
            binlog_cache_use = int(status.get("Binlog_cache_use", 0))
            binlog_cache_disk = int(status.get("Binlog_cache_disk_use", 0))
            cache_disk_pct = (
                round(binlog_cache_disk / binlog_cache_use * 100, 2)
                if binlog_cache_use else 0
            )

            output["throughput"]["binlog_cache_use"] = binlog_cache_use
            output["throughput"]["binlog_cache_disk_use"] = binlog_cache_disk
            output["throughput"]["cache_disk_pct"] = cache_disk_pct

            # Throughput based on status
            bytes_sent = int(status.get("Binlog_bytes_written", 0))
            uptime = int(status.get("Uptime", 1))
            if bytes_sent > 0:
                output["throughput"]["bytes_written_total"] = bytes_sent
                output["throughput"]["bytes_per_sec"] = round(
                    bytes_sent / uptime, 2
                )
                output["throughput"]["mb_per_hour"] = round(
                    bytes_sent / uptime * 3600 / 1024 / 1024, 2
                )

            # Recommendations
            sync_binlog = int(variables.get("sync_binlog", 1))
            if sync_binlog == 0:
                output["recommendations"].append(
                    "sync_binlog=0 risks losing committed transactions on crash. "
                    "Set sync_binlog=1 for full durability."
                )

            binlog_format = variables.get("binlog_format", "").upper()
            if binlog_format == "STATEMENT":
                output["recommendations"].append(
                    "binlog_format=STATEMENT is not safe for all operations. "
                    "Consider ROW format for deterministic replication."
                )

            if cache_disk_pct > 10:
                output["recommendations"].append(
                    f"Binlog cache disk usage is {cache_disk_pct:.1f}%. "
                    "Consider increasing binlog_cache_size to reduce disk writes."
                )

            expire_seconds = int(variables.get(
                "binlog_expire_logs_seconds",
                int(variables.get("expire_logs_days", 0)) * 86400
            ) or 0)
            if expire_seconds == 0:
                output["recommendations"].append(
                    "Binlog expiration is not set. Old binlogs will accumulate. "
                    "Set binlog_expire_logs_seconds (e.g., 604800 for 7 days)."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class GlobalStatusSnapshotToolHandler(ToolHandler):
    """Tool handler for capturing global status snapshots."""

    name = "get_global_status_snapshot"
    title = "Global Status Snapshot"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Capture a snapshot of key MySQL global status counters for performance analysis.

Provides a curated set of the most performance-relevant SHOW GLOBAL STATUS counters:
- Throughput: Questions, Com_select, Com_insert, Com_update, Com_delete per second
- InnoDB: Buffer pool reads, row operations, log writes
- Connections: Created, aborted, running threads
- Query quality: Slow queries, full joins, select scans, sort passes
- Handler stats: Read key, read next, read rnd, write operations

This snapshot is designed to be compared over time or used to assess overall server load.
Call it twice with a delay to compute delta rates."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Category of status counters",
                        "enum": [
                            "all", "throughput", "innodb",
                            "connections", "query_quality", "handlers"
                        ],
                        "default": "all"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            category = arguments.get("category", "all")

            status = await self.sql_driver.get_server_status()
            uptime = int(status.get("Uptime", 1))

            output: dict[str, Any] = {
                "uptime_seconds": uptime,
                "uptime_days": round(uptime / 86400, 2),
                "counters": {},
                "rates_per_second": {},
            }

            # Define counter categories
            throughput_keys = [
                "Questions", "Queries", "Com_select", "Com_insert",
                "Com_update", "Com_delete", "Com_replace",
                "Com_commit", "Com_rollback",
                "Bytes_received", "Bytes_sent",
            ]

            innodb_keys = [
                "Innodb_buffer_pool_reads", "Innodb_buffer_pool_read_requests",
                "Innodb_buffer_pool_write_requests",
                "Innodb_rows_read", "Innodb_rows_inserted",
                "Innodb_rows_updated", "Innodb_rows_deleted",
                "Innodb_data_reads", "Innodb_data_writes",
                "Innodb_os_log_written", "Innodb_log_writes",
                "Innodb_buffer_pool_pages_total",
                "Innodb_buffer_pool_pages_free",
                "Innodb_buffer_pool_pages_dirty",
            ]

            connection_keys = [
                "Connections", "Threads_connected", "Threads_running",
                "Threads_created", "Threads_cached",
                "Aborted_clients", "Aborted_connects",
                "Max_used_connections",
            ]

            query_quality_keys = [
                "Slow_queries", "Select_full_join", "Select_full_range_join",
                "Select_range", "Select_range_check", "Select_scan",
                "Sort_merge_passes", "Sort_range", "Sort_rows", "Sort_scan",
                "Created_tmp_tables", "Created_tmp_disk_tables",
                "Created_tmp_files",
            ]

            handler_keys = [
                "Handler_read_first", "Handler_read_key",
                "Handler_read_last", "Handler_read_next",
                "Handler_read_prev", "Handler_read_rnd",
                "Handler_read_rnd_next", "Handler_write",
                "Handler_update", "Handler_delete",
            ]

            selected_keys: list[str] = []
            if category in ("all", "throughput"):
                selected_keys.extend(throughput_keys)
            if category in ("all", "innodb"):
                selected_keys.extend(innodb_keys)
            if category in ("all", "connections"):
                selected_keys.extend(connection_keys)
            if category in ("all", "query_quality"):
                selected_keys.extend(query_quality_keys)
            if category in ("all", "handlers"):
                selected_keys.extend(handler_keys)

            for key in selected_keys:
                val = status.get(key)
                if val is not None:
                    int_val = int(val)
                    output["counters"][key] = int_val

                    # Compute per-second rate for cumulative counters
                    # (skip gauges like Threads_connected)
                    gauge_keys = {
                        "Threads_connected", "Threads_running",
                        "Threads_cached", "Max_used_connections",
                        "Innodb_buffer_pool_pages_total",
                        "Innodb_buffer_pool_pages_free",
                        "Innodb_buffer_pool_pages_dirty",
                    }
                    if key not in gauge_keys and uptime > 0:
                        output["rates_per_second"][key] = round(
                            int_val / uptime, 4
                        )

            # Add computed metrics
            questions = int(status.get("Questions", 0))
            if questions > 0:
                slow_q = int(status.get("Slow_queries", 0))
                output["computed"] = {
                    "qps": round(questions / uptime, 2) if uptime else 0,
                    "slow_query_pct": round(
                        slow_q / questions * 100, 4
                    ),
                }

                selects = int(status.get("Com_select", 0))
                inserts = int(status.get("Com_insert", 0))
                updates = int(status.get("Com_update", 0))
                deletes = int(status.get("Com_delete", 0))
                total_dml = selects + inserts + updates + deletes

                if total_dml > 0:
                    output["computed"]["read_pct"] = round(
                        selects / total_dml * 100, 2
                    )
                    output["computed"]["write_pct"] = round(
                        (inserts + updates + deletes) / total_dml * 100, 2
                    )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
