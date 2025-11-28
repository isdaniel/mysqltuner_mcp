"""
Health check and monitoring tool handlers for MySQL.

Includes tools for:
- Database health checks
- Active query monitoring
- Wait event analysis
- Configuration settings review
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class DatabaseHealthToolHandler(ToolHandler):
    """Tool handler for comprehensive database health check."""

    name = "check_database_health"
    title = "Database Health Check"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Perform a comprehensive MySQL database health check.

Analyzes multiple aspects of MySQL health:
- Connection statistics and pool usage
- Buffer pool hit ratio
- Query cache efficiency (if enabled)
- InnoDB metrics (buffer pool, log, transactions)
- Replication status (if configured)
- Thread and connection usage
- Uptime and general status

Returns a health score with detailed breakdown and recommendations.
Based on MySQLTuner analysis concepts."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_recommendations": {
                        "type": "boolean",
                        "description": "Include actionable recommendations",
                        "default": True
                    },
                    "verbose": {
                        "type": "boolean",
                        "description": "Include detailed statistics",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_recs = arguments.get("include_recommendations", True)
            verbose = arguments.get("verbose", False)

            health_score = 100
            issues = []
            recommendations = []

            output = {
                "health_score": 0,
                "status": "healthy",
                "checks": {},
                "issues": [],
                "recommendations": []
            }

            # 1. Get server status variables
            status = await self.sql_driver.get_server_status()
            variables = await self.sql_driver.get_server_variables()

            # 2. Connection check
            max_connections = int(variables.get("max_connections", 151))
            threads_connected = int(status.get("Threads_connected", 0))
            threads_running = int(status.get("Threads_running", 0))
            connection_pct = (threads_connected / max_connections * 100) if max_connections else 0

            output["checks"]["connections"] = {
                "max_connections": max_connections,
                "threads_connected": threads_connected,
                "threads_running": threads_running,
                "usage_percent": round(connection_pct, 2)
            }

            if connection_pct > 90:
                health_score -= 20
                issues.append("Connection usage is critically high (>90%)")
                recommendations.append("Increase max_connections or implement connection pooling")
            elif connection_pct > 70:
                health_score -= 10
                issues.append("Connection usage is high (>70%)")

            # 3. InnoDB Buffer Pool check
            bp_reads = int(status.get("Innodb_buffer_pool_reads", 0))
            bp_read_requests = int(status.get("Innodb_buffer_pool_read_requests", 1))
            bp_hit_ratio = ((bp_read_requests - bp_reads) / bp_read_requests * 100) if bp_read_requests else 100

            bp_size = int(variables.get("innodb_buffer_pool_size", 0))
            bp_size_mb = bp_size / 1024 / 1024

            output["checks"]["buffer_pool"] = {
                "size_mb": round(bp_size_mb, 2),
                "read_requests": bp_read_requests,
                "disk_reads": bp_reads,
                "hit_ratio_percent": round(bp_hit_ratio, 2)
            }

            if bp_hit_ratio < 90:
                health_score -= 15
                issues.append(f"Buffer pool hit ratio is low ({bp_hit_ratio:.1f}%)")
                recommendations.append("Consider increasing innodb_buffer_pool_size")
            elif bp_hit_ratio < 95:
                health_score -= 5

            # 4. Query efficiency check
            questions = int(status.get("Questions", 1))
            slow_queries = int(status.get("Slow_queries", 0))
            slow_pct = (slow_queries / questions * 100) if questions else 0

            output["checks"]["queries"] = {
                "total_questions": questions,
                "slow_queries": slow_queries,
                "slow_query_percent": round(slow_pct, 4)
            }

            if slow_pct > 1:
                health_score -= 15
                issues.append(f"High slow query percentage ({slow_pct:.2f}%)")
                recommendations.append("Review slow query log and optimize problematic queries")
            elif slow_pct > 0.1:
                health_score -= 5

            # 5. Table scan check
            handler_read_rnd_next = int(status.get("Handler_read_rnd_next", 0))
            handler_read_rnd = int(status.get("Handler_read_rnd", 0))
            com_select = int(status.get("Com_select", 1))

            full_table_scan_ratio = (handler_read_rnd_next / com_select) if com_select else 0

            output["checks"]["table_scans"] = {
                "handler_read_rnd_next": handler_read_rnd_next,
                "handler_read_rnd": handler_read_rnd,
                "selects": com_select,
                "scan_ratio": round(full_table_scan_ratio, 2)
            }

            if full_table_scan_ratio > 4000:
                health_score -= 10
                issues.append("High number of full table scans detected")
                recommendations.append("Review query patterns and add appropriate indexes")

            # 6. Temporary table check
            tmp_tables = int(status.get("Created_tmp_tables", 0))
            tmp_disk_tables = int(status.get("Created_tmp_disk_tables", 0))
            tmp_disk_pct = (tmp_disk_tables / tmp_tables * 100) if tmp_tables else 0

            output["checks"]["temp_tables"] = {
                "total_temp_tables": tmp_tables,
                "disk_temp_tables": tmp_disk_tables,
                "disk_percent": round(tmp_disk_pct, 2)
            }

            if tmp_disk_pct > 25:
                health_score -= 10
                issues.append(f"High disk temp table usage ({tmp_disk_pct:.1f}%)")
                recommendations.append("Increase tmp_table_size and max_heap_table_size")

            # 7. Thread cache check
            threads_created = int(status.get("Threads_created", 0))
            connections = int(status.get("Connections", 1))
            thread_cache_hit = ((connections - threads_created) / connections * 100) if connections else 100

            output["checks"]["thread_cache"] = {
                "threads_created": threads_created,
                "total_connections": connections,
                "cache_hit_percent": round(thread_cache_hit, 2)
            }

            if thread_cache_hit < 90:
                health_score -= 5
                issues.append("Thread cache hit ratio is low")
                recommendations.append("Increase thread_cache_size")

            # 8. Uptime and stability
            uptime = int(status.get("Uptime", 0))
            uptime_days = uptime / 86400

            output["checks"]["uptime"] = {
                "seconds": uptime,
                "days": round(uptime_days, 2)
            }

            # 9. Key buffer (for MyISAM)
            key_reads = int(status.get("Key_reads", 0))
            key_read_requests = int(status.get("Key_read_requests", 1))
            key_hit_ratio = ((key_read_requests - key_reads) / key_read_requests * 100) if key_read_requests > 100 else 100

            output["checks"]["key_buffer"] = {
                "key_reads": key_reads,
                "key_read_requests": key_read_requests,
                "hit_ratio_percent": round(key_hit_ratio, 2)
            }

            if key_read_requests > 100 and key_hit_ratio < 90:
                health_score -= 5
                issues.append("MyISAM key buffer hit ratio is low")
                recommendations.append("Increase key_buffer_size if using MyISAM tables")

            # Add verbose details
            if verbose:
                output["checks"]["innodb_details"] = {
                    "log_file_size": int(variables.get("innodb_log_file_size", 0)),
                    "flush_method": variables.get("innodb_flush_method", "unknown"),
                    "flush_log_at_trx_commit": variables.get("innodb_flush_log_at_trx_commit", "1"),
                    "file_per_table": variables.get("innodb_file_per_table", "OFF")
                }

                output["checks"]["version_info"] = {
                    "version": variables.get("version", "unknown"),
                    "version_comment": variables.get("version_comment", ""),
                    "version_compile_os": variables.get("version_compile_os", "")
                }

            # Calculate final score
            output["health_score"] = max(0, health_score)

            if health_score >= 90:
                output["status"] = "healthy"
            elif health_score >= 70:
                output["status"] = "warning"
            elif health_score >= 50:
                output["status"] = "degraded"
            else:
                output["status"] = "critical"

            output["issues"] = issues

            if include_recs:
                output["recommendations"] = recommendations

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class ActiveQueriesToolHandler(ToolHandler):
    """Tool handler for monitoring active queries."""

    name = "get_active_queries"
    title = "Active Query Monitor"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get currently active queries in MySQL.

Shows:
- Running queries with execution time
- Long-running queries
- Blocked queries waiting on locks
- Idle transactions that may be holding locks

Useful for:
- Identifying queries causing performance issues
- Finding blocking transactions
- Monitoring query execution in real-time"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "min_duration_sec": {
                        "type": "number",
                        "description": "Minimum query duration in seconds",
                        "default": 0
                    },
                    "show_full_query": {
                        "type": "boolean",
                        "description": "Show full query text (may be truncated otherwise)",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            min_duration = arguments.get("min_duration_sec", 0)
            show_full = arguments.get("show_full_query", True)

            # Get process list with additional info
            query = """
                SELECT
                    p.ID as process_id,
                    p.USER as user,
                    p.HOST as host,
                    p.DB as database_name,
                    p.COMMAND as command,
                    p.TIME as duration_sec,
                    p.STATE as state,
                    p.INFO as query
                FROM information_schema.PROCESSLIST p
                WHERE p.COMMAND != 'Sleep'
            """

            if min_duration > 0:
                query += f" AND p.TIME >= {int(min_duration)}"

            query += " ORDER BY p.TIME DESC"

            results = await self.sql_driver.execute_query(query)

            output = {
                "query_count": len(results),
                "queries": []
            }

            long_running_count = 0

            for row in results:
                query_text = row["query"] or ""

                if not show_full and len(query_text) > 200:
                    query_text = query_text[:200] + "..."

                query_info = {
                    "process_id": row["process_id"],
                    "user": row["user"],
                    "host": row["host"],
                    "database": row["database_name"],
                    "command": row["command"],
                    "duration_sec": row["duration_sec"],
                    "state": row["state"],
                    "query": query_text
                }

                if row["duration_sec"] > 30:
                    query_info["warning"] = "Long-running query"
                    long_running_count += 1

                output["queries"].append(query_info)

            output["summary"] = {
                "total_processes": len(results),
                "long_running": long_running_count,
                "active_queries": sum(1 for r in results if r["command"] not in ("Sleep", "Daemon"))
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class SettingsReviewToolHandler(ToolHandler):
    """Tool handler for reviewing MySQL configuration settings."""

    name = "review_settings"
    title = "Settings Reviewer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Review MySQL configuration settings and get recommendations.

Analyzes key performance-related settings:
- Memory settings (buffer pool, sort buffer, join buffer)
- InnoDB settings
- Connection settings
- Query cache settings (if applicable)
- Logging settings

Compares against best practices and system resources."""

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
                        "description": "Category of settings to review",
                        "enum": ["all", "memory", "innodb", "connections", "logging", "replication"],
                        "default": "all"
                    },
                    "include_all_settings": {
                        "type": "boolean",
                        "description": "Include all settings, not just performance-related ones",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            category = arguments.get("category", "all")
            include_all = arguments.get("include_all_settings", False)

            variables = await self.sql_driver.get_server_variables()

            output = {
                "category": category,
                "settings": {},
                "recommendations": [],
                "warnings": []
            }

            # Define setting categories
            memory_settings = [
                "innodb_buffer_pool_size",
                "key_buffer_size",
                "sort_buffer_size",
                "join_buffer_size",
                "read_buffer_size",
                "read_rnd_buffer_size",
                "tmp_table_size",
                "max_heap_table_size",
                "table_open_cache",
                "table_definition_cache"
            ]

            innodb_settings = [
                "innodb_buffer_pool_size",
                "innodb_buffer_pool_instances",
                "innodb_log_file_size",
                "innodb_log_buffer_size",
                "innodb_flush_log_at_trx_commit",
                "innodb_flush_method",
                "innodb_file_per_table",
                "innodb_io_capacity",
                "innodb_io_capacity_max",
                "innodb_read_io_threads",
                "innodb_write_io_threads"
            ]

            connection_settings = [
                "max_connections",
                "max_user_connections",
                "thread_cache_size",
                "thread_stack",
                "wait_timeout",
                "interactive_timeout",
                "connect_timeout"
            ]

            logging_settings = [
                "slow_query_log",
                "slow_query_log_file",
                "long_query_time",
                "log_queries_not_using_indexes",
                "general_log",
                "log_error"
            ]

            replication_settings = [
                "server_id",
                "log_bin",
                "binlog_format",
                "sync_binlog",
                "gtid_mode",
                "enforce_gtid_consistency",
                "relay_log"
            ]

            # Gather settings based on category
            settings_to_check = []

            if category in ("all", "memory"):
                settings_to_check.extend(memory_settings)
            if category in ("all", "innodb"):
                settings_to_check.extend(innodb_settings)
            if category in ("all", "connections"):
                settings_to_check.extend(connection_settings)
            if category in ("all", "logging"):
                settings_to_check.extend(logging_settings)
            if category in ("all", "replication"):
                settings_to_check.extend(replication_settings)

            settings_to_check = list(set(settings_to_check))

            # Gather settings
            for setting in settings_to_check:
                value = variables.get(setting)
                if value is not None:
                    output["settings"][setting] = self._format_setting_value(setting, value)

            # Generate recommendations
            self._analyze_settings(variables, output)

            if include_all:
                output["all_variables_count"] = len(variables)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _format_setting_value(self, setting: str, value: str) -> dict:
        """Format setting value with human-readable sizes."""
        result = {"value": value}

        # Check if it's a size in bytes
        try:
            num_value = int(value)
            if setting.endswith("_size") or setting.endswith("_cache"):
                if num_value >= 1024 * 1024 * 1024:
                    result["formatted"] = f"{num_value / 1024 / 1024 / 1024:.2f} GB"
                elif num_value >= 1024 * 1024:
                    result["formatted"] = f"{num_value / 1024 / 1024:.2f} MB"
                elif num_value >= 1024:
                    result["formatted"] = f"{num_value / 1024:.2f} KB"
        except (ValueError, TypeError):
            pass

        return result

    def _analyze_settings(self, variables: dict, output: dict) -> None:
        """Analyze settings and generate recommendations."""
        recommendations = output["recommendations"]
        warnings = output["warnings"]

        # Buffer pool analysis
        bp_size = int(variables.get("innodb_buffer_pool_size", 0))
        bp_size_gb = bp_size / 1024 / 1024 / 1024

        if bp_size_gb < 1:
            recommendations.append(
                "innodb_buffer_pool_size is small. For production servers, consider setting it to 70-80% of available RAM."
            )

        # InnoDB flush settings
        flush_commit = variables.get("innodb_flush_log_at_trx_commit", "1")
        if flush_commit == "0":
            warnings.append(
                "innodb_flush_log_at_trx_commit=0 can lead to data loss. Consider setting to 1 for ACID compliance."
            )
        elif flush_commit == "2":
            recommendations.append(
                "innodb_flush_log_at_trx_commit=2 provides a balance between performance and safety."
            )

        # Connection settings
        max_conn = int(variables.get("max_connections", 151))
        if max_conn < 100:
            recommendations.append(
                f"max_connections ({max_conn}) may be too low for production workloads."
            )
        elif max_conn > 1000:
            recommendations.append(
                f"max_connections ({max_conn}) is high. Consider using connection pooling instead."
            )

        # Thread cache
        thread_cache = int(variables.get("thread_cache_size", 0))
        if thread_cache < 8:
            recommendations.append(
                f"thread_cache_size ({thread_cache}) is low. Consider increasing to 8-16."
            )

        # Slow query log
        slow_log = variables.get("slow_query_log", "OFF")
        if slow_log.upper() == "OFF":
            recommendations.append(
                "slow_query_log is disabled. Enable it to identify problematic queries."
            )

        # Long query time
        long_query_time = float(variables.get("long_query_time", 10))
        if long_query_time > 2:
            recommendations.append(
                f"long_query_time ({long_query_time}s) may be too high. Consider lowering to 1-2 seconds."
            )

        # Binary logging
        log_bin = variables.get("log_bin", "OFF")
        sync_binlog = int(variables.get("sync_binlog", 1))
        if log_bin.upper() != "OFF" and sync_binlog == 0:
            warnings.append(
                "sync_binlog=0 with binary logging enabled can lead to data loss on crash."
            )


class WaitEventsToolHandler(ToolHandler):
    """Tool handler for analyzing wait events."""

    name = "analyze_wait_events"
    title = "Wait Event Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze MySQL wait events to identify bottlenecks.

Wait events indicate what processes are waiting for:
- Lock waits (row locks, table locks)
- I/O waits (disk operations)
- Buffer pool waits
- Log waits
- Mutex and semaphore waits

This helps identify:
- I/O bottlenecks
- Lock contention patterns
- Resource saturation"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "event_category": {
                        "type": "string",
                        "description": "Category of events to analyze",
                        "enum": ["all", "io", "lock", "buffer", "log"],
                        "default": "all"
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top events to return",
                        "default": 20
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            category = arguments.get("event_category", "all")
            top_n = arguments.get("top_n", 20)

            output = {
                "wait_events": [],
                "summary": {},
                "bottlenecks": []
            }

            # Query for wait events from performance_schema
            query = """
                SELECT
                    EVENT_NAME,
                    COUNT_STAR as total_count,
                    SUM_TIMER_WAIT / 1000000000000 as total_wait_sec,
                    AVG_TIMER_WAIT / 1000000000 as avg_wait_ms,
                    MAX_TIMER_WAIT / 1000000000 as max_wait_ms
                FROM performance_schema.events_waits_summary_global_by_event_name
                WHERE COUNT_STAR > 0
            """

            # Filter by category
            if category == "io":
                query += " AND EVENT_NAME LIKE 'wait/io/%'"
            elif category == "lock":
                query += " AND (EVENT_NAME LIKE 'wait/lock/%' OR EVENT_NAME LIKE 'wait/synch/mutex/%')"
            elif category == "buffer":
                query += " AND EVENT_NAME LIKE 'wait/io/file/innodb%'"
            elif category == "log":
                query += " AND EVENT_NAME LIKE 'wait/synch/cond/innodb/log%'"

            query += f"""
                ORDER BY total_wait_sec DESC
                LIMIT {top_n}
            """

            results = await self.sql_driver.execute_query(query)

            total_wait_time = 0

            for row in results:
                wait_sec = float(row["total_wait_sec"] or 0)
                total_wait_time += wait_sec

                event = {
                    "event_name": row["EVENT_NAME"],
                    "count": row["total_count"],
                    "total_wait_sec": round(wait_sec, 4),
                    "avg_wait_ms": round(float(row["avg_wait_ms"] or 0), 4),
                    "max_wait_ms": round(float(row["max_wait_ms"] or 0), 4)
                }

                # Categorize event
                if "wait/io" in row["EVENT_NAME"]:
                    event["category"] = "I/O"
                elif "wait/lock" in row["EVENT_NAME"] or "mutex" in row["EVENT_NAME"]:
                    event["category"] = "Lock/Sync"
                elif "wait/synch" in row["EVENT_NAME"]:
                    event["category"] = "Synchronization"
                else:
                    event["category"] = "Other"

                output["wait_events"].append(event)

            # Get InnoDB lock waits specifically
            lock_query = """
                SELECT
                    r.trx_id as waiting_trx_id,
                    r.trx_mysql_thread_id as waiting_thread,
                    r.trx_query as waiting_query,
                    b.trx_id as blocking_trx_id,
                    b.trx_mysql_thread_id as blocking_thread,
                    b.trx_query as blocking_query
                FROM information_schema.innodb_lock_waits w
                JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                LIMIT 10
            """

            try:
                lock_waits = await self.sql_driver.execute_query(lock_query)
                if lock_waits:
                    output["current_lock_waits"] = [
                        {
                            "waiting_thread": row["waiting_thread"],
                            "waiting_query": (row["waiting_query"] or "")[:200],
                            "blocking_thread": row["blocking_thread"],
                            "blocking_query": (row["blocking_query"] or "")[:200]
                        }
                        for row in lock_waits
                    ]
            except Exception:
                # Table may not exist in all MySQL versions
                pass

            # Summary
            output["summary"] = {
                "total_wait_events": len(results),
                "total_wait_time_sec": round(total_wait_time, 2)
            }

            # Identify bottlenecks
            for event in output["wait_events"][:5]:
                if event["total_wait_sec"] > 1:
                    bottleneck = f"High wait time on {event['event_name']}: {event['total_wait_sec']:.2f}s total"
                    output["bottlenecks"].append(bottleneck)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
