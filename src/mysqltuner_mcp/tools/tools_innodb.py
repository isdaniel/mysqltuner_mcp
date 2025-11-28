"""
InnoDB analysis tool handlers for MySQL.

Includes tools for:
- InnoDB engine status analysis (SHOW ENGINE INNODB STATUS)
- Buffer pool detailed analysis
- InnoDB log file analysis
- Transaction and lock analysis
- InnoDB metrics from performance_schema

Based on MySQLTuner InnoDB analysis patterns.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class InnoDBStatusToolHandler(ToolHandler):
    """Tool handler for InnoDB engine status analysis."""

    name = "get_innodb_status"
    title = "InnoDB Status Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze InnoDB engine status from SHOW ENGINE INNODB STATUS.

Parses and analyzes:
- Buffer pool statistics and hit ratios
- InnoDB log information and checkpoints
- Row operations (reads, inserts, updates, deletes)
- Transaction information and history list
- Semaphore waits and mutex contention
- Deadlock information (if any)
- I/O statistics and pending operations
- Redo log performance

Based on MySQLTuner's InnoDB analysis patterns.
Provides actionable recommendations for InnoDB optimization."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_raw_output": {
                        "type": "boolean",
                        "description": "Include raw INNODB STATUS output",
                        "default": False
                    },
                    "detailed_analysis": {
                        "type": "boolean",
                        "description": "Include detailed analysis with all metrics",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_raw = arguments.get("include_raw_output", False)
            detailed = arguments.get("detailed_analysis", True)

            output = {
                "innodb_status": {},
                "buffer_pool": {},
                "log_info": {},
                "transactions": {},
                "row_operations": {},
                "io_stats": {},
                "semaphores": {},
                "issues": [],
                "recommendations": []
            }

            # Get InnoDB status
            status_result = await self.sql_driver.execute_query(
                "SHOW ENGINE INNODB STATUS"
            )

            if status_result:
                raw_status = status_result[0].get("Status", "")
                if include_raw:
                    output["raw_status"] = raw_status[:10000]  # Limit size

                # Parse the status output
                self._parse_innodb_status(raw_status, output)

            # Get InnoDB variables for additional context
            variables = await self.sql_driver.get_server_variables("innodb%")
            status = await self.sql_driver.get_server_status("Innodb%")

            # Buffer pool analysis
            await self._analyze_buffer_pool(variables, status, output)

            # Log file analysis
            await self._analyze_log_files(variables, status, output)

            # InnoDB metrics from status
            await self._analyze_innodb_metrics(status, output)

            if detailed:
                # Additional detailed metrics
                await self._get_detailed_metrics(output)

            # Generate recommendations
            self._generate_recommendations(variables, status, output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _parse_innodb_status(self, raw_status: str, output: dict) -> None:
        """Parse SHOW ENGINE INNODB STATUS output."""

        # Parse SEMAPHORES section
        semaphore_waits = re.search(
            r'OS WAIT ARRAY INFO: reservation count (\d+)',
            raw_status
        )
        if semaphore_waits:
            output["semaphores"]["reservation_count"] = int(semaphore_waits.group(1))

        # Parse mutex spin waits
        mutex_spin = re.search(
            r'Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)',
            raw_status
        )
        if mutex_spin:
            output["semaphores"]["mutex_spin_waits"] = int(mutex_spin.group(1))
            output["semaphores"]["mutex_rounds"] = int(mutex_spin.group(2))
            output["semaphores"]["mutex_os_waits"] = int(mutex_spin.group(3))

        # Parse RW-shared and RW-excl
        rw_shared = re.search(
            r'RW-shared spins (\d+), rounds (\d+), OS waits (\d+)',
            raw_status
        )
        if rw_shared:
            output["semaphores"]["rw_shared_spins"] = int(rw_shared.group(1))
            output["semaphores"]["rw_shared_rounds"] = int(rw_shared.group(2))
            output["semaphores"]["rw_shared_os_waits"] = int(rw_shared.group(3))

        # Parse LATEST DETECTED DEADLOCK
        deadlock_match = re.search(
            r'LATEST DETECTED DEADLOCK\n-+\n(.*?)(?=\n-{3,}|\nTRANSACTIONS)',
            raw_status,
            re.DOTALL
        )
        if deadlock_match:
            output["deadlock_info"] = {
                "has_deadlock": True,
                "details": deadlock_match.group(1)[:2000]  # Limit size
            }
            output["issues"].append("Deadlock detected in InnoDB history")
        else:
            output["deadlock_info"] = {"has_deadlock": False}

        # Parse TRANSACTIONS section
        trx_match = re.search(r'Trx id counter (\d+)', raw_status)
        if trx_match:
            output["transactions"]["trx_id_counter"] = int(trx_match.group(1))

        history_list = re.search(r'History list length (\d+)', raw_status)
        if history_list:
            output["transactions"]["history_list_length"] = int(history_list.group(1))
            if int(history_list.group(1)) > 1000000:
                output["issues"].append(
                    f"History list length is very high ({history_list.group(1)})"
                )
                output["recommendations"].append(
                    "Long-running transactions may be preventing purge. "
                    "Check for uncommitted transactions."
                )

        # Parse ROW OPERATIONS
        row_ops = re.search(
            r'(\d+) queries inside InnoDB, (\d+) queries in queue',
            raw_status
        )
        if row_ops:
            output["row_operations"]["queries_inside"] = int(row_ops.group(1))
            output["row_operations"]["queries_in_queue"] = int(row_ops.group(2))

        reads_match = re.search(
            r'(\d+(?:\.\d+)?) reads/s, (\d+(?:\.\d+)?) creates/s, (\d+(?:\.\d+)?) writes/s',
            raw_status
        )
        if reads_match:
            output["row_operations"]["reads_per_sec"] = float(reads_match.group(1))
            output["row_operations"]["creates_per_sec"] = float(reads_match.group(2))
            output["row_operations"]["writes_per_sec"] = float(reads_match.group(3))

        # Parse BUFFER POOL AND MEMORY
        bp_size = re.search(
            r'Total large memory allocated (\d+)',
            raw_status
        )
        if bp_size:
            output["buffer_pool"]["total_memory_allocated"] = int(bp_size.group(1))

        pages_info = re.search(
            r'Buffer pool size\s+(\d+)',
            raw_status
        )
        if pages_info:
            output["buffer_pool"]["total_pages"] = int(pages_info.group(1))

        free_pages = re.search(r'Free buffers\s+(\d+)', raw_status)
        if free_pages:
            output["buffer_pool"]["free_pages"] = int(free_pages.group(1))

        modified_pages = re.search(r'Modified db pages\s+(\d+)', raw_status)
        if modified_pages:
            output["buffer_pool"]["modified_pages"] = int(modified_pages.group(1))

        # Parse LOG section
        log_seq = re.search(r'Log sequence number\s+(\d+)', raw_status)
        if log_seq:
            output["log_info"]["log_sequence_number"] = int(log_seq.group(1))

        log_flushed = re.search(r'Log flushed up to\s+(\d+)', raw_status)
        if log_flushed:
            output["log_info"]["log_flushed_up_to"] = int(log_flushed.group(1))

        pages_flushed = re.search(r'Pages flushed up to\s+(\d+)', raw_status)
        if pages_flushed:
            output["log_info"]["pages_flushed_up_to"] = int(pages_flushed.group(1))

        last_checkpoint = re.search(r'Last checkpoint at\s+(\d+)', raw_status)
        if last_checkpoint:
            output["log_info"]["last_checkpoint"] = int(last_checkpoint.group(1))

        # Parse pending I/O
        pending_reads = re.search(r'Pending normal aio reads:\s*(\d+)', raw_status)
        if pending_reads:
            output["io_stats"]["pending_normal_aio_reads"] = int(pending_reads.group(1))

        pending_writes = re.search(
            r'Pending normal aio writes:\s*(\d+)',
            raw_status
        )
        if pending_writes:
            output["io_stats"]["pending_normal_aio_writes"] = int(pending_writes.group(1))

        pending_ibuf = re.search(
            r'ibuf aio reads:\s*(\d+)',
            raw_status
        )
        if pending_ibuf:
            output["io_stats"]["pending_ibuf_aio_reads"] = int(pending_ibuf.group(1))

    async def _analyze_buffer_pool(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Analyze InnoDB buffer pool statistics."""

        bp_size = int(variables.get("innodb_buffer_pool_size", 0))
        bp_instances = int(variables.get("innodb_buffer_pool_instances", 1))

        output["buffer_pool"]["size_bytes"] = bp_size
        output["buffer_pool"]["size_mb"] = round(bp_size / 1024 / 1024, 2)
        output["buffer_pool"]["size_gb"] = round(bp_size / 1024 / 1024 / 1024, 2)
        output["buffer_pool"]["instances"] = bp_instances

        # Buffer pool page statistics
        pages_total = int(status.get("Innodb_buffer_pool_pages_total", 0))
        pages_free = int(status.get("Innodb_buffer_pool_pages_free", 0))
        pages_data = int(status.get("Innodb_buffer_pool_pages_data", 0))
        pages_dirty = int(status.get("Innodb_buffer_pool_pages_dirty", 0))

        output["buffer_pool"]["pages_total"] = pages_total
        output["buffer_pool"]["pages_free"] = pages_free
        output["buffer_pool"]["pages_data"] = pages_data
        output["buffer_pool"]["pages_dirty"] = pages_dirty

        if pages_total > 0:
            output["buffer_pool"]["usage_pct"] = round(
                (pages_total - pages_free) / pages_total * 100, 2
            )
            output["buffer_pool"]["dirty_pct"] = round(
                pages_dirty / pages_total * 100, 2
            )

        # Buffer pool hit ratio
        read_requests = int(status.get("Innodb_buffer_pool_read_requests", 0))
        reads = int(status.get("Innodb_buffer_pool_reads", 0))

        if read_requests > 0:
            hit_ratio = (read_requests - reads) / read_requests * 100
            output["buffer_pool"]["hit_ratio_pct"] = round(hit_ratio, 4)

            if hit_ratio < 99:
                output["issues"].append(
                    f"Buffer pool hit ratio is low ({hit_ratio:.2f}%)"
                )
                output["recommendations"].append(
                    "Consider increasing innodb_buffer_pool_size"
                )

        # Write statistics
        write_requests = int(status.get("Innodb_buffer_pool_write_requests", 0))
        pages_written = int(status.get("Innodb_buffer_pool_pages_flushed", 0))

        output["buffer_pool"]["write_requests"] = write_requests
        output["buffer_pool"]["pages_flushed"] = pages_written

    async def _analyze_log_files(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Analyze InnoDB log file configuration."""

        # Check for new MySQL 8.0.30+ innodb_redo_log_capacity
        redo_capacity = variables.get("innodb_redo_log_capacity")
        log_file_size = int(variables.get("innodb_log_file_size", 0))
        log_files_in_group = int(variables.get("innodb_log_files_in_group", 1))
        log_buffer_size = int(variables.get("innodb_log_buffer_size", 0))
        bp_size = int(variables.get("innodb_buffer_pool_size", 1))

        if redo_capacity:
            # MySQL 8.0.30+
            redo_capacity = int(redo_capacity)
            output["log_info"]["redo_log_capacity_bytes"] = redo_capacity
            output["log_info"]["redo_log_capacity_mb"] = round(
                redo_capacity / 1024 / 1024, 2
            )
            total_log_size = redo_capacity
        else:
            # Older versions
            output["log_info"]["log_file_size_bytes"] = log_file_size
            output["log_info"]["log_file_size_mb"] = round(
                log_file_size / 1024 / 1024, 2
            )
            output["log_info"]["log_files_in_group"] = log_files_in_group
            total_log_size = log_file_size * log_files_in_group

        output["log_info"]["total_log_size_mb"] = round(
            total_log_size / 1024 / 1024, 2
        )
        output["log_info"]["log_buffer_size_mb"] = round(
            log_buffer_size / 1024 / 1024, 2
        )

        # Calculate log size as percentage of buffer pool (should be 25%)
        if bp_size > 0:
            log_pct = total_log_size / bp_size * 100
            output["log_info"]["log_to_buffer_pool_pct"] = round(log_pct, 2)

            if log_pct < 20 or log_pct > 30:
                output["issues"].append(
                    f"InnoDB log size is {log_pct:.1f}% of buffer pool "
                    "(recommended: 25%)"
                )
                recommended_size = bp_size // 4
                output["recommendations"].append(
                    f"Consider setting innodb_log_file_size to "
                    f"{recommended_size // 1024 // 1024}MB for optimal performance"
                )

        # Log waits
        log_waits = int(status.get("Innodb_log_waits", 0))
        log_writes = int(status.get("Innodb_log_writes", 1))

        output["log_info"]["log_waits"] = log_waits
        output["log_info"]["log_writes"] = log_writes

        if log_writes > 0 and (log_waits / log_writes) > 0.01:
            output["issues"].append(
                f"InnoDB log waits detected ({log_waits} waits / {log_writes} writes)"
            )
            output["recommendations"].append(
                "Consider increasing innodb_log_buffer_size"
            )

    async def _analyze_innodb_metrics(self, status: dict, output: dict) -> None:
        """Analyze InnoDB metrics from global status."""

        # Row operations
        output["row_operations"]["rows_read"] = int(status.get("Innodb_rows_read", 0))
        output["row_operations"]["rows_inserted"] = int(
            status.get("Innodb_rows_inserted", 0)
        )
        output["row_operations"]["rows_updated"] = int(
            status.get("Innodb_rows_updated", 0)
        )
        output["row_operations"]["rows_deleted"] = int(
            status.get("Innodb_rows_deleted", 0)
        )

        # Data operations
        output["io_stats"]["data_read"] = int(status.get("Innodb_data_read", 0))
        output["io_stats"]["data_written"] = int(status.get("Innodb_data_written", 0))
        output["io_stats"]["data_reads"] = int(status.get("Innodb_data_reads", 0))
        output["io_stats"]["data_writes"] = int(status.get("Innodb_data_writes", 0))

        # OS log stats
        output["log_info"]["os_log_written"] = int(
            status.get("Innodb_os_log_written", 0)
        )
        output["log_info"]["os_log_fsyncs"] = int(
            status.get("Innodb_os_log_fsyncs", 0)
        )

    async def _get_detailed_metrics(self, output: dict) -> None:
        """Get detailed InnoDB metrics from information_schema and sys."""

        try:
            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Try to get buffer pool stats by schema
            bp_schema_query = f"""
                SELECT
                    object_schema,
                    ROUND(SUM(allocated) / 1024 / 1024, 2) as allocated_mb,
                    SUM(pages) as pages
                FROM sys.x$innodb_buffer_stats_by_schema
                WHERE object_schema NOT IN {system_schemas}
                GROUP BY object_schema
                ORDER BY allocated_mb DESC
                LIMIT 10
            """
            results = await self.sql_driver.execute_query(bp_schema_query)
            if results:
                output["buffer_pool"]["by_schema"] = [
                    {
                        "schema": row["object_schema"],
                        "allocated_mb": float(row["allocated_mb"] or 0),
                        "pages": row["pages"]
                    }
                    for row in results
                ]
        except Exception:
            # sys schema may not be available
            pass

        try:
            # Get InnoDB lock waits
            lock_waits_query = """
                SELECT COUNT(*) as wait_count
                FROM information_schema.innodb_trx
                WHERE trx_state = 'LOCK WAIT'
            """
            result = await self.sql_driver.execute_scalar(lock_waits_query)
            output["transactions"]["lock_wait_count"] = result or 0
        except Exception:
            pass

    def _generate_recommendations(
        self,
        variables: dict,
        status: dict,
        output: dict
    ) -> None:
        """Generate InnoDB-specific recommendations."""

        # File per table check
        file_per_table = variables.get("innodb_file_per_table", "OFF")
        if file_per_table.upper() != "ON":
            output["issues"].append("innodb_file_per_table is disabled")
            output["recommendations"].append(
                "Enable innodb_file_per_table for better tablespace management"
            )

        # Flush log at trx commit
        flush_log = variables.get("innodb_flush_log_at_trx_commit", "1")
        if flush_log == "0":
            output["issues"].append(
                "innodb_flush_log_at_trx_commit=0 risks data loss on crash"
            )
            output["recommendations"].append(
                "Set innodb_flush_log_at_trx_commit=1 for full ACID compliance"
            )

        # Buffer pool instances
        bp_size = int(variables.get("innodb_buffer_pool_size", 0))
        bp_instances = int(variables.get("innodb_buffer_pool_instances", 1))

        if bp_size > 1024 * 1024 * 1024:  # > 1GB
            recommended_instances = min(bp_size // (1024 * 1024 * 1024), 64)
            if bp_instances < recommended_instances:
                output["recommendations"].append(
                    f"Consider increasing innodb_buffer_pool_instances to "
                    f"{recommended_instances} for better concurrency"
                )

        # Check dirty page percentage
        dirty_pct = output["buffer_pool"].get("dirty_pct", 0)
        if dirty_pct > 75:
            output["issues"].append(
                f"High percentage of dirty pages ({dirty_pct:.1f}%)"
            )
            output["recommendations"].append(
                "Check disk I/O performance or increase innodb_io_capacity"
            )


class InnoDBBufferPoolToolHandler(ToolHandler):
    """Tool handler for detailed InnoDB buffer pool analysis."""

    name = "analyze_buffer_pool"
    title = "Buffer Pool Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Detailed analysis of InnoDB buffer pool usage.

Analyzes:
- Buffer pool allocation by schema and table
- Page types and distribution
- Hit ratios and efficiency metrics
- Memory allocation patterns
- Recommendations for buffer pool sizing

Note: When analyzing by schema/table, this tool only shows user/custom tables
and excludes MySQL system tables (mysql, information_schema, performance_schema, sys).

Uses sys schema views for detailed breakdown when available."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "by_schema": {
                        "type": "boolean",
                        "description": "Include breakdown by schema",
                        "default": True
                    },
                    "by_table": {
                        "type": "boolean",
                        "description": "Include breakdown by table (top N)",
                        "default": True
                    },
                    "top_n": {
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
            by_schema = arguments.get("by_schema", True)
            by_table = arguments.get("by_table", True)
            top_n = arguments.get("top_n", 20)

            output = {
                "buffer_pool_summary": {},
                "by_schema": [],
                "by_table": [],
                "recommendations": []
            }

            # Get buffer pool variables and status
            variables = await self.sql_driver.get_server_variables("innodb_buffer%")
            status = await self.sql_driver.get_server_status("Innodb_buffer%")

            bp_size = int(variables.get("innodb_buffer_pool_size", 0))
            pages_total = int(status.get("Innodb_buffer_pool_pages_total", 0))
            pages_free = int(status.get("Innodb_buffer_pool_pages_free", 0))
            pages_data = int(status.get("Innodb_buffer_pool_pages_data", 0))
            pages_dirty = int(status.get("Innodb_buffer_pool_pages_dirty", 0))
            pages_misc = int(status.get("Innodb_buffer_pool_pages_misc", 0))

            read_requests = int(status.get("Innodb_buffer_pool_read_requests", 0))
            reads = int(status.get("Innodb_buffer_pool_reads", 0))

            output["buffer_pool_summary"] = {
                "size_gb": round(bp_size / 1024 / 1024 / 1024, 2),
                "pages_total": pages_total,
                "pages_free": pages_free,
                "pages_data": pages_data,
                "pages_dirty": pages_dirty,
                "pages_misc": pages_misc,
                "usage_pct": round((pages_total - pages_free) / max(pages_total, 1) * 100, 2),
                "dirty_pct": round(pages_dirty / max(pages_total, 1) * 100, 2),
                "hit_ratio_pct": round(
                    (read_requests - reads) / max(read_requests, 1) * 100, 4
                ),
                "read_requests": read_requests,
                "disk_reads": reads
            }

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Get breakdown by schema
            if by_schema:
                try:
                    schema_query = f"""
                        SELECT
                            object_schema,
                            ROUND(allocated / 1024 / 1024, 2) as allocated_mb,
                            ROUND(data / 1024 / 1024, 2) as data_mb,
                            pages
                        FROM sys.innodb_buffer_stats_by_schema
                        WHERE object_schema NOT IN {system_schemas}
                        ORDER BY allocated DESC
                    """
                    schema_results = await self.sql_driver.execute_query(schema_query)
                    output["by_schema"] = [
                        {
                            "schema": row["object_schema"],
                            "allocated_mb": float(row["allocated_mb"] or 0),
                            "data_mb": float(row["data_mb"] or 0),
                            "pages": row["pages"]
                        }
                        for row in schema_results
                    ]
                except Exception:
                    output["by_schema"] = []
                    output["recommendations"].append(
                        "Install sys schema for detailed buffer pool analysis"
                    )

            # Get breakdown by table
            if by_table:
                try:
                    table_query = f"""
                        SELECT
                            object_schema,
                            object_name,
                            ROUND(allocated / 1024 / 1024, 2) as allocated_mb,
                            ROUND(data / 1024 / 1024, 2) as data_mb,
                            pages
                        FROM sys.innodb_buffer_stats_by_table
                        WHERE object_schema NOT IN {system_schemas}
                        ORDER BY allocated DESC
                        LIMIT {top_n}
                    """
                    table_results = await self.sql_driver.execute_query(table_query)
                    output["by_table"] = [
                        {
                            "schema": row["object_schema"],
                            "table": row["object_name"],
                            "allocated_mb": float(row["allocated_mb"] or 0),
                            "data_mb": float(row["data_mb"] or 0),
                            "pages": row["pages"]
                        }
                        for row in table_results
                    ]
                except Exception:
                    output["by_table"] = []

            # Generate recommendations
            if output["buffer_pool_summary"]["hit_ratio_pct"] < 99:
                output["recommendations"].append(
                    f"Buffer pool hit ratio ({output['buffer_pool_summary']['hit_ratio_pct']}%) "
                    "is below optimal. Consider increasing innodb_buffer_pool_size."
                )

            if output["buffer_pool_summary"]["dirty_pct"] > 50:
                output["recommendations"].append(
                    "High percentage of dirty pages. Check I/O subsystem or "
                    "increase innodb_io_capacity."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class InnoDBTransactionsToolHandler(ToolHandler):
    """Tool handler for InnoDB transaction analysis."""

    name = "analyze_innodb_transactions"
    title = "InnoDB Transaction Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze InnoDB transactions and locking.

Identifies:
- Long-running transactions
- Lock waits and blocking transactions
- Deadlock history
- History list length (purge lag)
- Transaction isolation levels

Helps identify transaction-related performance issues."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_queries": {
                        "type": "boolean",
                        "description": "Include transaction queries",
                        "default": True
                    },
                    "min_duration_sec": {
                        "type": "integer",
                        "description": "Minimum transaction duration to include",
                        "default": 0
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_queries = arguments.get("include_queries", True)
            min_duration = arguments.get("min_duration_sec", 0)

            output = {
                "transaction_summary": {},
                "active_transactions": [],
                "lock_waits": [],
                "issues": [],
                "recommendations": []
            }

            # Get transaction isolation level
            iso_level = await self.sql_driver.execute_scalar(
                "SELECT @@transaction_isolation"
            )
            output["transaction_summary"]["isolation_level"] = iso_level

            # Get active transactions
            trx_query = """
                SELECT
                    trx_id,
                    trx_state,
                    trx_started,
                    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_sec,
                    trx_requested_lock_id,
                    trx_wait_started,
                    trx_weight,
                    trx_mysql_thread_id,
                    trx_query,
                    trx_operation_state,
                    trx_tables_in_use,
                    trx_tables_locked,
                    trx_lock_structs,
                    trx_rows_locked,
                    trx_rows_modified
                FROM information_schema.innodb_trx
                ORDER BY trx_started
            """
            trx_results = await self.sql_driver.execute_query(trx_query)

            long_running_count = 0
            lock_wait_count = 0

            for row in trx_results:
                duration = row["duration_sec"] or 0

                if duration < min_duration:
                    continue

                trx_info = {
                    "trx_id": row["trx_id"],
                    "state": row["trx_state"],
                    "duration_sec": duration,
                    "thread_id": row["trx_mysql_thread_id"],
                    "tables_in_use": row["trx_tables_in_use"],
                    "tables_locked": row["trx_tables_locked"],
                    "rows_locked": row["trx_rows_locked"],
                    "rows_modified": row["trx_rows_modified"]
                }

                if include_queries and row["trx_query"]:
                    trx_info["query"] = row["trx_query"][:500]

                if row["trx_state"] == "LOCK WAIT":
                    lock_wait_count += 1
                    trx_info["waiting_for_lock"] = True

                if duration > 60:
                    long_running_count += 1
                    trx_info["is_long_running"] = True

                output["active_transactions"].append(trx_info)

            output["transaction_summary"]["total_active"] = len(trx_results)
            output["transaction_summary"]["lock_wait_count"] = lock_wait_count
            output["transaction_summary"]["long_running_count"] = long_running_count

            # Get lock wait information
            try:
                # MySQL 8.0+ uses data_locks and data_lock_waits
                lock_waits_query = """
                    SELECT
                        r.trx_id as waiting_trx_id,
                        r.trx_mysql_thread_id as waiting_thread,
                        r.trx_query as waiting_query,
                        b.trx_id as blocking_trx_id,
                        b.trx_mysql_thread_id as blocking_thread,
                        b.trx_query as blocking_query,
                        TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) as wait_duration_sec
                    FROM performance_schema.data_lock_waits w
                    JOIN information_schema.innodb_trx r
                        ON r.trx_id = w.REQUESTING_ENGINE_TRANSACTION_ID
                    JOIN information_schema.innodb_trx b
                        ON b.trx_id = w.BLOCKING_ENGINE_TRANSACTION_ID
                """
                lock_results = await self.sql_driver.execute_query(lock_waits_query)

                for row in lock_results:
                    lock_info = {
                        "waiting_thread": row["waiting_thread"],
                        "waiting_query": (row["waiting_query"] or "")[:300],
                        "blocking_thread": row["blocking_thread"],
                        "blocking_query": (row["blocking_query"] or "")[:300],
                        "wait_duration_sec": row["wait_duration_sec"]
                    }
                    output["lock_waits"].append(lock_info)
            except Exception:
                # Try older MySQL version table
                try:
                    old_lock_query = """
                        SELECT
                            r.trx_id as waiting_trx_id,
                            r.trx_mysql_thread_id as waiting_thread,
                            b.trx_id as blocking_trx_id,
                            b.trx_mysql_thread_id as blocking_thread
                        FROM information_schema.innodb_lock_waits w
                        JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                        JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                    """
                    old_results = await self.sql_driver.execute_query(old_lock_query)
                    for row in old_results:
                        output["lock_waits"].append({
                            "waiting_thread": row["waiting_thread"],
                            "blocking_thread": row["blocking_thread"]
                        })
                except Exception:
                    pass

            # Get history list length
            status = await self.sql_driver.get_server_status("Innodb_history%")
            history_length = int(status.get("Innodb_history_list_length", 0))
            output["transaction_summary"]["history_list_length"] = history_length

            # Generate issues and recommendations
            if long_running_count > 0:
                output["issues"].append(
                    f"{long_running_count} long-running transaction(s) detected"
                )
                output["recommendations"].append(
                    "Review and optimize long-running transactions"
                )

            if lock_wait_count > 0:
                output["issues"].append(
                    f"{lock_wait_count} transaction(s) waiting for locks"
                )

            if history_length > 100000:
                output["issues"].append(
                    f"High history list length ({history_length}) - purge lag detected"
                )
                output["recommendations"].append(
                    "Long-running transactions preventing purge. "
                    "Consider increasing innodb_purge_threads."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
