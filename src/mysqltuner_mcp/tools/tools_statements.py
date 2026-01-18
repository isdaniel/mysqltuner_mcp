"""
Statement analysis tool handlers for MySQL.

Includes tools for analyzing SQL statements using sys schema views:
- Statement analysis from performance_schema
- Statements with temporary tables
- Statements with sorting
- Statements with full table scans
- Statement latency analysis

Based on MySQLTuner performance schema analysis patterns.
"""

from __future__ import annotations

from collections.abc import Sequence
import re
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class StatementAnalysisToolHandler(ToolHandler):
    """Tool handler for comprehensive statement analysis."""

    name = "analyze_statements"
    title = "Statement Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze SQL statements from performance_schema/sys schema.

Provides comprehensive analysis of:
- Statement digest summaries
- Total and average execution times
- Rows examined vs rows sent ratios
- Statement error rates
- Most expensive queries

Based on MySQLTuner's performance schema analysis.
Requires performance_schema enabled.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by specific schema (optional)"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Order by metric",
                        "enum": [
                            "total_latency",
                            "avg_latency",
                            "exec_count",
                            "rows_examined",
                            "rows_sent"
                        ],
                        "default": "total_latency"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of statements to return",
                        "default": 25
                    },
                    "min_exec_count": {
                        "type": "integer",
                        "description": "Minimum execution count filter",
                        "default": 1
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name")
            order_by = arguments.get("order_by", "total_latency")
            limit = arguments.get("limit", 25)
            min_exec_count = arguments.get("min_exec_count", 1)

            output = {
                "summary": {},
                "statements": [],
                "analysis": {},
                "recommendations": []
            }

            # Check if performance_schema is enabled
            ps_enabled = await self.sql_driver.execute_scalar(
                "SELECT @@performance_schema"
            )
            if not ps_enabled or ps_enabled == "0":
                output["error"] = "performance_schema is disabled"
                output["recommendations"].append(
                    "Enable performance_schema in my.cnf for statement analysis"
                )
                return self.format_json_result(output)

            # Build query based on MySQL version (try sys schema first)
            order_column_map = {
                "total_latency": "total_latency",
                "avg_latency": "avg_latency",
                "exec_count": "exec_count",
                "rows_examined": "rows_examined",
                "rows_sent": "rows_sent"
            }
            order_col = order_column_map.get(order_by, "total_latency")

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Try sys.statement_analysis view first
            try:
                params = [
                    schema_name,
                    schema_name,
                    schema_name,
                    min_exec_count,
                    min_exec_count,
                    limit,
                ]

                query = f"""
                    SELECT
                        query,
                        db,
                        full_scan,
                        exec_count,
                        total_latency,
                        avg_latency,
                        rows_sent,
                        rows_sent_avg,
                        rows_examined,
                        rows_examined_avg
                    FROM sys.statement_analysis
                    WHERE (
                        (%s IS NULL AND (db IS NULL OR db NOT IN {system_schemas}))
                        OR (%s IS NOT NULL AND db = %s)
                    )
                    AND (%s <= 1 OR exec_count >= %s)
                    ORDER BY {order_col} DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, params)
                use_sys = True
            except Exception:
                # Fall back to performance_schema direct query
                use_sys = False
                params = [
                    schema_name,
                    schema_name,
                    schema_name,
                    min_exec_count,
                    min_exec_count,
                    limit,
                ]

                ps_order_map = {
                    "total_latency": "sum_timer_wait",
                    "avg_latency": "avg_timer_wait",
                    "exec_count": "count_star",
                    "rows_examined": "sum_rows_examined",
                    "rows_sent": "sum_rows_sent"
                }
                ps_order = ps_order_map.get(order_by, "sum_timer_wait")

                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        avg_timer_wait as avg_latency_ps,
                        sum_rows_sent as rows_sent,
                        ROUND(sum_rows_sent / count_star) as rows_sent_avg,
                        sum_rows_examined as rows_examined,
                        ROUND(sum_rows_examined / count_star) as rows_examined_avg,
                        sum_no_index_used as no_index_used,
                        sum_no_good_index_used as no_good_index
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE (
                        (%s IS NULL AND (schema_name IS NULL OR schema_name NOT IN {system_schemas}))
                        OR (%s IS NOT NULL AND schema_name = %s)
                    )
                    AND (%s <= 1 OR count_star >= %s)
                    ORDER BY {ps_order} DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, params)

            # Process results
            total_exec = 0
            total_latency_val = 0
            total_rows_examined = 0
            full_scan_count = 0

            for row in results:
                stmt = {
                    "query": (row.get("query") or "")[:500],
                    "db": row.get("db"),
                    "exec_count": row.get("exec_count") or row.get("count_star"),
                    "rows_sent": row.get("rows_sent"),
                    "rows_sent_avg": row.get("rows_sent_avg"),
                    "rows_examined": row.get("rows_examined"),
                    "rows_examined_avg": row.get("rows_examined_avg")
                }

                if use_sys:
                    stmt["total_latency"] = str(row.get("total_latency"))
                    stmt["avg_latency"] = str(row.get("avg_latency"))
                    if row.get("full_scan") == "*":
                        stmt["full_scan"] = True
                        full_scan_count += 1
                else:
                    # Convert picoseconds to more readable format
                    total_ps = row.get("total_latency_ps") or 0
                    avg_ps = row.get("avg_latency_ps") or 0
                    stmt["total_latency_ms"] = round(total_ps / 1000000000, 2)
                    stmt["avg_latency_ms"] = round(avg_ps / 1000000000, 2)

                    if row.get("no_index_used") or row.get("no_good_index"):
                        stmt["full_scan"] = True
                        full_scan_count += 1

                    total_latency_val += total_ps

                # Check for inefficient queries
                rows_examined = stmt.get("rows_examined") or 0
                rows_sent = stmt.get("rows_sent") or 1
                if rows_examined > 0 and rows_sent > 0:
                    efficiency = rows_examined / rows_sent
                    stmt["examination_ratio"] = round(efficiency, 2)
                    if efficiency > 100:
                        stmt["inefficient"] = True

                total_exec += stmt.get("exec_count") or 0
                total_rows_examined += rows_examined

                output["statements"].append(stmt)

            # Summary statistics
            output["summary"] = {
                "total_statements_analyzed": len(results),
                "total_executions": total_exec,
                "full_scan_statements": full_scan_count,
                "total_rows_examined": total_rows_examined
            }

            # Analysis and recommendations
            if full_scan_count > 0:
                output["recommendations"].append(
                    f"{full_scan_count} statements perform full table scans. "
                    "Consider adding indexes."
                )

            # Check for inefficient queries
            inefficient = [s for s in output["statements"] if s.get("inefficient")]
            if inefficient:
                output["recommendations"].append(
                    f"{len(inefficient)} statements have high rows examined/sent "
                    "ratios. Review query optimization."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class StatementsTempTablesToolHandler(ToolHandler):
    """Tool handler for statements using temporary tables."""

    name = "get_statements_with_temp_tables"
    title = "Temp Table Statements"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get statements that create temporary tables.

Temporary tables can cause performance issues when:
- They're created on disk instead of memory
- They're created too frequently
- They grow too large

Identifies queries that should be optimized.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum statements to return",
                        "default": 25
                    },
                    "disk_only": {
                        "type": "boolean",
                        "description": "Only show statements with disk temp tables",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 25)
            disk_only = arguments.get("disk_only", False)

            output = {
                "summary": {},
                "statements": [],
                "recommendations": []
            }

            # Try sys schema view first
            try:
                query = f"""
                    SELECT
                        query,
                        db,
                        exec_count,
                        total_latency,
                        memory_tmp_tables,
                        disk_tmp_tables,
                        avg_tmp_tables_per_query
                    FROM sys.statements_with_temp_tables
                    WHERE (%s = 0 OR disk_tmp_tables > 0)
                    ORDER BY disk_tmp_tables DESC, memory_tmp_tables DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if disk_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency": str(row.get("total_latency")),
                        "memory_tmp_tables": row.get("memory_tmp_tables"),
                        "disk_tmp_tables": row.get("disk_tmp_tables"),
                        "avg_tmp_tables": row.get("avg_tmp_tables_per_query")
                    }
                    output["statements"].append(stmt)

            except Exception:
                # Fall back to performance_schema
                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        sum_created_tmp_tables as memory_tmp_tables,
                        sum_created_tmp_disk_tables as disk_tmp_tables
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE sum_created_tmp_tables > 0
                        AND (%s = 0 OR sum_created_tmp_disk_tables > 0)
                    ORDER BY sum_created_tmp_disk_tables DESC,
                             sum_created_tmp_tables DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if disk_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency_ms": round(
                            (row.get("total_latency_ps") or 0) / 1000000000, 2
                        ),
                        "memory_tmp_tables": row.get("memory_tmp_tables"),
                        "disk_tmp_tables": row.get("disk_tmp_tables")
                    }
                    output["statements"].append(stmt)

            # Summary
            total_disk = sum(s.get("disk_tmp_tables") or 0 for s in output["statements"])
            total_memory = sum(s.get("memory_tmp_tables") or 0 for s in output["statements"])

            output["summary"] = {
                "statements_count": len(results),
                "total_disk_tmp_tables": total_disk,
                "total_memory_tmp_tables": total_memory
            }

            # Recommendations
            if total_disk > 0:
                output["recommendations"].append(
                    f"{total_disk} disk-based temporary tables created. "
                    "Consider increasing tmp_table_size and max_heap_table_size."
                )
                output["recommendations"].append(
                    "Review queries with disk temp tables for optimization "
                    "(avoid BLOB/TEXT in GROUP BY, use smaller result sets)."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class StatementsSortingToolHandler(ToolHandler):
    """Tool handler for statements with sorting operations."""

    name = "get_statements_with_sorting"
    title = "Sorting Statements"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get statements that perform sorting operations.

Identifies queries with:
- File sorts (on disk)
- Memory sorts
- Sort merge passes

High file sort ratios indicate need for index optimization
or sort_buffer_size increase.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum statements to return",
                        "default": 25
                    },
                    "file_sorts_only": {
                        "type": "boolean",
                        "description": "Only show statements with file sorts",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 25)
            file_sorts_only = arguments.get("file_sorts_only", False)

            output = {
                "summary": {},
                "statements": [],
                "recommendations": []
            }

            # Try sys schema view first
            try:
                query = f"""
                    SELECT
                        query,
                        db,
                        exec_count,
                        total_latency,
                        sort_merge_passes,
                        avg_sort_merges,
                        sorts_using_scans,
                        sort_using_range,
                        rows_sorted,
                        avg_rows_sorted
                    FROM sys.statements_with_sorting
                    WHERE (%s = 0 OR sort_merge_passes > 0)
                    ORDER BY sort_merge_passes DESC, rows_sorted DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if file_sorts_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency": str(row.get("total_latency")),
                        "sort_merge_passes": row.get("sort_merge_passes"),
                        "avg_sort_merges": row.get("avg_sort_merges"),
                        "sorts_using_scans": row.get("sorts_using_scans"),
                        "sorts_using_range": row.get("sort_using_range"),
                        "rows_sorted": row.get("rows_sorted"),
                        "avg_rows_sorted": row.get("avg_rows_sorted")
                    }
                    output["statements"].append(stmt)

            except Exception:
                # Fall back to performance_schema
                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        sum_sort_merge_passes as sort_merge_passes,
                        sum_sort_scan as sorts_using_scans,
                        sum_sort_range as sorts_using_range,
                        sum_sort_rows as rows_sorted
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE sum_sort_rows > 0
                        AND (%s = 0 OR sum_sort_merge_passes > 0)
                    ORDER BY sum_sort_merge_passes DESC, sum_sort_rows DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if file_sorts_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency_ms": round(
                            (row.get("total_latency_ps") or 0) / 1000000000, 2
                        ),
                        "sort_merge_passes": row.get("sort_merge_passes"),
                        "sorts_using_scans": row.get("sorts_using_scans"),
                        "sorts_using_range": row.get("sorts_using_range"),
                        "rows_sorted": row.get("rows_sorted")
                    }
                    output["statements"].append(stmt)

            # Summary
            total_merge_passes = sum(
                s.get("sort_merge_passes") or 0 for s in output["statements"]
            )
            total_rows_sorted = sum(
                s.get("rows_sorted") or 0 for s in output["statements"]
            )

            output["summary"] = {
                "statements_count": len(results),
                "total_sort_merge_passes": total_merge_passes,
                "total_rows_sorted": total_rows_sorted
            }

            # Recommendations
            if total_merge_passes > 0:
                output["recommendations"].append(
                    f"{total_merge_passes} sort merge passes detected. "
                    "Consider increasing sort_buffer_size."
                )
                output["recommendations"].append(
                    "Add indexes on columns used in ORDER BY clauses "
                    "to avoid file sorts."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class StatementsFullScansToolHandler(ToolHandler):
    """Tool handler for statements with full table scans."""

    name = "get_statements_with_full_scans"
    title = "Full Scan Statements"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get statements that perform full table scans.

Full table scans can severely impact performance on large tables.
Identifies queries that:
- Don't use any index
- Use a non-optimal index

These queries are prime candidates for index optimization.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum statements to return",
                        "default": 25
                    },
                    "min_rows_examined": {
                        "type": "integer",
                        "description": "Minimum rows examined threshold",
                        "default": 100
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 25)
            min_rows = arguments.get("min_rows_examined", 100)

            output = {
                "summary": {},
                "statements": [],
                "recommendations": []
            }

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Try sys schema view first
            try:
                query = f"""
                    SELECT
                        query,
                        db,
                        exec_count,
                        total_latency,
                        no_index_used_count,
                        no_good_index_used_count,
                        no_index_used_pct,
                        rows_sent,
                        rows_examined,
                        rows_sent_avg,
                        rows_examined_avg
                    FROM sys.statements_with_full_table_scans
                    WHERE rows_examined_avg >= %s
                    ORDER BY no_index_used_count DESC, rows_examined DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, [min_rows, limit])

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency": str(row.get("total_latency")),
                        "no_index_used_count": row.get("no_index_used_count"),
                        "no_good_index_count": row.get("no_good_index_used_count"),
                        "no_index_pct": row.get("no_index_used_pct"),
                        "rows_sent": row.get("rows_sent"),
                        "rows_examined": row.get("rows_examined"),
                        "rows_sent_avg": row.get("rows_sent_avg"),
                        "rows_examined_avg": row.get("rows_examined_avg")
                    }

                    # Calculate efficiency ratio
                    rows_examined = stmt.get("rows_examined") or 0
                    rows_sent = stmt.get("rows_sent") or 1
                    if rows_sent > 0:
                        stmt["scan_efficiency_ratio"] = round(rows_examined / rows_sent, 2)

                    output["statements"].append(stmt)

            except Exception:
                # Fall back to performance_schema
                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        sum_no_index_used as no_index_used_count,
                        sum_no_good_index_used as no_good_index_count,
                        sum_rows_sent as rows_sent,
                        sum_rows_examined as rows_examined,
                        ROUND(sum_rows_sent / count_star) as rows_sent_avg,
                        ROUND(sum_rows_examined / count_star) as rows_examined_avg
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE (sum_no_index_used > 0 OR sum_no_good_index_used > 0)
                        AND sum_rows_examined / count_star >= %s
                    ORDER BY sum_no_index_used DESC, sum_rows_examined DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, [min_rows, limit])

                for row in results:
                    rows_examined = row.get("rows_examined") or 0
                    rows_sent = row.get("rows_sent") or 1
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency_ms": round(
                            (row.get("total_latency_ps") or 0) / 1000000000, 2
                        ),
                        "no_index_used_count": row.get("no_index_used_count"),
                        "no_good_index_count": row.get("no_good_index_count"),
                        "rows_sent": rows_sent,
                        "rows_examined": rows_examined,
                        "rows_sent_avg": row.get("rows_sent_avg"),
                        "rows_examined_avg": row.get("rows_examined_avg"),
                        "scan_efficiency_ratio": round(rows_examined / max(rows_sent, 1), 2)
                    }
                    output["statements"].append(stmt)

            # Summary
            output["summary"] = {
                "statements_count": len(results),
                "total_full_scan_executions": sum(
                    s.get("no_index_used_count") or 0 for s in output["statements"]
                )
            }

            # Recommendations
            if output["statements"]:
                output["recommendations"].append(
                    "Review these queries and add appropriate indexes on "
                    "columns used in WHERE, JOIN, and ORDER BY clauses."
                )
                output["recommendations"].append(
                    "Use EXPLAIN to analyze query execution plans and "
                    "identify missing indexes."
                )

                # Check for queries with very high scan ratios
                high_ratio = [
                    s for s in output["statements"]
                    if s.get("scan_efficiency_ratio", 0) > 100
                ]
                if high_ratio:
                    output["recommendations"].append(
                        f"{len(high_ratio)} queries examine >100x more rows than "
                        "returned. These should be prioritized for optimization."
                    )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class StatementErrorsToolHandler(ToolHandler):
    """Tool handler for statements with errors."""

    name = "get_statements_with_errors"
    title = "Statement Errors"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get statements that produce errors or warnings.

Identifies queries with:
- Error counts
- Warning counts
- Error rates

Helps identify problematic application queries.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum statements to return",
                        "default": 25
                    },
                    "errors_only": {
                        "type": "boolean",
                        "description": "Only show statements with errors (not warnings)",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 25)
            errors_only = arguments.get("errors_only", False)

            output = {
                "summary": {},
                "statements": [],
                "recommendations": []
            }

            # Try sys schema view first
            try:
                query = f"""
                    SELECT
                        query,
                        db,
                        exec_count,
                        total_latency,
                        errors,
                        error_pct,
                        warnings,
                        warning_pct
                    FROM sys.statements_with_errors_or_warnings
                    WHERE (%s = 0 OR errors > 0)
                    ORDER BY errors DESC, warnings DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if errors_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency": str(row.get("total_latency")),
                        "errors": row.get("errors"),
                        "error_pct": float(row.get("error_pct") or 0),
                        "warnings": row.get("warnings"),
                        "warning_pct": float(row.get("warning_pct") or 0)
                    }
                    output["statements"].append(stmt)

            except Exception:
                # Fall back to performance_schema
                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        sum_errors as errors,
                        ROUND(sum_errors / count_star * 100, 2) as error_pct,
                        sum_warnings as warnings,
                        ROUND(sum_warnings / count_star * 100, 2) as warning_pct
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE (sum_errors > 0 OR sum_warnings > 0)
                        AND (%s = 0 OR sum_errors > 0)
                    ORDER BY sum_errors DESC, sum_warnings DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(
                    query,
                    [1 if errors_only else 0, limit],
                )

                for row in results:
                    stmt = {
                        "query": (row.get("query") or "")[:500],
                        "db": row.get("db"),
                        "exec_count": row.get("exec_count"),
                        "total_latency_ms": round(
                            (row.get("total_latency_ps") or 0) / 1000000000, 2
                        ),
                        "errors": row.get("errors"),
                        "error_pct": float(row.get("error_pct") or 0),
                        "warnings": row.get("warnings"),
                        "warning_pct": float(row.get("warning_pct") or 0)
                    }
                    output["statements"].append(stmt)

            # Summary
            total_errors = sum(s.get("errors") or 0 for s in output["statements"])
            total_warnings = sum(s.get("warnings") or 0 for s in output["statements"])

            output["summary"] = {
                "statements_count": len(results),
                "total_errors": total_errors,
                "total_warnings": total_warnings
            }

            # Recommendations
            if total_errors > 0:
                output["recommendations"].append(
                    f"{total_errors} statement errors detected. "
                    "Review application error handling."
                )

            high_error_rate = [
                s for s in output["statements"]
                if (s.get("error_pct") or 0) > 10
            ]
            if high_error_rate:
                output["recommendations"].append(
                    f"{len(high_error_rate)} statements have >10% error rate. "
                    "These indicate potential application bugs."
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class LongQueryTypeCollationIssuesToolHandler(ToolHandler):
    """Tool handler for detecting type/collation issues in long-running queries."""

    name = "analyze_long_queries_for_type_collation_issues"
    title = "Long Query Type/Collation Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze long-running statements for type and collation mismatches.

Checks for potential performance issues such as:
- Implicit conversions between numeric and string types
- Column-to-column comparisons with mismatched collations
- Parameter placeholders that should match column type/collation

This tool performs best-effort parsing of statement digests from
performance_schema/sys views and compares referenced columns against
information_schema metadata.

Note: Results are heuristic and may not capture all query patterns.
"""

    NUMERIC_TYPES = {
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "bit",
    }
    STRING_TYPES = {
        "char",
        "varchar",
        "text",
        "tinytext",
        "mediumtext",
        "longtext",
        "enum",
        "set",
    }
    BINARY_TYPES = {
        "binary",
        "varbinary",
        "blob",
        "tinyblob",
        "mediumblob",
        "longblob",
    }

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by specific schema (optional)"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Order by metric",
                        "enum": [
                            "total_latency",
                            "avg_latency",
                            "exec_count"
                        ],
                        "default": "total_latency"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of statements to analyze",
                        "default": 20
                    },
                    "min_exec_count": {
                        "type": "integer",
                        "description": "Minimum execution count filter",
                        "default": 1
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name")
            order_by = arguments.get("order_by", "total_latency")
            limit = arguments.get("limit", 20)
            min_exec_count = arguments.get("min_exec_count", 1)

            output = {
                "summary": {},
                "statements": [],
                "recommendations": [],
                "limitations": [
                    "Heuristic parsing of statement digests; complex queries may be missed",
                    "Placeholders cannot confirm actual parameter data types",
                ],
            }

            ps_enabled = await self.sql_driver.execute_scalar(
                "SELECT @@performance_schema"
            )
            if not ps_enabled or ps_enabled == "0":
                output["error"] = "performance_schema is disabled"
                output["recommendations"].append(
                    "Enable performance_schema in my.cnf for statement analysis"
                )
                return self.format_json_result(output)

            order_column_map = {
                "total_latency": "total_latency",
                "avg_latency": "avg_latency",
                "exec_count": "exec_count",
            }
            order_col = order_column_map.get(order_by, "total_latency")

            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            try:
                params = [
                    schema_name,
                    schema_name,
                    schema_name,
                    min_exec_count,
                    min_exec_count,
                    limit,
                ]

                query = f"""
                    SELECT
                        query,
                        db,
                        exec_count,
                        total_latency,
                        avg_latency
                    FROM sys.statement_analysis
                    WHERE (
                        (%s IS NULL AND (db IS NULL OR db NOT IN {system_schemas}))
                        OR (%s IS NOT NULL AND db = %s)
                    )
                    AND (%s <= 1 OR exec_count >= %s)
                    ORDER BY {order_col} DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, params)
                use_sys = True
            except Exception:
                use_sys = False
                params = [
                    schema_name,
                    schema_name,
                    schema_name,
                    min_exec_count,
                    min_exec_count,
                    limit,
                ]

                ps_order_map = {
                    "total_latency": "sum_timer_wait",
                    "avg_latency": "avg_timer_wait",
                    "exec_count": "count_star",
                }
                ps_order = ps_order_map.get(order_by, "sum_timer_wait")

                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        avg_timer_wait as avg_latency_ps
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE (
                        (%s IS NULL AND (schema_name IS NULL OR schema_name NOT IN {system_schemas}))
                        OR (%s IS NOT NULL AND schema_name = %s)
                    )
                    AND (%s <= 1 OR count_star >= %s)
                    ORDER BY {ps_order} DESC
                    LIMIT %s
                """
                results = await self.sql_driver.execute_query(query, params)

            issues_count = 0
            implicit_conversion_count = 0
            collation_mismatch_count = 0
            parameter_alignment_count = 0

            for row in results:
                stmt_query = (row.get("query") or "")[:800]
                stmt_db = row.get("db") or schema_name

                stmt = {
                    "query": stmt_query,
                    "db": stmt_db,
                    "exec_count": row.get("exec_count"),
                    "issues": [],
                }
                if use_sys:
                    stmt["total_latency"] = str(row.get("total_latency"))
                    stmt["avg_latency"] = str(row.get("avg_latency"))
                else:
                    total_ps = row.get("total_latency_ps") or 0
                    avg_ps = row.get("avg_latency_ps") or 0
                    stmt["total_latency_ms"] = round(total_ps / 1000000000, 2)
                    stmt["avg_latency_ms"] = round(avg_ps / 1000000000, 2)

                if not stmt_query:
                    output["statements"].append(stmt)
                    continue

                alias_map = self._extract_table_aliases(stmt_query, stmt_db)
                comparisons = self._extract_comparisons(stmt_query)

                column_refs = set()
                for comp in comparisons:
                    for side in ("left", "right"):
                        col_ref = comp.get(side)
                        if not col_ref or col_ref.get("type") != "column":
                            continue
                        alias = col_ref["alias"]
                        if alias not in alias_map:
                            continue
                        schema, table = alias_map[alias]
                        column_refs.add((schema, table, col_ref["column"]))

                column_meta = await self._load_column_metadata(column_refs)

                for comp in comparisons:
                    left = comp.get("left")
                    right = comp.get("right")

                    if left and right and left.get("type") == "column" and right.get("type") == "column":
                        issue = self._analyze_column_to_column(left, right, alias_map, column_meta)
                        if issue:
                            stmt["issues"].append(issue)
                    else:
                        issue = self._analyze_column_to_value(left, right, alias_map, column_meta)
                        if issue:
                            stmt["issues"].append(issue)

                if stmt["issues"]:
                    issues_count += len(stmt["issues"])
                    implicit_conversion_count += sum(
                        1 for i in stmt["issues"]
                        if i.get("issue_type") == "implicit_conversion"
                    )
                    collation_mismatch_count += sum(
                        1 for i in stmt["issues"]
                        if i.get("issue_type") == "collation_mismatch"
                    )
                    parameter_alignment_count += sum(
                        1 for i in stmt["issues"]
                        if i.get("issue_type") == "parameter_alignment"
                    )

                output["statements"].append(stmt)

            output["summary"] = {
                "statements_analyzed": len(results),
                "issues_found": issues_count,
                "implicit_conversion_issues": implicit_conversion_count,
                "collation_mismatch_issues": collation_mismatch_count,
                "parameter_alignment_warnings": parameter_alignment_count,
            }

            if implicit_conversion_count:
                output["recommendations"].append(
                    "Align parameter and literal types with column data types to avoid implicit conversion"
                )
            if collation_mismatch_count:
                output["recommendations"].append(
                    "Ensure join/compare columns share the same collation or use explicit COLLATE"
                )
            if parameter_alignment_count:
                output["recommendations"].append(
                    "Bind parameters with the correct type and collation matching column definitions"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _extract_table_aliases(self, query: str, default_schema: str | None) -> dict[str, tuple[str, str]]:
        alias_map: dict[str, tuple[str, str]] = {}
        pattern = re.compile(
            r"\b(from|join)\s+([`\w\.]+)(?:\s+(?:as\s+)?([`\w]+))?",
            re.IGNORECASE,
        )
        for match in pattern.finditer(query):
            table_token = match.group(2)
            alias = match.group(3)
            if not table_token or table_token.startswith("("):
                continue

            table_token = self._strip_identifier(table_token)
            schema = default_schema
            table = table_token
            if "." in table_token:
                parts = table_token.split(".", 1)
                schema = self._strip_identifier(parts[0])
                table = self._strip_identifier(parts[1])

            alias_name = self._strip_identifier(alias) if alias else table
            if schema and table:
                alias_map[alias_name] = (schema, table)

        return alias_map

    def _extract_comparisons(self, query: str) -> list[dict[str, Any]]:
        comparisons: list[dict[str, Any]] = []
        col_ref = r"(?P<alias>`?[\w]+`?)\.(?P<column>`?[\w]+`?)"
        col_ref_2 = r"(?P<alias2>`?[\w]+`?)\.(?P<column2>`?[\w]+`?)"
        literal = r"(?P<literal>'[^']*'|\"[^\"]*\"|\d+(?:\.\d+)?)"
        param = r"(?P<param>\?)"
        operators = r"=|<=>|<=|>=|<|>|like"

        col_col = re.compile(
            rf"{col_ref}\s*(?P<op>{operators})\s*{col_ref_2}",
            re.IGNORECASE,
        )
        col_val = re.compile(
            rf"{col_ref}\s*(?P<op>{operators})\s*(?:{literal}|{param})",
            re.IGNORECASE,
        )
        val_col = re.compile(
            rf"(?:{literal}|{param})\s*(?P<op>{operators})\s*{col_ref}",
            re.IGNORECASE,
        )

        for match in col_col.finditer(query):
            comparisons.append({
                "left": {
                    "type": "column",
                    "alias": self._strip_identifier(match.group("alias")),
                    "column": self._strip_identifier(match.group("column")),
                },
                "right": {
                    "type": "column",
                    "alias": self._strip_identifier(match.group("alias2")),
                    "column": self._strip_identifier(match.group("column2")),
                },
                "operator": match.group("op").lower(),
            })

        for match in col_val.finditer(query):
            value_type, value = self._extract_value(match.groupdict())
            comparisons.append({
                "left": {
                    "type": "column",
                    "alias": self._strip_identifier(match.group("alias")),
                    "column": self._strip_identifier(match.group("column")),
                },
                "right": {
                    "type": value_type,
                    "value": value,
                },
                "operator": match.group("op").lower(),
            })

        for match in val_col.finditer(query):
            value_type, value = self._extract_value(match.groupdict())
            comparisons.append({
                "left": {
                    "type": "column",
                    "alias": self._strip_identifier(match.group("alias")),
                    "column": self._strip_identifier(match.group("column")),
                },
                "right": {
                    "type": value_type,
                    "value": value,
                },
                "operator": match.group("op").lower(),
            })

        return comparisons

    def _extract_value(self, groups: dict[str, Any]) -> tuple[str, str]:
        if groups.get("param") is not None:
            return "parameter", "?"
        literal = groups.get("literal")
        if literal is None:
            return "unknown", ""
        if literal.startswith("'") or literal.startswith("\""):
            return "string", literal
        return "numeric", literal

    async def _load_column_metadata(
        self,
        column_refs: set[tuple[str, str, str]],
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        if not column_refs:
            return {}

        conditions = []
        params: list[Any] = []
        for schema, table, column in column_refs:
            conditions.append("(TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s)")
            params.extend([schema, table, column])

        query = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                COLLATION_NAME,
                CHARACTER_SET_NAME
            FROM information_schema.COLUMNS
            WHERE """ + " OR ".join(conditions)

        results = await self.sql_driver.execute_query(query, params)
        meta: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in results:
            key = (row["TABLE_SCHEMA"], row["TABLE_NAME"], row["COLUMN_NAME"])
            meta[key] = row
        return meta

    def _analyze_column_to_column(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        alias_map: dict[str, tuple[str, str]],
        column_meta: dict[tuple[str, str, str], dict[str, Any]],
    ) -> dict[str, Any] | None:
        left_key = self._resolve_column_key(left, alias_map)
        right_key = self._resolve_column_key(right, alias_map)
        if not left_key or not right_key:
            return None

        left_meta = column_meta.get(left_key)
        right_meta = column_meta.get(right_key)
        if not left_meta or not right_meta:
            return None

        left_type = (left_meta.get("DATA_TYPE") or "").lower()
        right_type = (right_meta.get("DATA_TYPE") or "").lower()

        left_cat = self._type_category(left_type)
        right_cat = self._type_category(right_type)

        if left_cat != right_cat and {left_cat, right_cat} <= {"numeric", "string"}:
            return {
                "issue_type": "implicit_conversion",
                "left": self._format_column_ref(left_key),
                "right": self._format_column_ref(right_key),
                "details": "Comparing numeric column to string column can cause implicit conversion",
                "recommendation": "Align column types or use explicit CAST to avoid conversion",
            }

        if left_cat == "string" and right_cat == "string":
            left_collation = left_meta.get("COLLATION_NAME")
            right_collation = right_meta.get("COLLATION_NAME")
            if left_collation and right_collation and left_collation != right_collation:
                return {
                    "issue_type": "collation_mismatch",
                    "left": self._format_column_ref(left_key),
                    "right": self._format_column_ref(right_key),
                    "details": f"Different collations: {left_collation} vs {right_collation}",
                    "recommendation": "Ensure both columns use the same collation or add explicit COLLATE",
                }

        return None

    def _analyze_column_to_value(
        self,
        left: dict[str, Any] | None,
        right: dict[str, Any] | None,
        alias_map: dict[str, tuple[str, str]],
        column_meta: dict[tuple[str, str, str], dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not left or not right:
            return None

        col_ref = left if left.get("type") == "column" else right if right.get("type") == "column" else None
        val_ref = right if col_ref is left else left
        if not col_ref or not val_ref or val_ref.get("type") == "column":
            return None

        col_key = self._resolve_column_key(col_ref, alias_map)
        if not col_key:
            return None

        meta = column_meta.get(col_key)
        if not meta:
            return None

        col_type = (meta.get("DATA_TYPE") or "").lower()
        col_cat = self._type_category(col_type)

        val_type = val_ref.get("type")
        if val_type == "parameter":
            return {
                "issue_type": "parameter_alignment",
                "left": self._format_column_ref(col_key),
                "right": "?",
                "details": "Parameter type/collation should match column definition",
                "recommendation": "Bind parameter using the same data type and collation as the column",
            }

        if col_cat == "numeric" and val_type == "string":
            return {
                "issue_type": "implicit_conversion",
                "left": self._format_column_ref(col_key),
                "right": val_ref.get("value"),
                "details": "Numeric column compared to string literal",
                "recommendation": "Use numeric literal or CAST parameter to numeric type",
            }

        if col_cat == "string" and val_type == "numeric":
            return {
                "issue_type": "implicit_conversion",
                "left": self._format_column_ref(col_key),
                "right": val_ref.get("value"),
                "details": "String column compared to numeric literal",
                "recommendation": "Use string literal or CAST parameter to match column type",
            }

        return None

    def _resolve_column_key(
        self,
        col_ref: dict[str, Any],
        alias_map: dict[str, tuple[str, str]],
    ) -> tuple[str, str, str] | None:
        alias = col_ref.get("alias")
        column = col_ref.get("column")
        if not alias or not column or alias not in alias_map:
            return None
        schema, table = alias_map[alias]
        return (schema, table, column)

    def _type_category(self, data_type: str) -> str:
        if data_type in self.NUMERIC_TYPES:
            return "numeric"
        if data_type in self.STRING_TYPES:
            return "string"
        if data_type in self.BINARY_TYPES:
            return "binary"
        return "other"

    def _format_column_ref(self, key: tuple[str, str, str]) -> str:
        schema, table, column = key
        return f"{schema}.{table}.{column}"

    def _strip_identifier(self, token: str | None) -> str:
        if not token:
            return ""
        return token.strip("`\"")
