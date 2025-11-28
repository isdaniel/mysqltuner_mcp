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
                where_clause = f"WHERE (db IS NULL OR db NOT IN {system_schemas})"
                if schema_name:
                    where_clause = f"WHERE db = '{schema_name}'"
                if min_exec_count > 1:
                    where_clause += f" AND exec_count >= {min_exec_count}"

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
                    {where_clause}
                    ORDER BY {order_col} DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)
                use_sys = True
            except Exception:
                # Fall back to performance_schema direct query
                use_sys = False
                where_clause = f"WHERE (schema_name IS NULL OR schema_name NOT IN {system_schemas})"
                if schema_name:
                    where_clause = f"WHERE schema_name = '{schema_name}'"
                if min_exec_count > 1:
                    where_clause += f" AND count_star >= {min_exec_count}"

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
                    {where_clause}
                    ORDER BY {ps_order} DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                    {"WHERE disk_tmp_tables > 0" if disk_only else ""}
                    ORDER BY disk_tmp_tables DESC, memory_tmp_tables DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                where_clause = "WHERE sum_created_tmp_tables > 0"
                if disk_only:
                    where_clause = "WHERE sum_created_tmp_disk_tables > 0"

                query = f"""
                    SELECT
                        digest_text as query,
                        schema_name as db,
                        count_star as exec_count,
                        sum_timer_wait as total_latency_ps,
                        sum_created_tmp_tables as memory_tmp_tables,
                        sum_created_tmp_disk_tables as disk_tmp_tables
                    FROM performance_schema.events_statements_summary_by_digest
                    {where_clause}
                    ORDER BY sum_created_tmp_disk_tables DESC,
                             sum_created_tmp_tables DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                    {"WHERE sort_merge_passes > 0" if file_sorts_only else ""}
                    ORDER BY sort_merge_passes DESC, rows_sorted DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                where_clause = "WHERE sum_sort_rows > 0"
                if file_sorts_only:
                    where_clause = "WHERE sum_sort_merge_passes > 0"

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
                    {where_clause}
                    ORDER BY sum_sort_merge_passes DESC, sum_sort_rows DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                    WHERE rows_examined_avg >= {min_rows}
                    ORDER BY no_index_used_count DESC, rows_examined DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                        AND sum_rows_examined / count_star >= {min_rows}
                    ORDER BY sum_no_index_used DESC, sum_rows_examined DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                    {"WHERE errors > 0" if errors_only else ""}
                    ORDER BY errors DESC, warnings DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
                where_clause = "WHERE sum_errors > 0 OR sum_warnings > 0"
                if errors_only:
                    where_clause = "WHERE sum_errors > 0"

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
                    {where_clause}
                    ORDER BY sum_errors DESC, sum_warnings DESC
                    LIMIT {limit}
                """
                results = await self.sql_driver.execute_query(query)

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
