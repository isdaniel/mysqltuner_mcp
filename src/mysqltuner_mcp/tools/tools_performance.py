"""
Performance analysis tool handlers for MySQL.

Includes tools for:
- Slow query analysis
- Query execution plan analysis (EXPLAIN)
- Table statistics and metrics
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class GetSlowQueriesToolHandler(ToolHandler):
    """Tool handler for retrieving slow queries from MySQL."""

    name = "get_slow_queries"
    title = "Slow Query Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Retrieve slow queries from MySQL performance_schema.

Returns the top N slowest queries with detailed statistics:
- Total execution time
- Number of calls
- Average execution time
- Rows examined vs rows sent
- Full table scans
- Temporary tables usage

Requires performance_schema to be enabled (default in MySQL 5.6+).
For older versions, use the slow query log instead.

Note: This tool excludes queries against MySQL system schemas
(mysql, information_schema, performance_schema, sys) to focus on
user/application query performance."""

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
                        "description": "Maximum number of slow queries to return (default: 10)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 100
                    },
                    "min_exec_time_ms": {
                        "type": "number",
                        "description": "Minimum total execution time in milliseconds (default: 0)",
                        "default": 0
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Column to order results by",
                        "enum": ["total_time", "avg_time", "calls", "rows_examined"],
                        "default": "total_time"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by schema/database name (optional)"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 10)
            min_exec_time_ms = arguments.get("min_exec_time_ms", 0)
            order_by = arguments.get("order_by", "total_time")
            schema_name = arguments.get("schema_name")

            # Map order_by to actual column names
            order_map = {
                "total_time": "SUM_TIMER_WAIT",
                "avg_time": "AVG_TIMER_WAIT",
                "calls": "COUNT_STAR",
                "rows_examined": "SUM_ROWS_EXAMINED"
            }
            order_column = order_map.get(order_by, "SUM_TIMER_WAIT")

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Build query for performance_schema
            query = f"""
                SELECT
                    DIGEST_TEXT as query_text,
                    SCHEMA_NAME as schema_name,
                    COUNT_STAR as exec_count,
                    ROUND(SUM_TIMER_WAIT / 1000000000000, 4) as total_time_sec,
                    ROUND(AVG_TIMER_WAIT / 1000000000000, 6) as avg_time_sec,
                    ROUND(MAX_TIMER_WAIT / 1000000000000, 6) as max_time_sec,
                    SUM_ROWS_EXAMINED as rows_examined,
                    SUM_ROWS_SENT as rows_sent,
                    SUM_ROWS_AFFECTED as rows_affected,
                    SUM_NO_INDEX_USED as full_scans,
                    SUM_NO_GOOD_INDEX_USED as no_good_index,
                    SUM_CREATED_TMP_TABLES as tmp_tables,
                    SUM_CREATED_TMP_DISK_TABLES as tmp_disk_tables,
                    SUM_SELECT_FULL_JOIN as full_joins,
                    SUM_SORT_ROWS as sort_rows,
                    FIRST_SEEN as first_seen,
                    LAST_SEEN as last_seen
                FROM performance_schema.events_statements_summary_by_digest
                WHERE DIGEST_TEXT IS NOT NULL
                    AND SUM_TIMER_WAIT >= %s
                    AND (SCHEMA_NAME IS NULL OR SCHEMA_NAME NOT IN {system_schemas})
            """

            params = [min_exec_time_ms * 1000000000]  # Convert ms to picoseconds

            if schema_name:
                query += " AND SCHEMA_NAME = %s"
                params.append(schema_name)

            query += f" ORDER BY {order_column} DESC LIMIT %s"
            params.append(limit)

            results = await self.sql_driver.execute_query(query, params)

            # Format results
            output = {
                "total_queries": len(results),
                "filters": {
                    "limit": limit,
                    "min_exec_time_ms": min_exec_time_ms,
                    "order_by": order_by,
                    "schema_name": schema_name
                },
                "queries": []
            }

            for row in results:
                query_info = {
                    "query": row["query_text"][:500] if row["query_text"] else None,
                    "schema": row["schema_name"],
                    "execution_count": row["exec_count"],
                    "total_time_sec": float(row["total_time_sec"] or 0),
                    "avg_time_sec": float(row["avg_time_sec"] or 0),
                    "max_time_sec": float(row["max_time_sec"] or 0),
                    "rows_examined": row["rows_examined"],
                    "rows_sent": row["rows_sent"],
                    "rows_affected": row["rows_affected"],
                    "full_table_scans": row["full_scans"],
                    "no_good_index_used": row["no_good_index"],
                    "tmp_tables_created": row["tmp_tables"],
                    "tmp_disk_tables_created": row["tmp_disk_tables"],
                    "full_joins": row["full_joins"],
                    "sort_rows": row["sort_rows"],
                    "first_seen": str(row["first_seen"]) if row["first_seen"] else None,
                    "last_seen": str(row["last_seen"]) if row["last_seen"] else None
                }

                # Calculate efficiency metrics
                if row["rows_examined"] and row["rows_sent"]:
                    query_info["efficiency_ratio"] = round(
                        row["rows_sent"] / max(row["rows_examined"], 1) * 100, 2
                    )

                output["queries"].append(query_info)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class AnalyzeQueryToolHandler(ToolHandler):
    """Tool handler for analyzing query execution plans."""

    name = "analyze_query"
    title = "Query Execution Plan Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze a MySQL query's execution plan using EXPLAIN.

Provides detailed analysis of:
- Query execution plan with access types
- Index usage and potential missing indexes
- Join types and optimization opportunities
- Rows examined estimates
- Key usage and key length

Supports EXPLAIN FORMAT=JSON for MySQL 5.6+ for detailed cost analysis.
Use EXPLAIN ANALYZE (MySQL 8.0.18+) for actual execution statistics.

WARNING: With analyze=true, the query is actually executed!"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "Use EXPLAIN ANALYZE to get actual execution stats (MySQL 8.0.18+)",
                        "default": False
                    },
                    "format": {
                        "type": "string",
                        "description": "Output format for the execution plan",
                        "enum": ["traditional", "json", "tree"],
                        "default": "json"
                    }
                },
                "required": ["query"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            self.validate_required_args(arguments, ["query"])

            query = arguments["query"]
            analyze = arguments.get("analyze", False)
            format_type = arguments.get("format", "json")

            # Build EXPLAIN query
            if analyze:
                explain_query = f"EXPLAIN ANALYZE {query}"
            elif format_type == "json":
                explain_query = f"EXPLAIN FORMAT=JSON {query}"
            elif format_type == "tree":
                explain_query = f"EXPLAIN FORMAT=TREE {query}"
            else:
                explain_query = f"EXPLAIN {query}"

            # Execute EXPLAIN
            results = await self.sql_driver.execute_query(explain_query)

            output = {
                "query": query,
                "analyze_mode": analyze,
                "format": format_type,
                "plan": None,
                "analysis": {
                    "issues": [],
                    "recommendations": []
                }
            }

            if format_type == "json" and results:
                # Parse JSON format
                import json as json_module
                plan_json = results[0].get("EXPLAIN")
                if plan_json:
                    output["plan"] = json_module.loads(plan_json)
                    self._analyze_json_plan(output)
            elif format_type == "tree" and results:
                # Tree format is a single text result
                output["plan"] = results[0].get("EXPLAIN", "")
            else:
                # Traditional format
                output["plan"] = results
                self._analyze_traditional_plan(output, results)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _analyze_json_plan(self, output: dict) -> None:
        """Analyze JSON format EXPLAIN output."""
        plan = output.get("plan", {})
        query_block = plan.get("query_block", {})

        # Check for full table scans
        self._check_table_access(query_block, output)

    def _check_table_access(self, block: dict, output: dict, depth: int = 0) -> None:
        """Recursively check table access methods."""
        # Check table
        if "table" in block:
            table = block["table"]
            access_type = table.get("access_type", "")
            table_name = table.get("table_name", "unknown")

            if access_type in ("ALL", "index"):
                output["analysis"]["issues"].append(
                    f"Full table/index scan on '{table_name}' (access_type: {access_type})"
                )
                output["analysis"]["recommendations"].append(
                    f"Consider adding an index on '{table_name}' for the columns in WHERE/JOIN clause"
                )

            if table.get("using_filesort"):
                output["analysis"]["issues"].append(
                    f"Using filesort on '{table_name}'"
                )
                output["analysis"]["recommendations"].append(
                    f"Consider adding an index that matches the ORDER BY clause"
                )

            if table.get("using_temporary"):
                output["analysis"]["issues"].append(
                    f"Using temporary table for '{table_name}'"
                )

        # Check nested loops
        if "nested_loop" in block:
            for nested in block["nested_loop"]:
                self._check_table_access(nested, output, depth + 1)

        # Check ordering operation
        if "ordering_operation" in block:
            ordering = block["ordering_operation"]
            if ordering.get("using_filesort"):
                output["analysis"]["issues"].append("Query uses filesort for ordering")
            if "nested_loop" in ordering:
                for nested in ordering["nested_loop"]:
                    self._check_table_access(nested, output, depth + 1)

    def _analyze_traditional_plan(self, output: dict, results: list) -> None:
        """Analyze traditional EXPLAIN output."""
        for row in results:
            table_name = row.get("table", "unknown")
            access_type = row.get("type", "")

            # Check for problematic access types
            if access_type == "ALL":
                output["analysis"]["issues"].append(
                    f"Full table scan on '{table_name}'"
                )
                output["analysis"]["recommendations"].append(
                    f"Add an index on '{table_name}' for filtered/joined columns"
                )
            elif access_type == "index":
                output["analysis"]["issues"].append(
                    f"Full index scan on '{table_name}'"
                )

            # Check for missing keys
            possible_keys = row.get("possible_keys")
            key_used = row.get("key")

            if not possible_keys and access_type in ("ALL", "index"):
                output["analysis"]["recommendations"].append(
                    f"No suitable index found for '{table_name}' - consider creating one"
                )
            elif possible_keys and not key_used:
                output["analysis"]["issues"].append(
                    f"Index available but not used on '{table_name}'"
                )

            # Check Extra column for warnings
            extra = row.get("Extra", "")
            if "Using filesort" in extra:
                output["analysis"]["issues"].append(
                    f"Using filesort on '{table_name}'"
                )
            if "Using temporary" in extra:
                output["analysis"]["issues"].append(
                    f"Using temporary table on '{table_name}'"
                )
            if "Using where" in extra and access_type == "ALL":
                output["analysis"]["recommendations"].append(
                    f"Filtering after full scan on '{table_name}' - index would help"
                )


class TableStatsToolHandler(ToolHandler):
    """Tool handler for retrieving table statistics."""

    name = "get_table_stats"
    title = "Table Statistics Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get detailed statistics for MySQL user tables.

Returns information about:
- Table size (data, indexes, total)
- Row counts and average row length
- Index information
- Auto-increment values
- Table fragmentation
- Engine type and collation

Helps identify tables that may need:
- Optimization (OPTIMIZE TABLE)
- Index improvements
- Partitioning consideration

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys)."""

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
                        "description": "Schema/database to analyze (uses current database if not specified)"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Specific table to analyze (analyzes all tables if not provided)"
                    },
                    "include_indexes": {
                        "type": "boolean",
                        "description": "Include index statistics",
                        "default": True
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Order results by this metric",
                        "enum": ["size", "rows", "data_free", "name"],
                        "default": "size"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name")
            table_name = arguments.get("table_name")
            include_indexes = arguments.get("include_indexes", True)
            order_by = arguments.get("order_by", "size")

            # Get current database if schema not specified
            if not schema_name:
                result = await self.sql_driver.execute_scalar("SELECT DATABASE()")
                schema_name = result

            # Map order_by to columns
            order_map = {
                "size": "(DATA_LENGTH + INDEX_LENGTH)",
                "rows": "TABLE_ROWS",
                "data_free": "DATA_FREE",
                "name": "TABLE_NAME"
            }
            order_column = order_map.get(order_by, "(DATA_LENGTH + INDEX_LENGTH)")

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Build table stats query
            query = f"""
                SELECT
                    TABLE_NAME,
                    TABLE_TYPE,
                    ENGINE,
                    ROW_FORMAT,
                    TABLE_ROWS,
                    AVG_ROW_LENGTH,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    DATA_FREE,
                    AUTO_INCREMENT,
                    CREATE_TIME,
                    UPDATE_TIME,
                    TABLE_COLLATION
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                    AND TABLE_SCHEMA NOT IN {system_schemas}
            """
            params = [schema_name]

            if table_name:
                query += " AND TABLE_NAME = %s"
                params.append(table_name)

            query += f" ORDER BY {order_column} DESC"

            results = await self.sql_driver.execute_query(query, params)

            output = {
                "schema": schema_name,
                "table_count": len(results),
                "tables": []
            }

            total_data = 0
            total_index = 0
            total_rows = 0

            for row in results:
                data_length = row["DATA_LENGTH"] or 0
                index_length = row["INDEX_LENGTH"] or 0
                table_rows = row["TABLE_ROWS"] or 0
                data_free = row["DATA_FREE"] or 0

                total_data += data_length
                total_index += index_length
                total_rows += table_rows

                table_info = {
                    "name": row["TABLE_NAME"],
                    "type": row["TABLE_TYPE"],
                    "engine": row["ENGINE"],
                    "row_format": row["ROW_FORMAT"],
                    "rows": table_rows,
                    "avg_row_length": row["AVG_ROW_LENGTH"],
                    "data_size_bytes": data_length,
                    "data_size_mb": round(data_length / 1024 / 1024, 2),
                    "index_size_bytes": index_length,
                    "index_size_mb": round(index_length / 1024 / 1024, 2),
                    "total_size_mb": round((data_length + index_length) / 1024 / 1024, 2),
                    "data_free_bytes": data_free,
                    "data_free_mb": round(data_free / 1024 / 1024, 2),
                    "fragmentation_pct": round(data_free / max(data_length, 1) * 100, 2) if data_length else 0,
                    "auto_increment": row["AUTO_INCREMENT"],
                    "created": str(row["CREATE_TIME"]) if row["CREATE_TIME"] else None,
                    "updated": str(row["UPDATE_TIME"]) if row["UPDATE_TIME"] else None,
                    "collation": row["TABLE_COLLATION"]
                }

                # Get index information if requested
                if include_indexes and row["TABLE_NAME"]:
                    table_info["indexes"] = await self._get_table_indexes(
                        schema_name, row["TABLE_NAME"]
                    )

                output["tables"].append(table_info)

            # Add summary
            output["summary"] = {
                "total_data_mb": round(total_data / 1024 / 1024, 2),
                "total_index_mb": round(total_index / 1024 / 1024, 2),
                "total_size_mb": round((total_data + total_index) / 1024 / 1024, 2),
                "total_rows": total_rows
            }

            # Add analysis
            output["analysis"] = self._analyze_tables(output["tables"])

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _get_table_indexes(self, schema: str, table: str) -> list[dict]:
        """Get index information for a table."""
        # Define system schemas to exclude from analysis
        system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

        query = f"""
            SELECT
                INDEX_NAME,
                NON_UNIQUE,
                SEQ_IN_INDEX,
                COLUMN_NAME,
                CARDINALITY,
                INDEX_TYPE,
                NULLABLE
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                AND TABLE_SCHEMA NOT IN {system_schemas}
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """

        results = await self.sql_driver.execute_query(query, [schema, table])

        # Group columns by index
        indexes = {}
        for row in results:
            idx_name = row["INDEX_NAME"]
            if idx_name not in indexes:
                indexes[idx_name] = {
                    "name": idx_name,
                    "unique": not row["NON_UNIQUE"],
                    "type": row["INDEX_TYPE"],
                    "columns": [],
                    "cardinality": row["CARDINALITY"]
                }
            indexes[idx_name]["columns"].append({
                "name": row["COLUMN_NAME"],
                "seq": row["SEQ_IN_INDEX"],
                "nullable": row["NULLABLE"] == "YES"
            })

        return list(indexes.values())

    def _analyze_tables(self, tables: list[dict]) -> dict:
        """Analyze tables and generate recommendations."""
        analysis = {
            "fragmented_tables": [],
            "large_tables": [],
            "recommendations": []
        }

        for table in tables:
            name = table["name"]

            # Check fragmentation
            if table.get("fragmentation_pct", 0) > 20:
                analysis["fragmented_tables"].append({
                    "table": name,
                    "fragmentation_pct": table["fragmentation_pct"],
                    "data_free_mb": table["data_free_mb"]
                })

            # Check for large tables without recent updates
            if table.get("total_size_mb", 0) > 1000:  # 1GB+
                analysis["large_tables"].append({
                    "table": name,
                    "size_mb": table["total_size_mb"],
                    "rows": table["rows"]
                })

        # Generate recommendations
        if analysis["fragmented_tables"]:
            frag_tables = ", ".join(t["table"] for t in analysis["fragmented_tables"][:5])
            analysis["recommendations"].append(
                f"Run OPTIMIZE TABLE on fragmented tables: {frag_tables}"
            )

        if analysis["large_tables"]:
            large_tables = ", ".join(t["table"] for t in analysis["large_tables"][:5])
            analysis["recommendations"].append(
                f"Consider partitioning large tables: {large_tables}"
            )

        return analysis
