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
                    },
                    "confirm_write": {
                        "type": "boolean",
                        "description": "Required when analyze=true AND the query is a write (UPDATE/DELETE/INSERT/REPLACE). EXPLAIN ANALYZE actually executes the wrapped statement — set this flag to acknowledge.",
                        "default": False
                    }
                },
                "required": ["query"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            self.validate_required_args(arguments, ["query"])

            from ..security import assert_safe_explain_target

            query = arguments["query"].strip()
            analyze = arguments.get("analyze", False)
            format_type = arguments.get("format", "json")
            confirm_write = arguments.get("confirm_write", False)

            assert_safe_explain_target(
                query, analyze=analyze, confirm_write=confirm_write,
            )

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


class CompareExplainPlansToolHandler(ToolHandler):
    """Tool handler for diffing two query variants' EXPLAIN plans."""

    name = "compare_explain_plans"
    title = "EXPLAIN Plan Comparator"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Compare EXPLAIN plans for two query variants and pick the better one.

Inputs:
- query_a: first SQL variant
- query_b: second SQL variant
- label_a, label_b: optional human-friendly labels

Both queries pass through the sql_guard (single statement, no DDL),
then both get EXPLAIN FORMAT=JSON. The diff includes access_type
changes, key usage changes, rows_examined delta, full-scan delta.

Verdict heuristic (in order):
1. Fewer full scans wins
2. Else, side with materially fewer rows examined (>=20% delta) wins
3. Else, "no significant difference"."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query_a": {"type": "string", "description": "First SQL query"},
                    "query_b": {"type": "string", "description": "Second SQL query"},
                    "label_a": {"type": "string", "description": "Label for query A"},
                    "label_b": {"type": "string", "description": "Label for query B"},
                },
                "required": ["query_a", "query_b"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            from ..security import assert_safe_explain_target

            self.validate_required_args(arguments, ["query_a", "query_b"])
            query_a = arguments["query_a"].strip()
            query_b = arguments["query_b"].strip()
            label_a = arguments.get("label_a", "A")
            label_b = arguments.get("label_b", "B")

            assert_safe_explain_target(query_a, analyze=False, confirm_write=False)
            assert_safe_explain_target(query_b, analyze=False, confirm_write=False)

            res_a = await self.sql_driver.execute_query(f"EXPLAIN FORMAT=JSON {query_a}")
            res_b = await self.sql_driver.execute_query(f"EXPLAIN FORMAT=JSON {query_b}")

            import json as _json
            # Some MySQL drivers / mock environments return the column name
            # as "explain" instead of "EXPLAIN" — accept either to avoid a
            # KeyError on otherwise valid results.
            plan_a = (
                _json.loads(res_a[0].get("EXPLAIN") or res_a[0].get("explain") or "{}")
                if res_a else {}
            )
            plan_b = (
                _json.loads(res_b[0].get("EXPLAIN") or res_b[0].get("explain") or "{}")
                if res_b else {}
            )

            tables_a = self._extract_tables(plan_a)
            tables_b = self._extract_tables(plan_b)

            full_scans_a = sum(1 for t in tables_a if t["access_type"] == "ALL")
            full_scans_b = sum(1 for t in tables_b if t["access_type"] == "ALL")
            rows_a = sum(t["rows_examined_per_scan"] or 0 for t in tables_a)
            rows_b = sum(t["rows_examined_per_scan"] or 0 for t in tables_b)

            rationale: list[str] = []
            verdict = "no significant difference"

            if full_scans_a != full_scans_b:
                if full_scans_b < full_scans_a:
                    verdict = f"{label_b} is better"
                    rationale.append(
                        f"{label_b} eliminates {full_scans_a - full_scans_b} full scan(s)"
                    )
                else:
                    verdict = f"{label_a} is better"
                    rationale.append(
                        f"{label_a} eliminates {full_scans_b - full_scans_a} full scan(s)"
                    )
            else:
                # Require BOTH a relative delta (>=20% of the larger side) AND
                # an absolute floor — without the floor, trivial differences
                # like 1 vs 0 rows examined would be reported as "better",
                # which is noisy and misleading for tiny queries.
                rel_threshold = 0.2 * max(rows_a, rows_b, 1)
                abs_floor = 10
                delta = abs(rows_a - rows_b)
                if delta >= rel_threshold and delta >= abs_floor:
                    if rows_b < rows_a:
                        verdict = f"{label_b} is better"
                        rationale.append(
                            f"{label_b} examines {rows_a - rows_b} fewer rows"
                        )
                    else:
                        verdict = f"{label_a} is better"
                        rationale.append(
                            f"{label_a} examines {rows_b - rows_a} fewer rows"
                        )

            output = {
                "label_a": label_a,
                "label_b": label_b,
                "query_a": {"sql": query_a, "plan": plan_a, "tables": tables_a},
                "query_b": {"sql": query_b, "plan": plan_b, "tables": tables_b},
                "diff": {
                    "full_scans_a": full_scans_a,
                    "full_scans_b": full_scans_b,
                    "full_scans_change": full_scans_b - full_scans_a,
                    "rows_examined_a": rows_a,
                    "rows_examined_b": rows_b,
                    "rows_examined_delta": rows_b - rows_a,
                },
                "verdict": verdict,
                "rationale": rationale,
            }
            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _extract_tables(self, plan: dict) -> list[dict]:
        """Walk a FORMAT=JSON EXPLAIN tree; return per-table access info."""
        out: list[dict] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                if "table" in node and isinstance(node["table"], dict):
                    t = node["table"]
                    out.append({
                        "table_name": t.get("table_name"),
                        "access_type": t.get("access_type"),
                        "key": t.get("key"),
                        "rows_examined_per_scan": t.get("rows_examined_per_scan"),
                        "filtered": t.get("filtered"),
                    })
                for v in node.values():
                    visit(v)
            elif isinstance(node, list):
                for v in node:
                    visit(v)

        visit(plan)
        return out


class TableIoHotspotsToolHandler(ToolHandler):
    """Tool handler for ranking tables by file I/O latency."""

    name = "get_table_io_hotspots"
    title = "Table I/O Hotspots"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Rank tables by their file I/O latency from performance_schema.file_summary_by_instance.

Maps perf bottlenecks to specific tables (not just queries). The filename
in file_summary_by_instance is parsed to extract (schema, table) — the
basename without extension is the table; parent directory is the schema.

System schemas (mysql, information_schema, performance_schema, sys) are excluded."""

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
                        "description": "Max tables to return",
                        "default": 20, "minimum": 1, "maximum": 100,
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Filter to a specific schema (optional)",
                    },
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 20)
            schema_name = arguments.get("schema_name")

            query = """
                SELECT
                    FILE_NAME,
                    SUM_NUMBER_OF_BYTES_READ AS total_read_bytes,
                    SUM_NUMBER_OF_BYTES_WRITE AS total_write_bytes,
                    SUM_TIMER_READ AS total_read_timer,
                    SUM_TIMER_WRITE AS total_write_timer,
                    COUNT_READ AS read_count,
                    COUNT_WRITE AS write_count
                FROM performance_schema.file_summary_by_instance
                WHERE EVENT_NAME = 'wait/io/file/innodb/innodb_data_file'
                  AND SUM_NUMBER_OF_BYTES_READ + SUM_NUMBER_OF_BYTES_WRITE > 0
            """
            rows = await self.sql_driver.execute_query(query)

            system_schemas = {"mysql", "information_schema", "performance_schema", "sys"}
            tables: list[dict] = []
            for r in rows:
                schema, table = self._parse_filename(r["FILE_NAME"])
                if not schema or not table:
                    continue
                if schema in system_schemas:
                    continue
                if schema_name and schema != schema_name:
                    continue
                read_timer = r["total_read_timer"] or 0
                write_timer = r["total_write_timer"] or 0
                read_count = r["read_count"] or 0
                write_count = r["write_count"] or 0
                # performance_schema timer values are in picoseconds
                read_latency_sec = read_timer / 1e12
                write_latency_sec = write_timer / 1e12
                tables.append({
                    "schema": schema,
                    "table": table,
                    "total_read_bytes": r["total_read_bytes"],
                    "total_write_bytes": r["total_write_bytes"],
                    "total_read_latency_sec": round(read_latency_sec, 4),
                    "total_write_latency_sec": round(write_latency_sec, 4),
                    "avg_read_latency_us": round(read_timer / max(read_count, 1) / 1e6, 2),
                    "avg_write_latency_us": round(write_timer / max(write_count, 1) / 1e6, 2),
                    "_total_latency": read_latency_sec + write_latency_sec,
                })

            tables.sort(key=lambda t: t["_total_latency"], reverse=True)
            top = tables[:limit]
            for t in top:
                del t["_total_latency"]

            total_latency_all = sum(
                t["total_read_latency_sec"] + t["total_write_latency_sec"]
                for t in top
            )
            top_pct = 0.0
            if top and total_latency_all > 0:
                first_total = top[0]["total_read_latency_sec"] + top[0]["total_write_latency_sec"]
                top_pct = round(first_total / total_latency_all * 100, 2)

            recommendations: list[str] = []
            if top_pct > 50:
                recommendations.append(
                    f"Top table dominates I/O ({top_pct}% of measured latency); "
                    "consider partitioning or splitting hot rows."
                )
            for t in top:
                if t["avg_read_latency_us"] > 10000:
                    recommendations.append(
                        f"Investigate storage latency on {t['schema']}.{t['table']} "
                        f"(avg read latency {t['avg_read_latency_us']}us)."
                    )

            output = {
                "tables": top,
                "summary": {
                    "table_count": len(top),
                    "top_table_pct_of_total_io": top_pct,
                },
                "recommendations": recommendations,
            }
            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    @staticmethod
    def _parse_filename(file_name: str) -> tuple[str, str]:
        """Extract (schema, table) from a .ibd path.

        Examples:
            /var/lib/mysql/testdb/orders.ibd  ->  ("testdb", "orders")
            ./testdb/orders.ibd               ->  ("testdb", "orders")
        """
        if not file_name:
            return ("", "")
        # Normalize separators (file_summary uses '/' even on Windows)
        parts = file_name.replace("\\", "/").rstrip("/").split("/")
        if len(parts) < 2:
            return ("", "")
        basename = parts[-1]
        schema = parts[-2]
        # Strip .ibd / .ibt extension
        if "." in basename:
            basename = basename.rsplit(".", 1)[0]
        # Strip MySQL partition suffix (e.g. "orders#p#p1" or
        # "orders#P#p0#SP#sp0") so per-partition I/O aggregates back to
        # the logical table — otherwise top-N hotspots would report each
        # partition as a separate "table".
        table = basename.split("#")[0]
        return (schema, table)
