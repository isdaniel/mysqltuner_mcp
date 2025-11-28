"""
Index tuning tool handlers for MySQL.

Includes tools for:
- Index recommendations based on query patterns
- Unused index identification
- Duplicate index detection
- Index statistics analysis
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class IndexRecommendationsToolHandler(ToolHandler):
    """Tool handler for generating index recommendations."""

    name = "get_index_recommendations"
    title = "Index Advisor"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get AI-powered index recommendations for MySQL user tables.

Analyzes query patterns from performance_schema to recommend indexes:
- Identifies queries with full table scans
- Finds queries not using indexes efficiently
- Suggests composite indexes for multi-column filters
- Prioritizes recommendations by potential impact

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys).

Based on MySQL performance_schema statistics and query patterns.
Similar to MySQLTuner's index analysis but with more detailed recommendations."""

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
                        "description": "Schema/database to analyze"
                    },
                    "max_recommendations": {
                        "type": "integer",
                        "description": "Maximum number of recommendations (default: 10)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50
                    },
                    "min_improvement_percent": {
                        "type": "number",
                        "description": "Minimum expected improvement percentage (default: 10)",
                        "default": 10
                    },
                    "include_query_analysis": {
                        "type": "boolean",
                        "description": "Include analysis of specific queries",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name")
            max_recs = arguments.get("max_recommendations", 10)
            min_improvement = arguments.get("min_improvement_percent", 10)
            include_analysis = arguments.get("include_query_analysis", True)

            # Get current database if not specified
            if not schema_name:
                schema_name = await self.sql_driver.execute_scalar("SELECT DATABASE()")

            output = {
                "schema": schema_name,
                "recommendations": [],
                "analysis_summary": {}
            }

            # 1. Find queries with no index used
            no_index_query = """
                SELECT
                    DIGEST_TEXT as query,
                    SCHEMA_NAME,
                    COUNT_STAR as exec_count,
                    SUM_NO_INDEX_USED as no_index_count,
                    SUM_ROWS_EXAMINED as rows_examined,
                    SUM_ROWS_SENT as rows_sent,
                    ROUND(SUM_TIMER_WAIT / 1000000000000, 4) as total_time_sec
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SCHEMA_NAME = %s
                    AND SUM_NO_INDEX_USED > 0
                    AND DIGEST_TEXT IS NOT NULL
                    AND DIGEST_TEXT NOT LIKE 'SHOW%%'
                    AND DIGEST_TEXT NOT LIKE 'SET%%'
                ORDER BY SUM_NO_INDEX_USED * SUM_TIMER_WAIT DESC
                LIMIT 20
            """

            no_index_results = await self.sql_driver.execute_query(no_index_query, [schema_name])

            # 2. Find queries with poor index usage (examined >> sent)
            poor_index_query = """
                SELECT
                    DIGEST_TEXT as query,
                    SCHEMA_NAME,
                    COUNT_STAR as exec_count,
                    SUM_ROWS_EXAMINED as rows_examined,
                    SUM_ROWS_SENT as rows_sent,
                    ROUND(SUM_TIMER_WAIT / 1000000000000, 4) as total_time_sec
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SCHEMA_NAME = %s
                    AND SUM_ROWS_EXAMINED > SUM_ROWS_SENT * 10
                    AND SUM_ROWS_EXAMINED > 1000
                    AND DIGEST_TEXT IS NOT NULL
                    AND DIGEST_TEXT LIKE 'SELECT%%'
                ORDER BY (SUM_ROWS_EXAMINED - SUM_ROWS_SENT) * COUNT_STAR DESC
                LIMIT 20
            """

            poor_index_results = await self.sql_driver.execute_query(poor_index_query, [schema_name])

            # 3. Analyze and generate recommendations
            recommendations = []

            for row in no_index_results:
                query_text = row["query"]
                if not query_text:
                    continue

                # Extract table name from query (simplified)
                table_name = self._extract_table_name(query_text)
                if not table_name:
                    continue

                # Extract columns from WHERE clause
                columns = self._extract_where_columns(query_text)

                if columns:
                    impact_score = (
                        row["no_index_count"] * row["exec_count"] *
                        float(row["total_time_sec"] or 0)
                    )

                    rec = {
                        "table": table_name,
                        "columns": columns,
                        "reason": "Query performs full table scans",
                        "impact_score": round(impact_score, 2),
                        "affected_query_count": row["exec_count"],
                        "total_rows_examined": row["rows_examined"],
                        "create_statement": f"CREATE INDEX idx_{table_name}_{'_'.join(columns[:3])} ON {table_name} ({', '.join(columns)})"
                    }

                    if include_analysis:
                        rec["sample_query"] = query_text[:200]

                    recommendations.append(rec)

            # Process poor index usage queries
            for row in poor_index_results:
                query_text = row["query"]
                if not query_text:
                    continue

                table_name = self._extract_table_name(query_text)
                if not table_name:
                    continue

                columns = self._extract_where_columns(query_text)

                if columns:
                    efficiency = row["rows_sent"] / max(row["rows_examined"], 1) * 100

                    # Skip if already recommended
                    if any(r["table"] == table_name and set(r["columns"]) == set(columns)
                           for r in recommendations):
                        continue

                    rec = {
                        "table": table_name,
                        "columns": columns,
                        "reason": f"Poor index efficiency ({efficiency:.1f}% rows returned vs examined)",
                        "impact_score": round(row["rows_examined"] * row["exec_count"] / 1000000, 2),
                        "affected_query_count": row["exec_count"],
                        "total_rows_examined": row["rows_examined"],
                        "create_statement": f"CREATE INDEX idx_{table_name}_{'_'.join(columns[:3])} ON {table_name} ({', '.join(columns)})"
                    }

                    if include_analysis:
                        rec["sample_query"] = query_text[:200]

                    recommendations.append(rec)

            # Sort by impact and limit
            recommendations.sort(key=lambda x: x["impact_score"], reverse=True)
            output["recommendations"] = recommendations[:max_recs]

            # Summary statistics
            output["analysis_summary"] = {
                "queries_without_index": len(no_index_results),
                "queries_with_poor_index": len(poor_index_results),
                "total_recommendations": len(output["recommendations"])
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _extract_table_name(self, query: str) -> str | None:
        """Extract the primary table name from a query."""
        import re

        # Clean up the query
        query = query.upper()

        # Try FROM clause
        match = re.search(r'FROM\s+[`"]?(\w+)[`"]?', query, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        # Try UPDATE
        match = re.search(r'UPDATE\s+[`"]?(\w+)[`"]?', query, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        # Try DELETE FROM
        match = re.search(r'DELETE\s+FROM\s+[`"]?(\w+)[`"]?', query, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        return None

    def _extract_where_columns(self, query: str) -> list[str]:
        """Extract column names from WHERE clause."""
        import re

        columns = []

        # Find WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return columns

        where_clause = where_match.group(1)

        # Extract column names (simplified - looks for word = or word IN or word LIKE patterns)
        col_patterns = [
            r'[`"]?(\w+)[`"]?\s*[=<>!]+',  # column = value
            r'[`"]?(\w+)[`"]?\s+IN\s*\(',   # column IN (...)
            r'[`"]?(\w+)[`"]?\s+LIKE',      # column LIKE
            r'[`"]?(\w+)[`"]?\s+BETWEEN',   # column BETWEEN
            r'[`"]?(\w+)[`"]?\s+IS\s+',     # column IS NULL
        ]

        for pattern in col_patterns:
            matches = re.findall(pattern, where_clause, re.IGNORECASE)
            for match in matches:
                col = match.lower()
                # Filter out common non-column words
                if col not in ('and', 'or', 'not', 'null', 'true', 'false', 'select', 'from'):
                    if col not in columns:
                        columns.append(col)

        return columns[:5]  # Limit to 5 columns


class UnusedIndexesToolHandler(ToolHandler):
    """Tool handler for finding unused indexes."""

    name = "find_unused_indexes"
    title = "Unused Index Finder"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Find unused and duplicate indexes in MySQL user tables.

Identifies:
- Indexes with zero or very few reads since server start
- Duplicate indexes (same columns in same order)
- Redundant indexes (one index is a prefix of another)

Removing unused indexes can:
- Reduce storage space
- Speed up INSERT/UPDATE/DELETE operations
- Reduce memory usage for index buffers

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys).

Based on information_schema and performance_schema statistics."""

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
                        "description": "Schema/database to analyze"
                    },
                    "include_duplicates": {
                        "type": "boolean",
                        "description": "Include analysis of duplicate/redundant indexes",
                        "default": True
                    },
                    "min_size_mb": {
                        "type": "number",
                        "description": "Minimum index size in MB to include",
                        "default": 0
                    },
                    "exclude_primary": {
                        "type": "boolean",
                        "description": "Exclude primary keys from analysis",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name")
            include_duplicates = arguments.get("include_duplicates", True)
            min_size_mb = arguments.get("min_size_mb", 0)
            exclude_primary = arguments.get("exclude_primary", True)

            # Get current database if not specified
            if not schema_name:
                schema_name = await self.sql_driver.execute_scalar("SELECT DATABASE()")

            output = {
                "schema": schema_name,
                "unused_indexes": [],
                "duplicate_indexes": [],
                "redundant_indexes": [],
                "recommendations": [],
                "summary": {}
            }

            # 1. Find unused indexes from performance_schema
            # Note: Requires performance_schema.table_io_waits_summary_by_index_usage
            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            unused_query = f"""
                SELECT
                    s.TABLE_SCHEMA,
                    s.TABLE_NAME,
                    s.INDEX_NAME,
                    s.NON_UNIQUE,
                    GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX) as columns,
                    t.INDEX_LENGTH as index_size_bytes,
                    COALESCE(ps.COUNT_READ, 0) as read_count,
                    COALESCE(ps.COUNT_WRITE, 0) as write_count
                FROM information_schema.STATISTICS s
                JOIN information_schema.TABLES t
                    ON s.TABLE_SCHEMA = t.TABLE_SCHEMA AND s.TABLE_NAME = t.TABLE_NAME
                LEFT JOIN performance_schema.table_io_waits_summary_by_index_usage ps
                    ON s.TABLE_SCHEMA = ps.OBJECT_SCHEMA
                    AND s.TABLE_NAME = ps.OBJECT_NAME
                    AND s.INDEX_NAME = ps.INDEX_NAME
                WHERE s.TABLE_SCHEMA = %s
                    AND s.TABLE_SCHEMA NOT IN {system_schemas}
            """

            if exclude_primary:
                unused_query += " AND s.INDEX_NAME != 'PRIMARY'"

            unused_query += """
                GROUP BY s.TABLE_SCHEMA, s.TABLE_NAME, s.INDEX_NAME, s.NON_UNIQUE, t.INDEX_LENGTH, ps.COUNT_READ, ps.COUNT_WRITE
                HAVING read_count = 0 OR read_count IS NULL
                ORDER BY index_size_bytes DESC
            """

            unused_results = await self.sql_driver.execute_query(unused_query, [schema_name])

            total_unused_size = 0
            for row in unused_results:
                size_mb = (row["index_size_bytes"] or 0) / 1024 / 1024

                if size_mb < min_size_mb:
                    continue

                total_unused_size += row["index_size_bytes"] or 0

                idx_info = {
                    "table": row["TABLE_NAME"],
                    "index_name": row["INDEX_NAME"],
                    "columns": row["columns"],
                    "unique": not row["NON_UNIQUE"],
                    "size_mb": round(size_mb, 2),
                    "read_count": row["read_count"],
                    "write_count": row["write_count"],
                    "drop_statement": f"DROP INDEX `{row['INDEX_NAME']}` ON `{row['TABLE_NAME']}`"
                }
                output["unused_indexes"].append(idx_info)

            # 2. Find duplicate indexes
            if include_duplicates:
                await self._find_duplicate_indexes(schema_name, output, exclude_primary)
                await self._find_redundant_indexes(schema_name, output, exclude_primary)

            # Generate recommendations
            for idx in output["unused_indexes"]:
                output["recommendations"].append(
                    f"DROP unused index: {idx['drop_statement']} -- saves {idx['size_mb']} MB"
                )

            for dup in output["duplicate_indexes"]:
                output["recommendations"].append(
                    f"DROP duplicate index: DROP INDEX `{dup['duplicate_index']}` ON `{dup['table']}`"
                )

            for red in output["redundant_indexes"]:
                output["recommendations"].append(
                    f"Consider dropping redundant index: DROP INDEX `{red['redundant_index']}` ON `{red['table']}` (covered by {red['covering_index']})"
                )

            # Summary
            output["summary"] = {
                "unused_index_count": len(output["unused_indexes"]),
                "duplicate_index_count": len(output["duplicate_indexes"]),
                "redundant_index_count": len(output["redundant_indexes"]),
                "total_unused_size_mb": round(total_unused_size / 1024 / 1024, 2),
                "total_recommendations": len(output["recommendations"])
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _find_duplicate_indexes(self, schema: str, output: dict, exclude_primary: bool) -> None:
        """Find duplicate indexes (exact same columns)."""
        # Define system schemas to exclude from analysis
        system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

        query = f"""
            SELECT
                TABLE_NAME,
                GROUP_CONCAT(INDEX_NAME) as index_names,
                COLUMN_NAME as columns,
                COUNT(DISTINCT INDEX_NAME) as index_count
            FROM (
                SELECT
                    TABLE_NAME,
                    INDEX_NAME,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMN_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s
                    AND TABLE_SCHEMA NOT IN {system_schemas}
        """

        if exclude_primary:
            query += " AND INDEX_NAME != 'PRIMARY'"

        query += """
                GROUP BY TABLE_NAME, INDEX_NAME
            ) sub
            GROUP BY TABLE_NAME, COLUMN_NAME
            HAVING index_count > 1
        """

        results = await self.sql_driver.execute_query(query, [schema])

        for row in results:
            indexes = row["index_names"].split(",")
            output["duplicate_indexes"].append({
                "table": row["TABLE_NAME"],
                "columns": row["columns"],
                "duplicate_indexes": indexes,
                "keep_index": indexes[0],
                "duplicate_index": indexes[1] if len(indexes) > 1 else None
            })

    async def _find_redundant_indexes(self, schema: str, output: dict, exclude_primary: bool) -> None:
        """Find redundant indexes (one is prefix of another)."""
        # Define system schemas to exclude from analysis
        system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

        query = f"""
            SELECT
                s1.TABLE_NAME,
                s1.INDEX_NAME as shorter_index,
                s1.columns as shorter_columns,
                s2.INDEX_NAME as longer_index,
                s2.columns as longer_columns
            FROM (
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    INDEX_NAME,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
                    COUNT(*) as col_count
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s
                    AND TABLE_SCHEMA NOT IN {system_schemas}
        """

        if exclude_primary:
            query += " AND INDEX_NAME != 'PRIMARY'"

        query += f"""
                GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
            ) s1
            JOIN (
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    INDEX_NAME,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
                    COUNT(*) as col_count
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s
                    AND TABLE_SCHEMA NOT IN {system_schemas}
        """

        if exclude_primary:
            query += " AND INDEX_NAME != 'PRIMARY'"

        query += """
                GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
            ) s2
            ON s1.TABLE_SCHEMA = s2.TABLE_SCHEMA
                AND s1.TABLE_NAME = s2.TABLE_NAME
                AND s1.INDEX_NAME != s2.INDEX_NAME
                AND s1.col_count < s2.col_count
                AND s2.columns LIKE CONCAT(s1.columns, '%%')
        """

        results = await self.sql_driver.execute_query(query, [schema, schema])

        for row in results:
            output["redundant_indexes"].append({
                "table": row["TABLE_NAME"],
                "redundant_index": row["shorter_index"],
                "redundant_columns": row["shorter_columns"],
                "covering_index": row["longer_index"],
                "covering_columns": row["longer_columns"]
            })


class IndexStatsToolHandler(ToolHandler):
    """Tool handler for analyzing index statistics."""

    name = "get_index_stats"
    title = "Index Statistics Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get detailed index statistics for MySQL user tables.

Returns:
- Index cardinality and selectivity
- Index size and memory usage
- Read/write operation counts
- Index efficiency metrics

Helps identify:
- Low cardinality indexes
- Oversized indexes
- Infrequently used indexes

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
                        "description": "Schema/database to analyze"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Specific table to analyze (optional)"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Order results by",
                        "enum": ["size", "reads", "cardinality", "name"],
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
            order_by = arguments.get("order_by", "size")

            # Get current database if not specified
            if not schema_name:
                schema_name = await self.sql_driver.execute_scalar("SELECT DATABASE()")

            # Define system schemas to exclude from analysis
            system_schemas = "('mysql', 'information_schema', 'performance_schema', 'sys')"

            # Build query for index statistics
            query = f"""
                SELECT
                    s.TABLE_NAME,
                    s.INDEX_NAME,
                    s.NON_UNIQUE,
                    s.INDEX_TYPE,
                    GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX) as columns,
                    MAX(s.CARDINALITY) as cardinality,
                    t.TABLE_ROWS,
                    COALESCE(ps.COUNT_READ, 0) as read_count,
                    COALESCE(ps.COUNT_WRITE, 0) as write_count,
                    COALESCE(ps.SUM_TIMER_READ / 1000000000, 0) as read_time_ms,
                    COALESCE(ps.SUM_TIMER_WRITE / 1000000000, 0) as write_time_ms
                FROM information_schema.STATISTICS s
                JOIN information_schema.TABLES t
                    ON s.TABLE_SCHEMA = t.TABLE_SCHEMA AND s.TABLE_NAME = t.TABLE_NAME
                LEFT JOIN performance_schema.table_io_waits_summary_by_index_usage ps
                    ON s.TABLE_SCHEMA = ps.OBJECT_SCHEMA
                    AND s.TABLE_NAME = ps.OBJECT_NAME
                    AND s.INDEX_NAME = ps.INDEX_NAME
                WHERE s.TABLE_SCHEMA = %s
                    AND s.TABLE_SCHEMA NOT IN {system_schemas}
            """

            params = [schema_name]

            if table_name:
                query += " AND s.TABLE_NAME = %s"
                params.append(table_name)

            query += """
                GROUP BY s.TABLE_NAME, s.INDEX_NAME, s.NON_UNIQUE, s.INDEX_TYPE,
                         t.TABLE_ROWS, ps.COUNT_READ, ps.COUNT_WRITE, ps.SUM_TIMER_READ, ps.SUM_TIMER_WRITE
            """

            # Add ordering
            order_map = {
                "size": "cardinality DESC",
                "reads": "read_count DESC",
                "cardinality": "cardinality DESC",
                "name": "s.INDEX_NAME"
            }
            query += f" ORDER BY {order_map.get(order_by, 'cardinality DESC')}"

            results = await self.sql_driver.execute_query(query, params)

            output = {
                "schema": schema_name,
                "index_count": len(results),
                "indexes": []
            }

            for row in results:
                table_rows = row["TABLE_ROWS"] or 0
                cardinality = row["cardinality"] or 0

                # Calculate selectivity
                selectivity = round(cardinality / max(table_rows, 1) * 100, 2) if table_rows else 0

                idx_info = {
                    "table": row["TABLE_NAME"],
                    "index_name": row["INDEX_NAME"],
                    "columns": row["columns"],
                    "unique": not row["NON_UNIQUE"],
                    "type": row["INDEX_TYPE"],
                    "cardinality": cardinality,
                    "table_rows": table_rows,
                    "selectivity_pct": selectivity,
                    "read_count": row["read_count"],
                    "write_count": row["write_count"],
                    "read_time_ms": round(row["read_time_ms"], 2),
                    "write_time_ms": round(row["write_time_ms"], 2)
                }

                # Add analysis
                if selectivity < 1 and not row["NON_UNIQUE"]:
                    idx_info["warning"] = "Very low selectivity - may not be effective"
                elif selectivity < 10:
                    idx_info["note"] = "Low selectivity index"

                output["indexes"].append(idx_info)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
