"""
Storage engine analysis tool handlers for MySQL.

Includes tools for analyzing storage engines:
- Engine statistics and status
- MyISAM-specific checks
- InnoDB vs MyISAM comparison
- Engine recommendation

Based on MySQLTuner's storage engine analysis patterns.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class StorageEngineAnalysisToolHandler(ToolHandler):
    """Tool handler for storage engine analysis."""

    name = "analyze_storage_engines"
    title = "Storage Engine Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze storage engine usage and statistics for user tables.

Provides:
- List of available engines and their status
- Table count and size by engine
- Engine-specific metrics (InnoDB, MyISAM, MEMORY, etc.)
- Recommendations for engine optimization

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys) by default.

Based on MySQLTuner's engine analysis patterns."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_table_details": {
                        "type": "boolean",
                        "description": "Include per-table engine details",
                        "default": True
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by specific schema (optional)"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_details = arguments.get("include_table_details", True)
            schema_name = arguments.get("schema_name")

            output = {
                "available_engines": [],
                "engine_usage": {},
                "engine_summary": {},
                "myisam_analysis": {},
                "innodb_analysis": {},
                "issues": [],
                "recommendations": []
            }

            # Get available storage engines
            engines_query = """
                SELECT
                    ENGINE,
                    SUPPORT,
                    COMMENT,
                    TRANSACTIONS,
                    XA,
                    SAVEPOINTS
                FROM information_schema.ENGINES
                ORDER BY ENGINE
            """
            engines = await self.sql_driver.execute_query(engines_query)

            for engine in engines:
                output["available_engines"].append({
                    "engine": engine.get("ENGINE"),
                    "support": engine.get("SUPPORT"),
                    "transactions": engine.get("TRANSACTIONS"),
                    "xa": engine.get("XA"),
                    "savepoints": engine.get("SAVEPOINTS"),
                    "comment": engine.get("COMMENT")
                })

            # Get table count and size by engine
            where_clause = "WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')"
            if schema_name:
                where_clause = f"WHERE TABLE_SCHEMA = '{schema_name}'"

            engine_stats_query = f"""
                SELECT
                    ENGINE,
                    COUNT(*) as table_count,
                    SUM(TABLE_ROWS) as total_rows,
                    SUM(DATA_LENGTH) as data_size,
                    SUM(INDEX_LENGTH) as index_size,
                    SUM(DATA_LENGTH + INDEX_LENGTH) as total_size,
                    SUM(DATA_FREE) as data_free
                FROM information_schema.TABLES
                {where_clause}
                    AND TABLE_TYPE = 'BASE TABLE'
                    AND ENGINE IS NOT NULL
                GROUP BY ENGINE
                ORDER BY total_size DESC
            """
            engine_stats = await self.sql_driver.execute_query(engine_stats_query)

            total_tables = 0
            total_data = 0
            total_index = 0

            for stat in engine_stats:
                engine_name = stat.get("ENGINE")
                table_count = stat.get("table_count") or 0
                data_size = stat.get("data_size") or 0
                index_size = stat.get("index_size") or 0
                total_size = stat.get("total_size") or 0
                data_free = stat.get("data_free") or 0

                output["engine_usage"][engine_name] = {
                    "table_count": table_count,
                    "total_rows": stat.get("total_rows") or 0,
                    "data_size_bytes": data_size,
                    "data_size_mb": round(data_size / 1024 / 1024, 2),
                    "index_size_bytes": index_size,
                    "index_size_mb": round(index_size / 1024 / 1024, 2),
                    "total_size_mb": round(total_size / 1024 / 1024, 2),
                    "data_free_mb": round(data_free / 1024 / 1024, 2)
                }

                total_tables += table_count
                total_data += data_size
                total_index += index_size

            output["engine_summary"] = {
                "total_tables": total_tables,
                "total_data_size_mb": round(total_data / 1024 / 1024, 2),
                "total_index_size_mb": round(total_index / 1024 / 1024, 2),
                "total_size_mb": round((total_data + total_index) / 1024 / 1024, 2),
                "total_size_gb": round(
                    (total_data + total_index) / 1024 / 1024 / 1024, 2
                )
            }

            # Analyze MyISAM if present
            if "MyISAM" in output["engine_usage"]:
                await self._analyze_myisam(output)

            # Analyze InnoDB
            if "InnoDB" in output["engine_usage"]:
                await self._analyze_innodb(output)

            # Analyze MEMORY tables
            if "MEMORY" in output["engine_usage"]:
                await self._analyze_memory_engine(output)

            # Table details by engine
            if include_details:
                await self._get_table_details(output, schema_name)

            # Generate recommendations
            self._generate_recommendations(output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _analyze_myisam(self, output: dict) -> None:
        """Analyze MyISAM-specific metrics."""
        variables = await self.sql_driver.get_server_variables()
        status = await self.sql_driver.get_server_status()

        key_buffer_size = int(variables.get("key_buffer_size", 0))
        key_read_requests = int(status.get("Key_read_requests", 0))
        key_reads = int(status.get("Key_reads", 0))
        key_write_requests = int(status.get("Key_write_requests", 0))
        key_writes = int(status.get("Key_writes", 0))
        key_blocks_used = int(status.get("Key_blocks_used", 0))
        key_blocks_unused = int(status.get("Key_blocks_unused", 0))
        key_block_size = int(variables.get("key_cache_block_size", 1024))

        output["myisam_analysis"] = {
            "key_buffer_size_mb": round(key_buffer_size / 1024 / 1024, 2),
            "key_read_requests": key_read_requests,
            "key_reads": key_reads,
            "key_write_requests": key_write_requests,
            "key_writes": key_writes,
            "key_blocks_used": key_blocks_used,
            "key_blocks_unused": key_blocks_unused
        }

        # Key cache hit ratio
        if key_read_requests > 0:
            key_cache_hit = (
                (key_read_requests - key_reads) / key_read_requests * 100
            )
            output["myisam_analysis"]["key_cache_hit_ratio"] = round(
                key_cache_hit, 4
            )

            if key_cache_hit < 95:
                output["issues"].append(
                    f"MyISAM key cache hit ratio is low ({key_cache_hit:.2f}%)"
                )
                output["recommendations"].append(
                    "Consider increasing key_buffer_size for MyISAM tables"
                )

        # Key buffer usage
        if key_buffer_size > 0:
            key_used = key_blocks_used * key_block_size
            usage_pct = (key_used / key_buffer_size) * 100
            output["myisam_analysis"]["key_buffer_usage_pct"] = round(usage_pct, 2)

            if usage_pct < 10 and key_buffer_size > 64 * 1024 * 1024:
                output["recommendations"].append(
                    f"Key buffer usage is only {usage_pct:.1f}%. "
                    "Consider reducing key_buffer_size."
                )

        # MyISAM tables should generally be migrated to InnoDB
        myisam_tables = output["engine_usage"]["MyISAM"]["table_count"]
        if myisam_tables > 0:
            output["issues"].append(
                f"{myisam_tables} MyISAM tables found"
            )
            output["recommendations"].append(
                "Consider migrating MyISAM tables to InnoDB for better "
                "crash recovery, transactions, and row-level locking"
            )

    async def _analyze_innodb(self, output: dict) -> None:
        """Analyze InnoDB-specific metrics."""
        variables = await self.sql_driver.get_server_variables("innodb%")
        status = await self.sql_driver.get_server_status("Innodb%")

        bp_size = int(variables.get("innodb_buffer_pool_size", 0))
        bp_instances = int(variables.get("innodb_buffer_pool_instances", 1))

        read_requests = int(status.get("Innodb_buffer_pool_read_requests", 0))
        reads = int(status.get("Innodb_buffer_pool_reads", 0))

        output["innodb_analysis"] = {
            "buffer_pool_size_gb": round(bp_size / 1024 / 1024 / 1024, 2),
            "buffer_pool_instances": bp_instances,
            "file_per_table": variables.get("innodb_file_per_table"),
            "flush_method": variables.get("innodb_flush_method"),
            "flush_log_at_trx_commit": variables.get(
                "innodb_flush_log_at_trx_commit"
            ),
            "doublewrite": variables.get("innodb_doublewrite"),
            "read_io_threads": variables.get("innodb_read_io_threads"),
            "write_io_threads": variables.get("innodb_write_io_threads")
        }

        # Buffer pool hit ratio
        if read_requests > 0:
            hit_ratio = (read_requests - reads) / read_requests * 100
            output["innodb_analysis"]["buffer_pool_hit_ratio"] = round(hit_ratio, 4)

        # Compare buffer pool size to data size
        innodb_usage = output["engine_usage"].get("InnoDB", {})
        innodb_data = innodb_usage.get("data_size_bytes", 0)
        innodb_index = innodb_usage.get("index_size_bytes", 0)
        total_innodb = innodb_data + innodb_index

        if total_innodb > 0:
            bp_coverage = (bp_size / total_innodb) * 100
            output["innodb_analysis"]["buffer_pool_data_coverage_pct"] = round(
                bp_coverage, 2
            )

            if bp_coverage < 100:
                output["recommendations"].append(
                    f"InnoDB buffer pool ({round(bp_size/1024/1024/1024, 2)}GB) "
                    f"covers only {bp_coverage:.1f}% of InnoDB data "
                    f"({round(total_innodb/1024/1024/1024, 2)}GB). "
                    "Consider increasing innodb_buffer_pool_size."
                )

    async def _analyze_memory_engine(self, output: dict) -> None:
        """Analyze MEMORY engine tables."""
        variables = await self.sql_driver.get_server_variables()

        max_heap_table_size = int(variables.get("max_heap_table_size", 0))
        tmp_table_size = int(variables.get("tmp_table_size", 0))

        output["memory_engine"] = {
            "max_heap_table_size_mb": round(max_heap_table_size / 1024 / 1024, 2),
            "tmp_table_size_mb": round(tmp_table_size / 1024 / 1024, 2)
        }

        memory_usage = output["engine_usage"].get("MEMORY", {})
        if memory_usage.get("table_count", 0) > 0:
            output["recommendations"].append(
                "MEMORY tables lose data on restart. Consider using InnoDB "
                "with appropriate caching if persistence is needed."
            )

    async def _get_table_details(self, output: dict, schema_name: str = None) -> None:
        """Get detailed table information by engine."""

        where_clause = "WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')"
        if schema_name:
            where_clause = f"WHERE TABLE_SCHEMA = '{schema_name}'"

        # Get non-InnoDB tables (potential migration candidates)
        non_innodb_query = f"""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                ENGINE,
                TABLE_ROWS,
                DATA_LENGTH,
                INDEX_LENGTH,
                DATA_FREE
            FROM information_schema.TABLES
            {where_clause}
                AND ENGINE != 'InnoDB'
                AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY DATA_LENGTH DESC
            LIMIT 20
        """
        non_innodb = await self.sql_driver.execute_query(non_innodb_query)

        output["non_innodb_tables"] = [
            {
                "schema": row.get("TABLE_SCHEMA"),
                "table": row.get("TABLE_NAME"),
                "engine": row.get("ENGINE"),
                "rows": row.get("TABLE_ROWS"),
                "data_size_mb": round(
                    (row.get("DATA_LENGTH") or 0) / 1024 / 1024, 2
                ),
                "index_size_mb": round(
                    (row.get("INDEX_LENGTH") or 0) / 1024 / 1024, 2
                )
            }
            for row in non_innodb
        ]

        # Get fragmented tables (DATA_FREE > 10% of DATA_LENGTH)
        fragmented_query = f"""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                ENGINE,
                TABLE_ROWS,
                DATA_LENGTH,
                DATA_FREE,
                ROUND(DATA_FREE / DATA_LENGTH * 100, 2) as fragmentation_pct
            FROM information_schema.TABLES
            {where_clause}
                AND DATA_LENGTH > 0
                AND DATA_FREE > DATA_LENGTH * 0.1
                AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY DATA_FREE DESC
            LIMIT 20
        """
        fragmented = await self.sql_driver.execute_query(fragmented_query)

        output["fragmented_tables"] = [
            {
                "schema": row.get("TABLE_SCHEMA"),
                "table": row.get("TABLE_NAME"),
                "engine": row.get("ENGINE"),
                "rows": row.get("TABLE_ROWS"),
                "data_size_mb": round(
                    (row.get("DATA_LENGTH") or 0) / 1024 / 1024, 2
                ),
                "data_free_mb": round(
                    (row.get("DATA_FREE") or 0) / 1024 / 1024, 2
                ),
                "fragmentation_pct": row.get("fragmentation_pct")
            }
            for row in fragmented
        ]

        if output["fragmented_tables"]:
            output["recommendations"].append(
                f"{len(output['fragmented_tables'])} fragmented tables found. "
                "Consider running OPTIMIZE TABLE to reclaim space."
            )

    def _generate_recommendations(self, output: dict) -> None:
        """Generate engine-related recommendations."""

        # Check for engine diversity
        engine_count = len([
            e for e in output["engine_usage"]
            if output["engine_usage"][e]["table_count"] > 0
        ])

        if engine_count > 2:
            output["issues"].append(
                f"Using {engine_count} different storage engines"
            )
            output["recommendations"].append(
                "Consider consolidating to fewer storage engines (preferably InnoDB) "
                "for simpler management and better resource utilization"
            )

        # Check default storage engine
        try:
            # Note: This would need to be async, adding as a recommendation
            output["recommendations"].append(
                "Ensure default_storage_engine is set to InnoDB for new tables"
            )
        except Exception:
            pass


class FragmentedTablesToolHandler(ToolHandler):
    """Tool handler for fragmented tables analysis."""

    name = "get_fragmented_tables"
    title = "Fragmented Tables"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Find user tables with significant fragmentation.

Fragmentation occurs when:
- Data is deleted from tables
- Tables are frequently updated
- VARCHAR/TEXT columns are modified

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys) by default.

High fragmentation wastes disk space and can slow queries."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "min_fragmentation_pct": {
                        "type": "number",
                        "description": "Minimum fragmentation percentage threshold",
                        "default": 10
                    },
                    "min_data_free_mb": {
                        "type": "number",
                        "description": "Minimum wasted space in MB",
                        "default": 10
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by specific schema"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum tables to return",
                        "default": 50
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            min_frag_pct = arguments.get("min_fragmentation_pct", 10)
            min_data_free = arguments.get("min_data_free_mb", 10) * 1024 * 1024
            schema_name = arguments.get("schema_name")
            limit = arguments.get("limit", 50)

            output = {
                "summary": {},
                "fragmented_tables": [],
                "recommendations": []
            }

            where_clause = """
                WHERE TABLE_SCHEMA NOT IN
                    ('mysql', 'information_schema', 'performance_schema', 'sys')
            """
            if schema_name:
                where_clause = f"WHERE TABLE_SCHEMA = '{schema_name}'"

            query = f"""
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    ENGINE,
                    TABLE_ROWS,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    DATA_FREE,
                    ROUND(DATA_FREE / DATA_LENGTH * 100, 2) as fragmentation_pct
                FROM information_schema.TABLES
                {where_clause}
                    AND TABLE_TYPE = 'BASE TABLE'
                    AND DATA_LENGTH > 0
                    AND DATA_FREE >= {min_data_free}
                    AND (DATA_FREE / DATA_LENGTH * 100) >= {min_frag_pct}
                ORDER BY DATA_FREE DESC
                LIMIT {limit}
            """
            results = await self.sql_driver.execute_query(query)

            total_wasted = 0
            for row in results:
                data_free = row.get("DATA_FREE") or 0
                total_wasted += data_free

                output["fragmented_tables"].append({
                    "schema": row.get("TABLE_SCHEMA"),
                    "table": row.get("TABLE_NAME"),
                    "engine": row.get("ENGINE"),
                    "rows": row.get("TABLE_ROWS"),
                    "data_size_mb": round(
                        (row.get("DATA_LENGTH") or 0) / 1024 / 1024, 2
                    ),
                    "index_size_mb": round(
                        (row.get("INDEX_LENGTH") or 0) / 1024 / 1024, 2
                    ),
                    "data_free_mb": round(data_free / 1024 / 1024, 2),
                    "fragmentation_pct": row.get("fragmentation_pct"),
                    "optimize_command": (
                        f"OPTIMIZE TABLE `{row.get('TABLE_SCHEMA')}`."
                        f"`{row.get('TABLE_NAME')}`"
                    )
                })

            output["summary"] = {
                "fragmented_tables_count": len(results),
                "total_wasted_space_mb": round(total_wasted / 1024 / 1024, 2),
                "total_wasted_space_gb": round(
                    total_wasted / 1024 / 1024 / 1024, 2
                )
            }

            if results:
                output["recommendations"].append(
                    f"Found {len(results)} fragmented tables wasting "
                    f"{output['summary']['total_wasted_space_mb']:.1f} MB"
                )
                output["recommendations"].append(
                    "Run OPTIMIZE TABLE on fragmented tables during low-traffic "
                    "periods. Note: This locks the table for MyISAM."
                )

                # InnoDB specific recommendation
                innodb_tables = [
                    t for t in output["fragmented_tables"]
                    if t["engine"] == "InnoDB"
                ]
                if innodb_tables:
                    output["recommendations"].append(
                        "For InnoDB tables, consider ALTER TABLE ... ENGINE=InnoDB "
                        "as an alternative to OPTIMIZE TABLE for online rebuild."
                    )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class AutoIncrementAnalysisToolHandler(ToolHandler):
    """Tool handler for auto-increment column analysis."""

    name = "analyze_auto_increment"
    title = "Auto-Increment Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze auto-increment columns for potential overflow.

Checks:
- Current value vs maximum value for column type
- Usage percentage
- Tables approaching overflow

Note: This tool only analyzes user/custom tables and excludes MySQL system
tables (mysql, information_schema, performance_schema, sys) by default.

Based on MySQLTuner's auto-increment analysis."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "warning_threshold_pct": {
                        "type": "number",
                        "description": "Warning threshold percentage",
                        "default": 75
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Filter by specific schema"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            warning_pct = arguments.get("warning_threshold_pct", 75)
            schema_name = arguments.get("schema_name")

            output = {
                "summary": {},
                "at_risk_tables": [],
                "all_auto_increment": [],
                "recommendations": []
            }

            # Max values for different integer types
            max_values = {
                "tinyint": {"signed": 127, "unsigned": 255},
                "smallint": {"signed": 32767, "unsigned": 65535},
                "mediumint": {"signed": 8388607, "unsigned": 16777215},
                "int": {"signed": 2147483647, "unsigned": 4294967295},
                "bigint": {"signed": 9223372036854775807, "unsigned": 18446744073709551615}
            }

            where_clause = """
                WHERE TABLE_SCHEMA NOT IN
                    ('mysql', 'information_schema', 'performance_schema', 'sys')
            """
            if schema_name:
                where_clause = f"WHERE TABLE_SCHEMA = '{schema_name}'"

            # Get auto_increment columns
            query = f"""
                SELECT
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    t.AUTO_INCREMENT,
                    c.COLUMN_NAME,
                    c.COLUMN_TYPE,
                    c.DATA_TYPE
                FROM information_schema.TABLES t
                JOIN information_schema.COLUMNS c
                    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND t.TABLE_NAME = c.TABLE_NAME
                {where_clause}
                    AND t.AUTO_INCREMENT IS NOT NULL
                    AND c.EXTRA LIKE '%auto_increment%'
                ORDER BY t.AUTO_INCREMENT DESC
            """
            results = await self.sql_driver.execute_query(query)

            at_risk_count = 0

            for row in results:
                current_val = row.get("AUTO_INCREMENT") or 0
                data_type = (row.get("DATA_TYPE") or "int").lower()
                column_type = (row.get("COLUMN_TYPE") or "").lower()

                # Determine if unsigned
                is_unsigned = "unsigned" in column_type
                sign_type = "unsigned" if is_unsigned else "signed"

                # Get max value for this type
                type_limits = max_values.get(data_type, max_values["int"])
                max_val = type_limits[sign_type]

                # Calculate usage percentage
                usage_pct = (current_val / max_val) * 100

                table_info = {
                    "schema": row.get("TABLE_SCHEMA"),
                    "table": row.get("TABLE_NAME"),
                    "column": row.get("COLUMN_NAME"),
                    "column_type": row.get("COLUMN_TYPE"),
                    "current_value": current_val,
                    "max_value": max_val,
                    "usage_pct": round(usage_pct, 4)
                }

                output["all_auto_increment"].append(table_info)

                if usage_pct >= warning_pct:
                    at_risk_count += 1
                    table_info["at_risk"] = True
                    output["at_risk_tables"].append(table_info)

            output["summary"] = {
                "total_auto_increment_tables": len(results),
                "at_risk_tables_count": at_risk_count
            }

            # Generate recommendations for at-risk tables
            for table in output["at_risk_tables"]:
                data_type = table["column_type"].lower()

                if "bigint" in data_type:
                    output["recommendations"].append(
                        f"Table `{table['schema']}`.`{table['table']}` is at "
                        f"{table['usage_pct']:.2f}% of BIGINT capacity. "
                        "Consider data archival strategy."
                    )
                elif "unsigned" not in data_type:
                    output["recommendations"].append(
                        f"Table `{table['schema']}`.`{table['table']}` "
                        f"({table['column_type']}) is at {table['usage_pct']:.2f}% "
                        "capacity. Consider ALTER to UNSIGNED for 2x capacity or "
                        "upgrade to larger integer type."
                    )
                else:
                    next_type = {
                        "tinyint": "smallint",
                        "smallint": "mediumint",
                        "mediumint": "int",
                        "int": "bigint"
                    }
                    base_type = data_type.split()[0].replace("unsigned", "").strip()
                    suggested = next_type.get(base_type, "bigint")

                    output["recommendations"].append(
                        f"Table `{table['schema']}`.`{table['table']}` "
                        f"({table['column_type']}) is at {table['usage_pct']:.2f}% "
                        f"capacity. Consider upgrading to {suggested.upper()} UNSIGNED."
                    )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
