"""
SQL Driver for MySQL query execution.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Sequence

import aiomysql

from .db_pool import DbConnPool

logger = logging.getLogger("mysqltuner_mcp")


class SqlDriver:
    """
    MySQL SQL driver for executing queries.

    Provides a high-level interface for executing queries and
    managing database interactions using the connection pool.
    """

    def __init__(self, pool: DbConnPool):
        """
        Initialize the SQL driver.

        Args:
            pool: Database connection pool instance
        """
        self.pool = pool

    async def execute_query(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        fetch_all: bool = True
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.

        Args:
            query: SQL query string
            params: Optional query parameters
            fetch_all: Whether to fetch all results (default: True)

        Returns:
            List of dictionaries with column names as keys
        """
        async with self.pool.get_pool().acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await cursor.execute(query, params)

                    if fetch_all:
                        rows = await cursor.fetchall()
                        return list(rows) if rows else []
                    else:
                        row = await cursor.fetchone()
                        return [row] if row else []

                except Exception as e:
                    logger.error(f"Query execution failed: {str(e)}")
                    logger.debug(f"Query: {query}")
                    raise

    async def execute_one(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None
    ) -> Optional[dict[str, Any]]:
        """
        Execute a query and return a single result.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Single result dictionary or None
        """
        results = await self.execute_query(query, params, fetch_all=False)
        return results[0] if results else None

    async def execute_scalar(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None
    ) -> Any:
        """
        Execute a query and return the first column of the first row.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Scalar value or None
        """
        result = await self.execute_one(query, params)
        if result:
            # Return the first value from the dictionary
            return next(iter(result.values()), None)
        return None

    async def execute_many(
        self,
        query: str,
        params_list: Sequence[Sequence[Any]]
    ) -> int:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            params_list: List of parameter sequences

        Returns:
            Total number of affected rows
        """
        async with self.pool.get_pool().acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.executemany(query, params_list)
                    return cursor.rowcount
                except Exception as e:
                    logger.error(f"Batch execution failed: {str(e)}")
                    raise

    async def get_server_version(self) -> str:
        """
        Get the MySQL server version.

        Returns:
            Server version string
        """
        result = await self.execute_scalar("SELECT VERSION()")
        return str(result) if result else "unknown"

    async def get_server_variables(self, like_pattern: Optional[str] = None) -> dict[str, Any]:
        """
        Get MySQL server variables.

        Args:
            like_pattern: Optional LIKE pattern to filter variables

        Returns:
            Dictionary of variable names to values
        """
        if like_pattern:
            query = "SHOW VARIABLES LIKE %s"
            results = await self.execute_query(query, [like_pattern])
        else:
            query = "SHOW VARIABLES"
            results = await self.execute_query(query)

        return {row["Variable_name"]: row["Value"] for row in results}

    async def get_server_status(self, like_pattern: Optional[str] = None) -> dict[str, Any]:
        """
        Get MySQL server status variables.

        Args:
            like_pattern: Optional LIKE pattern to filter status

        Returns:
            Dictionary of status variable names to values
        """
        if like_pattern:
            query = "SHOW GLOBAL STATUS LIKE %s"
            results = await self.execute_query(query, [like_pattern])
        else:
            query = "SHOW GLOBAL STATUS"
            results = await self.execute_query(query)

        return {row["Variable_name"]: row["Value"] for row in results}

    async def get_databases(self) -> list[str]:
        """
        Get list of all databases.

        Returns:
            List of database names
        """
        results = await self.execute_query("SHOW DATABASES")
        return [row["Database"] for row in results]

    async def get_tables(self, database: Optional[str] = None) -> list[str]:
        """
        Get list of tables in a database.

        Args:
            database: Database name (uses current database if None)

        Returns:
            List of table names
        """
        if database:
            query = f"SHOW TABLES FROM `{database}`"
        else:
            query = "SHOW TABLES"

        results = await self.execute_query(query)
        if results:
            # The column name varies based on the database name
            key = list(results[0].keys())[0]
            return [row[key] for row in results]
        return []

    async def get_innodb_status(self) -> dict[str, Any]:
        """
        Get InnoDB engine status.

        Returns:
            Dictionary containing InnoDB status information
        """
        try:
            results = await self.execute_query("SHOW ENGINE INNODB STATUS")
            if results:
                return {
                    "type": results[0].get("Type"),
                    "name": results[0].get("Name"),
                    "status": results[0].get("Status")
                }
        except Exception as e:
            logger.error(f"Failed to get InnoDB status: {str(e)}")
        return {}

    async def get_storage_engines(self) -> list[dict[str, Any]]:
        """
        Get list of available storage engines.

        Returns:
            List of storage engine information dictionaries
        """
        query = """
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
        return await self.execute_query(query)

    async def get_processlist(
        self,
        full: bool = True,
        exclude_system: bool = True
    ) -> list[dict[str, Any]]:
        """
        Get current process list.

        Args:
            full: Include full query text
            exclude_system: Exclude system threads

        Returns:
            List of process information
        """
        query = "SHOW FULL PROCESSLIST" if full else "SHOW PROCESSLIST"
        results = await self.execute_query(query)

        if exclude_system:
            results = [
                r for r in results
                if r.get("User") not in ("system user", "event_scheduler")
            ]

        return results

    async def get_binary_logs(self) -> list[dict[str, Any]]:
        """
        Get list of binary logs.

        Returns:
            List of binary log information
        """
        try:
            return await self.execute_query("SHOW BINARY LOGS")
        except Exception as e:
            logger.debug(f"Binary logs not available: {str(e)}")
            return []

    async def get_master_status(self) -> Optional[dict[str, Any]]:
        """
        Get master/source replication status.

        Returns:
            Master status dictionary or None
        """
        try:
            # Try MySQL 8.0.22+ syntax first
            try:
                results = await self.execute_query("SHOW BINARY LOG STATUS")
            except Exception:
                results = await self.execute_query("SHOW MASTER STATUS")

            return results[0] if results else None
        except Exception as e:
            logger.debug(f"Master status not available: {str(e)}")
            return None

    async def get_slave_status(self) -> list[dict[str, Any]]:
        """
        Get slave/replica replication status.

        Returns:
            List of slave status dictionaries (for multi-source replication)
        """
        try:
            # Try MySQL 8.0.22+ syntax first
            try:
                return await self.execute_query("SHOW REPLICA STATUS")
            except Exception:
                return await self.execute_query("SHOW SLAVE STATUS")
        except Exception as e:
            logger.debug(f"Slave status not available: {str(e)}")
            return []

    async def get_table_stats(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Get table statistics from information_schema.

        Args:
            schema: Optional schema filter
            table: Optional table filter

        Returns:
            List of table statistics
        """
        query = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                ENGINE,
                TABLE_ROWS,
                DATA_LENGTH,
                INDEX_LENGTH,
                DATA_FREE,
                AUTO_INCREMENT,
                CREATE_TIME,
                UPDATE_TIME
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """

        conditions = []
        params = []

        if schema:
            conditions.append("TABLE_SCHEMA = %s")
            params.append(schema)
        else:
            conditions.append(
                "TABLE_SCHEMA NOT IN "
                "('mysql', 'information_schema', 'performance_schema', 'sys')"
            )

        if table:
            conditions.append("TABLE_NAME = %s")
            params.append(table)

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += " ORDER BY DATA_LENGTH DESC"

        return await self.execute_query(query, params if params else None)

    async def get_index_stats(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Get index statistics.

        Args:
            schema: Optional schema filter
            table: Optional table filter

        Returns:
            List of index statistics
        """
        query = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                INDEX_NAME,
                NON_UNIQUE,
                SEQ_IN_INDEX,
                COLUMN_NAME,
                CARDINALITY,
                SUB_PART,
                NULLABLE,
                INDEX_TYPE
            FROM information_schema.STATISTICS
            WHERE 1=1
        """

        params = []
        if schema:
            query += " AND TABLE_SCHEMA = %s"
            params.append(schema)
        else:
            query += (
                " AND TABLE_SCHEMA NOT IN "
                "('mysql', 'information_schema', 'performance_schema', 'sys')"
            )

        if table:
            query += " AND TABLE_NAME = %s"
            params.append(table)

        query += " ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"

        return await self.execute_query(query, params if params else None)

    async def check_performance_schema(self) -> bool:
        """
        Check if performance_schema is enabled.

        Returns:
            True if performance_schema is enabled
        """
        try:
            result = await self.execute_scalar("SELECT @@performance_schema")
            return result == 1 or result == "1"
        except Exception:
            return False

    async def check_sys_schema(self) -> bool:
        """
        Check if sys schema is available.

        Returns:
            True if sys schema exists
        """
        try:
            result = await self.execute_scalar(
                "SELECT COUNT(*) FROM information_schema.SCHEMATA "
                "WHERE SCHEMA_NAME = 'sys'"
            )
            return result and result > 0
        except Exception:
            return False
