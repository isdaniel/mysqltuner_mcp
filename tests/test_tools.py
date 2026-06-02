"""
Unit tests for all tool handlers.
"""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock

from mysqltuner_mcp.tools import (
    # Base handler
    ToolHandler,
    # Original tools (performance, index, health)
    GetSlowQueriesToolHandler,
    AnalyzeQueryToolHandler,
    TableStatsToolHandler,
    IndexRecommendationsToolHandler,
    UnusedIndexesToolHandler,
    IndexStatsToolHandler,
    DatabaseHealthToolHandler,
    ActiveQueriesToolHandler,
    SettingsReviewToolHandler,
    WaitEventsToolHandler,
    # InnoDB tools
    InnoDBStatusToolHandler,
    InnoDBBufferPoolToolHandler,
    InnoDBTransactionsToolHandler,
    # Statement tools
    StatementAnalysisToolHandler,
    StatementsTempTablesToolHandler,
    StatementsSortingToolHandler,
    StatementsFullScansToolHandler,
    StatementErrorsToolHandler,
    # Memory tools
    MemoryCalculationsToolHandler,
    MemoryByHostToolHandler,
    TableMemoryUsageToolHandler,
    # Engine tools
    StorageEngineAnalysisToolHandler,
    FragmentedTablesToolHandler,
    AutoIncrementAnalysisToolHandler,
    # Replication tools
    ReplicationStatusToolHandler,
    GaleraClusterToolHandler,
    GroupReplicationToolHandler,
    # Security tools
    SecurityAnalysisToolHandler,
    UserPrivilegesToolHandler,
    AuditLogToolHandler,
    # Diagnostic tools
    ConnectionAnalysisToolHandler,
    TableLockAnalysisToolHandler,
    TempTableAnalysisToolHandler,
    PerfSchemaConfigToolHandler,
    OptimizerConfigToolHandler,
    # Schema & binlog tools
    SchemaProfilingToolHandler,
    BinlogAnalysisToolHandler,
    GlobalStatusSnapshotToolHandler,
)


def create_mock_sql_driver():
    """Create a mock SQL driver for testing."""
    driver = MagicMock()
    driver.execute_query = AsyncMock(return_value=[])
    driver.execute_one = AsyncMock(return_value={})
    driver.execute_scalar = AsyncMock(return_value=None)
    driver.get_server_status = AsyncMock(return_value={})
    driver.get_server_variables = AsyncMock(return_value={})
    return driver


# =============================================================================
# Base Tool Handler Tests
# =============================================================================


class TestToolHandlerBase:
    """Tests for the base ToolHandler class."""

    def test_format_json_result(self):
        """Test JSON result formatting."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        result = handler.format_json_result({"key": "value", "number": 42})

        assert len(result) == 1
        assert result[0].type == "text"

        # Parse the JSON to verify it's valid
        parsed = json.loads(result[0].text)
        assert parsed["key"] == "value"
        assert parsed["number"] == 42

    def test_format_error(self):
        """Test error formatting."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        result = handler.format_error(ValueError("Test error"))

        assert len(result) == 1
        assert result[0].type == "text"
        assert "error" in result[0].text.lower()
        assert "Test error" in result[0].text

    def test_validate_required_args_success(self):
        """Test required args validation success."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        # Should not raise
        handler.validate_required_args(
            {"arg1": "value1", "arg2": "value2"},
            ["arg1", "arg2"]
        )

    def test_validate_required_args_failure(self):
        """Test required args validation failure."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        with pytest.raises(ValueError, match="Missing required"):
            handler.validate_required_args(
                {"arg1": "value1"},
                ["arg1", "arg2", "arg3"]
            )

    def test_get_annotations(self):
        """Test annotation generation."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        annotations = handler.get_annotations()

        assert annotations["title"] == handler.title
        assert annotations["readOnlyHint"] == handler.read_only_hint
        assert annotations["destructiveHint"] == handler.destructive_hint


# =============================================================================
# Performance Tool Tests (Original)
# =============================================================================


class TestGetSlowQueriesToolHandler:
    """Tests for GetSlowQueriesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = GetSlowQueriesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_slow_queries"
        assert "slow" in definition.description.lower()
        assert "inputSchema" in dir(definition) or hasattr(definition, "inputSchema")

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running the tool."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "query_text": "SELECT * FROM users WHERE id = ?",
                "schema_name": "testdb",
                "exec_count": 100,
                "total_time_sec": 5.5,
                "avg_time_sec": 0.055,
                "max_time_sec": 0.2,
                "rows_examined": 10000,
                "rows_sent": 100,
                "rows_affected": 0,
                "full_scans": 0,
                "no_good_index": 0,
                "tmp_tables": 0,
                "tmp_disk_tables": 0,
                "full_joins": 0,
                "sort_rows": 0,
                "first_seen": "2024-01-01",
                "last_seen": "2024-06-01"
            }
        ])

        handler = GetSlowQueriesToolHandler(driver)

        result = await handler.run_tool({"limit": 10})

        assert len(result) == 1
        assert result[0].type == "text"

        parsed = json.loads(result[0].text)
        assert "queries" in parsed
        assert len(parsed["queries"]) == 1


class TestAnalyzeQueryToolHandler:
    """Tests for AnalyzeQueryToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = AnalyzeQueryToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_query"
        assert "query" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_explain(self):
        """Test running EXPLAIN."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "id": 1,
                "select_type": "SIMPLE",
                "table": "users",
                "type": "ref",
                "possible_keys": "idx_email",
                "key": "idx_email",
                "key_len": "767",
                "ref": "const",
                "rows": 1,
                "Extra": "Using index"
            }
        ])

        handler = AnalyzeQueryToolHandler(driver)

        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE email = 'test@test.com'",
            "format": "traditional"
        })

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "plan" in parsed

    @pytest.mark.asyncio
    async def test_run_tool_missing_query(self):
        """Test error when query is missing."""
        driver = create_mock_sql_driver()
        handler = AnalyzeQueryToolHandler(driver)

        result = await handler.run_tool({})

        assert len(result) == 1
        assert "error" in result[0].text.lower()


class TestTableStatsToolHandler:
    """Tests for TableStatsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = TableStatsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_table_stats"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running table stats."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="testdb")
        driver.execute_query = AsyncMock(return_value=[
            {
                "TABLE_NAME": "users",
                "TABLE_TYPE": "BASE TABLE",
                "ENGINE": "InnoDB",
                "ROW_FORMAT": "Dynamic",
                "TABLE_ROWS": 1000,
                "AVG_ROW_LENGTH": 100,
                "DATA_LENGTH": 10485760,
                "INDEX_LENGTH": 2621440,
                "DATA_FREE": 0,
                "AUTO_INCREMENT": 1001,
                "CREATE_TIME": "2024-01-01 00:00:00",
                "UPDATE_TIME": "2024-06-01 00:00:00",
                "TABLE_COLLATION": "utf8mb4_general_ci"
            }
        ])

        handler = TableStatsToolHandler(driver)

        # Set include_indexes=False to avoid secondary query for index data
        result = await handler.run_tool({"include_indexes": False})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "tables" in parsed


# =============================================================================
# Index Tool Tests (Original)
# =============================================================================


class TestIndexRecommendationsToolHandler:
    """Tests for IndexRecommendationsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = IndexRecommendationsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_index_recommendations"

    def test_extract_table_name(self):
        """Test table name extraction."""
        driver = create_mock_sql_driver()
        handler = IndexRecommendationsToolHandler(driver)

        assert handler._extract_table_name("SELECT * FROM users") == "users"
        assert handler._extract_table_name("UPDATE orders SET status = 1") == "orders"
        assert handler._extract_table_name("DELETE FROM logs WHERE id > 100") == "logs"

    def test_extract_where_columns(self):
        """Test WHERE column extraction."""
        driver = create_mock_sql_driver()
        handler = IndexRecommendationsToolHandler(driver)

        columns = handler._extract_where_columns(
            "SELECT * FROM users WHERE email = 'test' AND status = 1"
        )
        assert "email" in columns
        assert "status" in columns

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running index recommendations."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="testdb")
        driver.execute_query = AsyncMock(return_value=[])

        handler = IndexRecommendationsToolHandler(driver)

        result = await handler.run_tool({})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "recommendations" in parsed


class TestUnusedIndexesToolHandler:
    """Tests for UnusedIndexesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = UnusedIndexesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "find_unused_indexes"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running unused index finder."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="testdb")
        driver.execute_query = AsyncMock(return_value=[])

        handler = UnusedIndexesToolHandler(driver)

        result = await handler.run_tool({})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "unused_indexes" in parsed
        assert "summary" in parsed


class TestIndexStatsToolHandler:
    """Tests for IndexStatsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = IndexStatsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_index_stats"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running index stats."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="testdb")
        driver.execute_query = AsyncMock(return_value=[
            {
                "TABLE_NAME": "users",
                "INDEX_NAME": "PRIMARY",
                "NON_UNIQUE": 0,
                "INDEX_TYPE": "BTREE",
                "columns": "id",
                "cardinality": 1000,
                "TABLE_ROWS": 1000,
                "read_count": 5000,
                "write_count": 100,
                "read_time_ms": 50.0,
                "write_time_ms": 10.0
            }
        ])

        handler = IndexStatsToolHandler(driver)

        result = await handler.run_tool({})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "indexes" in parsed


# =============================================================================
# Health Tool Tests (Original)
# =============================================================================


class TestDatabaseHealthToolHandler:
    """Tests for DatabaseHealthToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = DatabaseHealthToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "check_database_health"

    @pytest.mark.asyncio
    async def test_run_tool_healthy(self):
        """Test health check on healthy database."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Threads_connected": "10",
            "Threads_running": "2",
            "Innodb_buffer_pool_reads": "100",
            "Innodb_buffer_pool_read_requests": "100000",
            "Questions": "1000000",
            "Slow_queries": "10",
            "Handler_read_rnd_next": "1000",
            "Handler_read_rnd": "100",
            "Com_select": "50000",
            "Created_tmp_tables": "1000",
            "Created_tmp_disk_tables": "10",
            "Threads_created": "50",
            "Connections": "1000",
            "Uptime": "86400",
            "Key_reads": "100",
            "Key_read_requests": "10000"
        })
        driver.get_server_variables = AsyncMock(return_value={
            "max_connections": "151",
            "innodb_buffer_pool_size": "134217728"
        })

        handler = DatabaseHealthToolHandler(driver)

        result = await handler.run_tool({})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "health_score" in parsed
        assert "status" in parsed
        assert "checks" in parsed

    @pytest.mark.asyncio
    async def test_run_tool_with_issues(self):
        """Test health check that detects issues."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Threads_connected": "140",  # High connection usage
            "Threads_running": "50",
            "Innodb_buffer_pool_reads": "50000",  # Low hit ratio
            "Innodb_buffer_pool_read_requests": "100000",
            "Questions": "1000",
            "Slow_queries": "100",  # High slow query %
            "Handler_read_rnd_next": "1000000",
            "Handler_read_rnd": "100",
            "Com_select": "100",
            "Created_tmp_tables": "100",
            "Created_tmp_disk_tables": "50",  # High disk temp usage
            "Threads_created": "900",  # Low thread cache hit
            "Connections": "1000",
            "Uptime": "86400",
            "Key_reads": "100",
            "Key_read_requests": "10000"
        })
        driver.get_server_variables = AsyncMock(return_value={
            "max_connections": "151",
            "innodb_buffer_pool_size": "134217728"
        })

        handler = DatabaseHealthToolHandler(driver)

        result = await handler.run_tool({"include_recommendations": True})

        parsed = json.loads(result[0].text)
        assert parsed["health_score"] < 100
        assert len(parsed["issues"]) > 0
        assert len(parsed["recommendations"]) > 0


class TestActiveQueriesToolHandler:
    """Tests for ActiveQueriesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = ActiveQueriesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_active_queries"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test active query monitoring."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "process_id": 1,
                "user": "root",
                "host": "localhost",
                "database_name": "testdb",
                "command": "Query",
                "duration_sec": 5,
                "state": "executing",
                "query": "SELECT * FROM large_table"
            }
        ])

        handler = ActiveQueriesToolHandler(driver)

        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert "queries" in parsed
        assert "summary" in parsed


class TestSettingsReviewToolHandler:
    """Tests for SettingsReviewToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = SettingsReviewToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "review_settings"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test settings review."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "134217728",
            "max_connections": "151",
            "thread_cache_size": "8",
            "slow_query_log": "ON",
            "long_query_time": "2"
        })

        handler = SettingsReviewToolHandler(driver)

        result = await handler.run_tool({"category": "all"})

        parsed = json.loads(result[0].text)
        assert "settings" in parsed
        assert "recommendations" in parsed


class TestWaitEventsToolHandler:
    """Tests for WaitEventsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = WaitEventsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_wait_events"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test wait events analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "EVENT_NAME": "wait/io/file/innodb/innodb_data_file",
                "total_count": 1000,
                "total_wait_sec": 5.5,
                "avg_wait_ms": 5.5,
                "max_wait_ms": 100.0
            }
        ])

        handler = WaitEventsToolHandler(driver)

        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert "wait_events" in parsed
        assert "summary" in parsed


# =============================================================================
# InnoDB Tool Tests
# =============================================================================


class TestInnoDBStatusToolHandler:
    """Tests for InnoDBStatusToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = InnoDBStatusToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_innodb_status"
        assert "innodb" in definition.description.lower()
        assert "include_raw_output" in definition.inputSchema["properties"]
        assert "detailed_analysis" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_basic(self):
        """Test running the InnoDB status tool."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {"Status": "INNODB STATUS OUTPUT\nBuffer pool size   16384\n"}
        ])
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "134217728",
            "innodb_buffer_pool_instances": "1",
            "innodb_log_file_size": "50331648",
            "innodb_log_files_in_group": "2",
            "innodb_log_buffer_size": "16777216",
            "innodb_file_per_table": "ON",
            "innodb_flush_log_at_trx_commit": "1"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_buffer_pool_pages_total": "8192",
            "Innodb_buffer_pool_pages_free": "1000",
            "Innodb_buffer_pool_pages_data": "7000",
            "Innodb_buffer_pool_pages_dirty": "100",
            "Innodb_buffer_pool_read_requests": "100000",
            "Innodb_buffer_pool_reads": "100",
            "Innodb_log_waits": "0",
            "Innodb_log_writes": "1000",
            "Innodb_rows_read": "50000",
            "Innodb_rows_inserted": "1000",
            "Innodb_data_read": "104857600",
            "Innodb_data_written": "52428800"
        })

        handler = InnoDBStatusToolHandler(driver)
        result = await handler.run_tool({"detailed_analysis": False})

        assert len(result) == 1
        parsed = json.loads(result[0].text)
        assert "buffer_pool" in parsed
        assert "log_info" in parsed
        assert "recommendations" in parsed

    @pytest.mark.asyncio
    async def test_run_tool_with_low_hit_ratio(self):
        """Test InnoDB status detects low buffer pool hit ratio."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[{"Status": ""}])
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "134217728",
            "innodb_buffer_pool_instances": "1",
            "innodb_file_per_table": "ON",
            "innodb_flush_log_at_trx_commit": "1"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_buffer_pool_pages_total": "8192",
            "Innodb_buffer_pool_pages_free": "100",
            "Innodb_buffer_pool_read_requests": "1000",
            "Innodb_buffer_pool_reads": "500",  # 50% miss rate = 50% hit ratio
        })

        handler = InnoDBStatusToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        # Should detect low hit ratio
        assert any("hit ratio" in issue.lower() or "buffer pool" in issue.lower()
                   for issue in parsed.get("issues", []) + parsed.get("recommendations", []))

    @pytest.mark.asyncio
    async def test_parse_deadlock_info(self):
        """Test parsing deadlock information from status."""
        driver = create_mock_sql_driver()
        status_with_deadlock = """
------------------------
LATEST DETECTED DEADLOCK
------------------------
2024-01-15 10:30:00
*** (1) TRANSACTION:
TRANSACTION 12345, ACTIVE 5 sec
*** (2) TRANSACTION:
TRANSACTION 12346, ACTIVE 3 sec
------------------------
TRANSACTIONS
"""
        driver.execute_query = AsyncMock(return_value=[{"Status": status_with_deadlock}])
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "134217728",
            "innodb_file_per_table": "ON",
            "innodb_flush_log_at_trx_commit": "1"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_buffer_pool_pages_total": "8192",
            "Innodb_buffer_pool_pages_free": "1000",
            "Innodb_buffer_pool_read_requests": "100000",
            "Innodb_buffer_pool_reads": "100"
        })

        handler = InnoDBStatusToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["deadlock_info"]["has_deadlock"] is True


class TestInnoDBBufferPoolToolHandler:
    """Tests for InnoDBBufferPoolToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = InnoDBBufferPoolToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_buffer_pool"
        assert "by_schema" in definition.inputSchema["properties"]
        assert "by_table" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test running buffer pool analysis."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "1073741824"  # 1GB
        })
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_buffer_pool_pages_total": "65536",
            "Innodb_buffer_pool_pages_free": "1000",
            "Innodb_buffer_pool_pages_data": "60000",
            "Innodb_buffer_pool_pages_dirty": "500",
            "Innodb_buffer_pool_pages_misc": "4036",
            "Innodb_buffer_pool_read_requests": "1000000",
            "Innodb_buffer_pool_reads": "100"
        })

        handler = InnoDBBufferPoolToolHandler(driver)
        result = await handler.run_tool({"by_schema": False, "by_table": False})

        parsed = json.loads(result[0].text)
        assert "buffer_pool_summary" in parsed
        assert parsed["buffer_pool_summary"]["size_gb"] == 1.0
        assert parsed["buffer_pool_summary"]["hit_ratio_pct"] > 99


class TestInnoDBTransactionsToolHandler:
    """Tests for InnoDBTransactionsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = InnoDBTransactionsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_innodb_transactions"
        assert "include_queries" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_with_transactions(self):
        """Test transaction analysis with active transactions."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="REPEATABLE-READ")
        driver.execute_query = AsyncMock(side_effect=[
            # First call: active transactions
            [
                {
                    "trx_id": "12345",
                    "trx_state": "RUNNING",
                    "trx_started": "2024-01-15 10:00:00",
                    "duration_sec": 120,
                    "trx_requested_lock_id": None,
                    "trx_wait_started": None,
                    "trx_weight": 10,
                    "trx_mysql_thread_id": 100,
                    "trx_query": "SELECT * FROM large_table",
                    "trx_operation_state": "fetching rows",
                    "trx_tables_in_use": 1,
                    "trx_tables_locked": 0,
                    "trx_lock_structs": 0,
                    "trx_rows_locked": 0,
                    "trx_rows_modified": 0
                }
            ],
            # Second call: lock waits
            []
        ])
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_history_list_length": "100"
        })

        handler = InnoDBTransactionsToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["transaction_summary"]["total_active"] == 1
        assert parsed["transaction_summary"]["isolation_level"] == "REPEATABLE-READ"
        assert len(parsed["active_transactions"]) == 1
        assert parsed["active_transactions"][0]["is_long_running"] is True


# =============================================================================
# Statement Tool Tests
# =============================================================================


class TestStatementAnalysisToolHandler:
    """Tests for StatementAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StatementAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_statements"
        assert "order_by" in definition.inputSchema["properties"]
        assert "limit" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_with_performance_schema(self):
        """Test statement analysis with performance_schema."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="1")  # performance_schema enabled
        driver.execute_query = AsyncMock(return_value=[
            {
                "query": "SELECT * FROM users WHERE id = ?",
                "db": "testdb",
                "full_scan": "",
                "exec_count": 1000,
                "total_latency": "5.00 s",
                "avg_latency": "5.00 ms",
                "rows_sent": 1000,
                "rows_sent_avg": 1,
                "rows_examined": 50000,
                "rows_examined_avg": 50
            }
        ])

        handler = StatementAnalysisToolHandler(driver)
        result = await handler.run_tool({"limit": 10})

        parsed = json.loads(result[0].text)
        assert "statements" in parsed
        assert len(parsed["statements"]) == 1
        assert parsed["statements"][0]["exec_count"] == 1000

    @pytest.mark.asyncio
    async def test_run_tool_disabled_performance_schema(self):
        """Test behavior when performance_schema is disabled."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(return_value="0")

        handler = StatementAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert "error" in parsed
        assert "performance_schema" in parsed["error"]


class TestStatementsTempTablesToolHandler:
    """Tests for StatementsTempTablesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StatementsTempTablesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_statements_with_temp_tables"
        assert "disk_only" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test statements with temp tables analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "query": "SELECT * FROM users GROUP BY name",
                "db": "testdb",
                "exec_count": 100,
                "total_latency": "10.00 s",
                "memory_tmp_tables": 100,
                "disk_tmp_tables": 50,
                "avg_tmp_tables_per_query": 1.5
            }
        ])

        handler = StatementsTempTablesToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert "statements" in parsed
        assert parsed["summary"]["total_disk_tmp_tables"] == 50
        assert len(parsed["recommendations"]) > 0


class TestStatementsSortingToolHandler:
    """Tests for StatementsSortingToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StatementsSortingToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_statements_with_sorting"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test statements with sorting analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "query": "SELECT * FROM users ORDER BY name",
                "db": "testdb",
                "exec_count": 500,
                "total_latency": "30.00 s",
                "sort_merge_passes": 100,
                "avg_sort_merges": 0.2,
                "sorts_using_scans": 400,
                "sort_using_range": 100,
                "rows_sorted": 50000,
                "avg_rows_sorted": 100
            }
        ])

        handler = StatementsSortingToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["summary"]["total_sort_merge_passes"] == 100


class TestStatementsFullScansToolHandler:
    """Tests for StatementsFullScansToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StatementsFullScansToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_statements_with_full_scans"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test full table scan analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "query": "SELECT * FROM users WHERE status = 1",
                "db": "testdb",
                "exec_count": 1000,
                "total_latency": "60.00 s",
                "no_index_used_count": 1000,
                "no_good_index_used_count": 0,
                "no_index_used_pct": 100.0,
                "rows_sent": 100,
                "rows_examined": 100000,
                "rows_sent_avg": 0.1,
                "rows_examined_avg": 100
            }
        ])

        handler = StatementsFullScansToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert len(parsed["statements"]) == 1
        assert parsed["statements"][0]["scan_efficiency_ratio"] == 1000.0


class TestStatementErrorsToolHandler:
    """Tests for StatementErrorsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StatementErrorsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_statements_with_errors"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test statement errors analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "query": "INSERT INTO users (id, name) VALUES (?, ?)",
                "db": "testdb",
                "exec_count": 1000,
                "total_latency": "5.00 s",
                "errors": 50,
                "error_pct": 5.0,
                "warnings": 100,
                "warning_pct": 10.0
            }
        ])

        handler = StatementErrorsToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["summary"]["total_errors"] == 50
        assert parsed["summary"]["total_warnings"] == 100


# =============================================================================
# Memory Tool Tests
# =============================================================================


class TestMemoryCalculationsToolHandler:
    """Tests for MemoryCalculationsToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = MemoryCalculationsToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "calculate_memory_usage"
        assert "physical_memory_gb" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test memory calculations."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "key_buffer_size": "134217728",  # 128MB
            "innodb_buffer_pool_size": "1073741824",  # 1GB
            "innodb_log_buffer_size": "16777216",  # 16MB
            "query_cache_size": "0",
            "read_buffer_size": "262144",  # 256KB
            "read_rnd_buffer_size": "524288",  # 512KB
            "sort_buffer_size": "524288",  # 512KB
            "join_buffer_size": "262144",  # 256KB
            "thread_stack": "262144",
            "binlog_stmt_cache_size": "32768",
            "net_buffer_length": "16384",
            "tmp_table_size": "16777216",
            "max_heap_table_size": "16777216",
            "max_connections": "151"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Threads_connected": "10",
            "Max_used_connections": "50"
        })

        handler = MemoryCalculationsToolHandler(driver)
        result = await handler.run_tool({"physical_memory_gb": 16, "detailed": False})

        parsed = json.loads(result[0].text)
        assert "server_buffers" in parsed
        assert "per_thread_buffers" in parsed
        assert "memory_summary" in parsed
        assert parsed["memory_summary"]["max_connections"] == 151

    @pytest.mark.asyncio
    async def test_run_tool_memory_exceeds_physical(self):
        """Test detection when MySQL memory exceeds physical memory."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "key_buffer_size": "0",
            "innodb_buffer_pool_size": "17179869184",  # 16GB
            "innodb_log_buffer_size": "16777216",
            "query_cache_size": "0",
            "read_buffer_size": "262144",
            "read_rnd_buffer_size": "524288",
            "sort_buffer_size": "524288",
            "join_buffer_size": "262144",
            "thread_stack": "262144",
            "binlog_stmt_cache_size": "32768",
            "net_buffer_length": "16384",
            "tmp_table_size": "16777216",
            "max_heap_table_size": "16777216",
            "max_connections": "500"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Threads_connected": "10",
            "Max_used_connections": "50"
        })

        handler = MemoryCalculationsToolHandler(driver)
        result = await handler.run_tool({"physical_memory_gb": 8, "detailed": False})

        parsed = json.loads(result[0].text)
        # Should detect memory issue
        assert len(parsed["issues"]) > 0


class TestMemoryByHostToolHandler:
    """Tests for MemoryByHostToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = MemoryByHostToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_memory_by_host"
        assert "group_by" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test memory by host analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=[
            [
                {
                    "host": "localhost",
                    "current_count_used": 1000,
                    "current_bytes": 104857600,
                    "current_allocated": "100.00 MB",
                    "current_avg_alloc": "104.86 KB",
                    "current_max_alloc": "16.00 MB",
                    "total_allocated": "500.00 MB"
                }
            ],
            104857600  # Total query
        ])
        driver.execute_scalar = AsyncMock(return_value=104857600)

        handler = MemoryByHostToolHandler(driver)
        result = await handler.run_tool({"group_by": "host"})

        parsed = json.loads(result[0].text)
        assert "memory_usage" in parsed


class TestTableMemoryUsageToolHandler:
    """Tests for TableMemoryUsageToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = TableMemoryUsageToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_table_memory_usage"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test table memory usage analysis."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "table_open_cache": "4000",
            "table_open_cache_instances": "16",
            "table_definition_cache": "2000"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Open_tables": "500",
            "Opened_tables": "1000",
            "Open_table_definitions": "400",
            "Opened_table_definitions": "500"
        })
        driver.execute_query = AsyncMock(return_value=[])

        handler = TableMemoryUsageToolHandler(driver)
        result = await handler.run_tool({"include_buffer_pool": False})

        parsed = json.loads(result[0].text)
        assert "table_cache" in parsed
        assert parsed["table_cache"]["table_open_cache"] == 4000


# =============================================================================
# Engine Tool Tests
# =============================================================================


class TestStorageEngineAnalysisToolHandler:
    """Tests for StorageEngineAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = StorageEngineAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_storage_engines"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test storage engine analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=[
            # Available engines
            [
                {
                    "ENGINE": "InnoDB",
                    "SUPPORT": "DEFAULT",
                    "COMMENT": "Supports transactions",
                    "TRANSACTIONS": "YES",
                    "XA": "YES",
                    "SAVEPOINTS": "YES"
                },
                {
                    "ENGINE": "MyISAM",
                    "SUPPORT": "YES",
                    "COMMENT": "MyISAM storage engine",
                    "TRANSACTIONS": "NO",
                    "XA": "NO",
                    "SAVEPOINTS": "NO"
                }
            ],
            # Engine stats
            [
                {
                    "ENGINE": "InnoDB",
                    "table_count": 50,
                    "total_rows": 1000000,
                    "data_size": 1073741824,
                    "index_size": 268435456,
                    "total_size": 1342177280,
                    "data_free": 10485760
                },
                {
                    "ENGINE": "MyISAM",
                    "table_count": 5,
                    "total_rows": 10000,
                    "data_size": 10485760,
                    "index_size": 1048576,
                    "total_size": 11534336,
                    "data_free": 0
                }
            ],
            # Non-InnoDB tables
            [],
            # Fragmented tables
            []
        ])
        driver.get_server_variables = AsyncMock(return_value={
            "innodb_buffer_pool_size": "1073741824",
            "innodb_buffer_pool_instances": "1",
            "innodb_file_per_table": "ON",
            "innodb_flush_method": "O_DIRECT",
            "key_buffer_size": "16777216"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Innodb_buffer_pool_read_requests": "1000000",
            "Innodb_buffer_pool_reads": "100",
            "Key_read_requests": "10000",
            "Key_reads": "10",
            "Key_blocks_used": "100",
            "Key_blocks_unused": "1000"
        })

        handler = StorageEngineAnalysisToolHandler(driver)
        result = await handler.run_tool({"include_table_details": False})

        parsed = json.loads(result[0].text)
        assert "available_engines" in parsed
        assert "engine_usage" in parsed
        assert "InnoDB" in parsed["engine_usage"]


class TestFragmentedTablesToolHandler:
    """Tests for FragmentedTablesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = FragmentedTablesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_fragmented_tables"
        assert "min_fragmentation_pct" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test fragmented tables analysis."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "TABLE_SCHEMA": "testdb",
                "TABLE_NAME": "orders",
                "ENGINE": "InnoDB",
                "TABLE_ROWS": 100000,
                "DATA_LENGTH": 104857600,
                "INDEX_LENGTH": 26214400,
                "DATA_FREE": 52428800,
                "fragmentation_pct": 50.0
            }
        ])

        handler = FragmentedTablesToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert len(parsed["fragmented_tables"]) == 1
        assert parsed["summary"]["total_wasted_space_mb"] == 50.0


class TestAutoIncrementAnalysisToolHandler:
    """Tests for AutoIncrementAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = AutoIncrementAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_auto_increment"
        assert "warning_threshold_pct" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_with_at_risk_table(self):
        """Test auto-increment analysis with at-risk table."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(return_value=[
            {
                "TABLE_SCHEMA": "testdb",
                "TABLE_NAME": "users",
                "AUTO_INCREMENT": 2000000000,  # ~93% of signed int
                "COLUMN_NAME": "id",
                "COLUMN_TYPE": "int",
                "DATA_TYPE": "int"
            }
        ])

        handler = AutoIncrementAnalysisToolHandler(driver)
        result = await handler.run_tool({"warning_threshold_pct": 75})

        parsed = json.loads(result[0].text)
        assert len(parsed["at_risk_tables"]) == 1
        assert parsed["at_risk_tables"][0]["usage_pct"] > 75


# =============================================================================
# Replication Tool Tests
# =============================================================================


class TestReplicationStatusToolHandler:
    """Tests for ReplicationStatusToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = ReplicationStatusToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_replication_status"

    @pytest.mark.asyncio
    async def test_run_tool_as_master(self):
        """Test replication status on master."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "log_bin": "ON",
            "server_id": "1",
            "binlog_format": "ROW",
            "gtid_mode": "ON"
        })
        driver.get_server_status = AsyncMock(return_value={})
        driver.execute_query = AsyncMock(side_effect=[
            # SHOW BINARY LOG STATUS / SHOW MASTER STATUS
            [
                {
                    "File": "mysql-bin.000001",
                    "Position": 1234567,
                    "Binlog_Do_DB": "",
                    "Binlog_Ignore_DB": "",
                    "Executed_Gtid_Set": "uuid:1-100"
                }
            ],
            # SHOW BINARY LOGS
            [
                {"Log_name": "mysql-bin.000001", "File_size": 104857600}
            ],
            # SHOW REPLICAS / SHOW SLAVE HOSTS
            [
                {
                    "Server_id": 2,
                    "Host": "replica1",
                    "Port": 3306,
                    "Replica_UUID": "uuid-replica-1"
                }
            ],
            # SHOW REPLICA STATUS / SHOW SLAVE STATUS
            []
        ])

        handler = ReplicationStatusToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["is_master"] is True
        assert parsed["master_status"]["file"] == "mysql-bin.000001"

    @pytest.mark.asyncio
    async def test_run_tool_as_slave(self):
        """Test replication status on slave."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "log_bin": "OFF",
            "server_id": "2"
        })
        driver.get_server_status = AsyncMock(return_value={})
        driver.execute_query = AsyncMock(side_effect=[
            # SHOW REPLICA STATUS / SHOW SLAVE STATUS
            [
                {
                    "Channel_Name": "",
                    "Master_Host": "master1",
                    "Master_Port": 3306,
                    "Master_User": "repl",
                    "Slave_IO_Running": "Yes",
                    "Slave_SQL_Running": "Yes",
                    "Seconds_Behind_Master": 0,
                    "Last_IO_Error": "",
                    "Last_SQL_Error": "",
                    "Relay_Log_File": "relay.000001",
                    "Relay_Log_Pos": 12345,
                    "Master_Log_File": "mysql-bin.000001",
                    "Read_Master_Log_Pos": 1234567,
                    "Exec_Master_Log_Pos": 1234567,
                    "Executed_Gtid_Set": "uuid:1-100",
                    "Auto_Position": 1
                }
            ]
        ])

        handler = ReplicationStatusToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["is_slave"] is True
        assert len(parsed["slave_status"]) == 1
        assert parsed["slave_status"][0]["io_running"] == "Yes"


class TestGaleraClusterToolHandler:
    """Tests for GaleraClusterToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = GaleraClusterToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_galera_status"

    @pytest.mark.asyncio
    async def test_run_tool_galera_enabled(self):
        """Test Galera status when enabled."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "wsrep_on": "ON",
            "wsrep_ready": "ON",
            "wsrep_connected": "ON",
            "wsrep_cluster_name": "my_cluster",
            "wsrep_cluster_size": "3",
            "wsrep_cluster_state_uuid": "uuid-123",
            "wsrep_cluster_status": "Primary",
            "wsrep_local_state": "4",
            "wsrep_local_state_comment": "Synced",
            "wsrep_node_name": "node1",
            "wsrep_local_recv_queue": "0",
            "wsrep_local_recv_queue_avg": "0.0",
            "wsrep_local_send_queue": "0",
            "wsrep_local_send_queue_avg": "0.0",
            "wsrep_local_cert_failures": "0",
            "wsrep_local_bf_aborts": "0",
            "wsrep_flow_control_paused": "0.0",
            "wsrep_flow_control_sent": "0",
            "wsrep_flow_control_recv": "0"
        })

        handler = GaleraClusterToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["is_galera"] is True
        assert parsed["cluster_status"]["cluster_size"] == 3

    @pytest.mark.asyncio
    async def test_run_tool_galera_disabled(self):
        """Test Galera status when disabled."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={})

        handler = GaleraClusterToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["is_galera"] is False


class TestGroupReplicationToolHandler:
    """Tests for GroupReplicationToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = GroupReplicationToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "get_group_replication_status"

    @pytest.mark.asyncio
    async def test_run_tool_gr_enabled(self):
        """Test Group Replication status when enabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "group_replication_group_name": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "group_replication_single_primary_mode": "ON"
        })
        driver.execute_query = AsyncMock(side_effect=[
            # Members query
            [
                {
                    "CHANNEL_NAME": "group_replication_applier",
                    "MEMBER_ID": "uuid-1",
                    "MEMBER_HOST": "node1",
                    "MEMBER_PORT": 3306,
                    "MEMBER_STATE": "ONLINE",
                    "MEMBER_ROLE": "PRIMARY",
                    "MEMBER_VERSION": "8.0.32"
                },
                {
                    "CHANNEL_NAME": "group_replication_applier",
                    "MEMBER_ID": "uuid-2",
                    "MEMBER_HOST": "node2",
                    "MEMBER_PORT": 3306,
                    "MEMBER_STATE": "ONLINE",
                    "MEMBER_ROLE": "SECONDARY",
                    "MEMBER_VERSION": "8.0.32"
                }
            ],
            # Local stats query
            [
                {
                    "MEMBER_ID": "uuid-1",
                    "COUNT_TRANSACTIONS_IN_QUEUE": 0,
                    "COUNT_TRANSACTIONS_CHECKED": 1000,
                    "COUNT_CONFLICTS_DETECTED": 0,
                    "COUNT_TRANSACTIONS_ROWS_VALIDATING": 0
                }
            ]
        ])

        handler = GroupReplicationToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["is_group_replication"] is True
        assert len(parsed["members"]) == 2


# =============================================================================
# Security Tool Tests
# =============================================================================


class TestSecurityAnalysisToolHandler:
    """Tests for SecurityAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = SecurityAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_security"
        assert "include_user_list" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_secure_setup(self):
        """Test security analysis on secure setup."""
        driver = create_mock_sql_driver()

        # Mock all security check queries
        driver.execute_query = AsyncMock(side_effect=[
            [],  # No anonymous users
            [],  # No users without password
            [],  # No root remote access
            [],  # No dangerous privileges
            [],  # No wildcard hosts
            [],  # No test databases
        ])
        driver.execute_scalar = AsyncMock(side_effect=[
            1,  # Root exists
        ])
        driver.get_server_variables = AsyncMock(return_value={
            "validate_password.policy": "MEDIUM",
            "validate_password.length": "8",
            "have_ssl": "YES",
            "require_secure_transport": "ON",
            "tls_version": "TLSv1.2,TLSv1.3"
        })
        driver.get_server_status = AsyncMock(return_value={
            "Ssl_accepts": "1000",
            "Ssl_finished_accepts": "995"
        })

        handler = SecurityAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert "security_score" in parsed
        assert parsed["password_policy"]["enabled"] is True

    @pytest.mark.asyncio
    async def test_run_tool_insecure_setup(self):
        """Test security analysis detects issues."""
        driver = create_mock_sql_driver()

        driver.execute_query = AsyncMock(side_effect=[
            [{"User": "", "Host": "%"}],  # Anonymous user found
            [{"User": "testuser", "Host": "%", "plugin": "mysql_native_password"}],  # User without password
            [{"User": "root", "Host": "%"}],  # Root remote access
            [{"User": "admin", "Host": "%", "Super_priv": "Y", "File_priv": "N",
              "Process_priv": "N", "Shutdown_priv": "N", "Grant_priv": "N"}],  # Dangerous privileges
            [{"User": "app", "Host": "%"}],  # Wildcard host
            [{"SCHEMA_NAME": "test"}],  # Test database exists
        ])
        driver.execute_scalar = AsyncMock(return_value=1)
        driver.get_server_variables = AsyncMock(return_value={
            "have_ssl": "NO"
        })
        driver.get_server_status = AsyncMock(return_value={})

        handler = SecurityAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["security_score"] < 100
        assert len(parsed["issues"]) > 0


class TestUserPrivilegesToolHandler:
    """Tests for UserPrivilegesToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = UserPrivilegesToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "analyze_user_privileges"
        assert "username" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_specific_user(self):
        """Test privilege analysis for specific user."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=[
            # Global privileges
            [
                {
                    "User": "testuser",
                    "Host": "%",
                    "Select_priv": "Y",
                    "Insert_priv": "Y",
                    "Update_priv": "N",
                    "Super_priv": "N"
                }
            ],
            # Database privileges
            [
                {
                    "Db": "testdb",
                    "Select_priv": "Y",
                    "Insert_priv": "Y"
                }
            ],
            # Table privileges
            []
        ])

        handler = UserPrivilegesToolHandler(driver)
        result = await handler.run_tool({"username": "testuser", "hostname": "%"})

        parsed = json.loads(result[0].text)
        assert len(parsed["users"]) == 1
        assert parsed["users"][0]["user"] == "testuser"


class TestAuditLogToolHandler:
    """Tests for AuditLogToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = AuditLogToolHandler(driver)

        definition = handler.get_tool_definition()

        assert definition.name == "check_audit_log"

    @pytest.mark.asyncio
    async def test_run_tool_audit_enabled(self):
        """Test audit log check with MySQL Enterprise Audit."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "audit_log_file": "/var/log/mysql/audit.log",
            "audit_log_format": "JSON",
            "audit_log_policy": "ALL"
        })
        driver.execute_query = AsyncMock(return_value=[
            {"PLUGIN_NAME": "audit_log", "PLUGIN_STATUS": "ACTIVE"}
        ])

        handler = AuditLogToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["audit_enabled"] is True
        assert parsed["audit_plugin"] == "MySQL Enterprise Audit"

    @pytest.mark.asyncio
    async def test_run_tool_audit_disabled(self):
        """Test audit log check when disabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={})
        driver.execute_query = AsyncMock(return_value=[])

        handler = AuditLogToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["audit_enabled"] is False
        assert len(parsed["recommendations"]) > 0

class TestErrorHandling:
    """Tests for error handling across all handlers."""

    @pytest.mark.asyncio
    async def test_innodb_status_error_handling(self):
        """Test InnoDB status error handling."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=Exception("Connection lost"))

        handler = InnoDBStatusToolHandler(driver)
        result = await handler.run_tool({})

        assert len(result) == 1
        assert "error" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_statement_analysis_error_handling(self):
        """Test statement analysis error handling."""
        driver = create_mock_sql_driver()
        driver.execute_scalar = AsyncMock(side_effect=Exception("Access denied"))

        handler = StatementAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        assert len(result) == 1
        assert "error" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_memory_calculation_error_handling(self):
        """Test memory calculation error handling."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(side_effect=Exception("Timeout"))

        handler = MemoryCalculationsToolHandler(driver)
        result = await handler.run_tool({})

        assert len(result) == 1
        assert "error" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_security_analysis_error_handling(self):
        """Test security analysis error handling."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=Exception("Permission denied"))

        handler = SecurityAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        assert len(result) == 1
        assert "error" in result[0].text.lower()


class TestAnnotations:
    """Tests for tool annotations."""

    def test_all_handlers_have_annotations(self):
        """Test that all handlers have proper annotations."""
        driver = create_mock_sql_driver()

        handlers = [
            InnoDBStatusToolHandler(driver),
            InnoDBBufferPoolToolHandler(driver),
            InnoDBTransactionsToolHandler(driver),
            StatementAnalysisToolHandler(driver),
            StatementsTempTablesToolHandler(driver),
            StatementsSortingToolHandler(driver),
            StatementsFullScansToolHandler(driver),
            StatementErrorsToolHandler(driver),
            MemoryCalculationsToolHandler(driver),
            MemoryByHostToolHandler(driver),
            TableMemoryUsageToolHandler(driver),
            StorageEngineAnalysisToolHandler(driver),
            FragmentedTablesToolHandler(driver),
            AutoIncrementAnalysisToolHandler(driver),
            ReplicationStatusToolHandler(driver),
            GaleraClusterToolHandler(driver),
            GroupReplicationToolHandler(driver),
            SecurityAnalysisToolHandler(driver),
            UserPrivilegesToolHandler(driver),
            AuditLogToolHandler(driver),
            # New diagnostic tools
            ConnectionAnalysisToolHandler(driver),
            TableLockAnalysisToolHandler(driver),
            TempTableAnalysisToolHandler(driver),
            PerfSchemaConfigToolHandler(driver),
            OptimizerConfigToolHandler(driver),
            # New schema & binlog tools
            SchemaProfilingToolHandler(driver),
            BinlogAnalysisToolHandler(driver),
            GlobalStatusSnapshotToolHandler(driver),
        ]

        for handler in handlers:
            annotations = handler.get_annotations()
            assert "title" in annotations
            assert "readOnlyHint" in annotations
            assert "destructiveHint" in annotations
            assert annotations["readOnlyHint"] is True
            assert annotations["destructiveHint"] is False


# =============================================================================
# Diagnostic Tool Tests
# =============================================================================


class TestConnectionAnalysisToolHandler:
    """Tests for ConnectionAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = ConnectionAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "analyze_connections"
        assert "inputSchema" in definition.model_dump()

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test connection analysis with mock data."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Threads_connected": "50",
            "Threads_running": "5",
            "Threads_cached": "10",
            "Max_used_connections": "80",
            "Connections": "10000",
            "Aborted_clients": "50",
            "Aborted_connects": "10",
            "Uptime": "86400",
        })
        driver.get_server_variables = AsyncMock(return_value={
            "max_connections": "151",
        })
        driver.execute_query = AsyncMock(return_value=[
            {
                "group_key": "Sleep",
                "connection_count": 40,
                "sleeping": 40,
                "active": 0,
                "max_time_sec": 3600,
                "avg_time_sec": 120.5,
            },
            {
                "group_key": "Query",
                "connection_count": 5,
                "sleeping": 0,
                "active": 5,
                "max_time_sec": 10,
                "avg_time_sec": 2.0,
            }
        ])

        handler = ConnectionAnalysisToolHandler(driver)
        result = await handler.run_tool({"group_by": "state"})

        parsed = json.loads(result[0].text)
        assert parsed["connection_overview"]["current_connections"] == 50
        assert parsed["connection_overview"]["max_connections"] == 151
        assert len(parsed["breakdown"]) == 2

    @pytest.mark.asyncio
    async def test_run_tool_error(self):
        """Test error handling."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(side_effect=Exception("Connection lost"))

        handler = ConnectionAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        assert "error" in result[0].text.lower()


class TestTableLockAnalysisToolHandler:
    """Tests for TableLockAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = TableLockAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "analyze_table_locks"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test table lock analysis with mock data."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Table_locks_waited": "100",
            "Table_locks_immediate": "50000",
            "Innodb_row_lock_waits": "500",
            "Innodb_row_lock_time_avg": "50",
            "Innodb_row_lock_time": "25000",
            "Innodb_row_lock_current_waits": "0",
        })
        driver.get_server_variables = AsyncMock(return_value={
            "lock_wait_timeout": "31536000",
            "innodb_lock_wait_timeout": "50",
        })
        driver.execute_query = AsyncMock(side_effect=[
            # table_lock_waits_summary_by_table
            [
                {
                    "OBJECT_SCHEMA": "mydb",
                    "OBJECT_NAME": "orders",
                    "read_locks": 5000,
                    "write_locks": 1000,
                    "read_normal": 4000,
                    "write_allow_write": 500,
                    "total_wait_ms": 1200.5,
                    "read_wait_ms": 800.0,
                    "write_wait_ms": 400.5,
                }
            ],
            # metadata_locks
            [
                {
                    "OBJECT_SCHEMA": "mydb",
                    "OBJECT_NAME": "orders",
                    "OBJECT_TYPE": "TABLE",
                    "LOCK_TYPE": "SHARED_READ",
                    "LOCK_DURATION": "TRANSACTION",
                    "LOCK_STATUS": "GRANTED",
                    "OWNER_THREAD_ID": 42,
                }
            ],
        ])

        handler = TableLockAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["lock_overview"]["table_locks_waited"] == 100
        assert len(parsed["table_lock_waits"]) == 1
        assert parsed["table_lock_waits"][0]["table"] == "orders"


class TestTempTableAnalysisToolHandler:
    """Tests for TempTableAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = TempTableAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "analyze_temp_tables"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test temp table analysis with mock data."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Created_tmp_tables": "10000",
            "Created_tmp_disk_tables": "3000",
            "Created_tmp_files": "50",
        })
        driver.get_server_variables = AsyncMock(return_value={
            "tmp_table_size": "67108864",
            "max_heap_table_size": "67108864",
            "internal_tmp_mem_storage_engine": "TempTable",
            "tmpdir": "/tmp",
        })
        driver.execute_query = AsyncMock(return_value=[
            {
                "DIGEST_TEXT": "SELECT `col1`, COUNT(*) FROM `t1` GROUP BY `col1`",
                "exec_count": 500,
                "tmp_tables": 500,
                "disk_tmp_tables": 200,
                "disk_pct": 40.0,
                "total_time_sec": 12.5,
                "SCHEMA_NAME": "mydb",
            }
        ])

        handler = TempTableAnalysisToolHandler(driver)
        result = await handler.run_tool({"top_n": 10})

        parsed = json.loads(result[0].text)
        assert parsed["overview"]["disk_tmp_pct"] == 30.0
        assert parsed["configuration"]["tmp_table_size_mb"] == 64.0
        assert len(parsed["top_disk_temp_queries"]) == 1

    @pytest.mark.asyncio
    async def test_run_tool_high_disk_pct(self):
        """Test recommendations for high disk temp table percentage."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Created_tmp_tables": "10000",
            "Created_tmp_disk_tables": "5000",
            "Created_tmp_files": "100",
        })
        driver.get_server_variables = AsyncMock(return_value={
            "tmp_table_size": "16777216",
            "max_heap_table_size": "33554432",
            "internal_tmp_mem_storage_engine": "TempTable",
            "tmpdir": "/tmp",
        })
        driver.execute_query = AsyncMock(return_value=[])

        handler = TempTableAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["overview"]["disk_tmp_pct"] == 50.0
        assert len(parsed["recommendations"]) > 0
        # Should recommend aligning tmp_table_size and max_heap_table_size
        recs_text = " ".join(parsed["recommendations"])
        assert "differ" in recs_text.lower() or "disk" in recs_text.lower()


class TestPerfSchemaConfigToolHandler:
    """Tests for PerfSchemaConfigToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = PerfSchemaConfigToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "check_perf_schema_config"

    @pytest.mark.asyncio
    async def test_run_tool_enabled(self):
        """Test with performance_schema enabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "performance_schema": "ON",
        })
        driver.get_server_status = AsyncMock(return_value={
            "Performance_schema_memory": "104857600",
        })
        driver.execute_query = AsyncMock(side_effect=[
            # setup_instruments
            [
                {"category": "statement/sql", "total": 100, "enabled": 80, "timed": 80},
                {"category": "wait/io", "total": 50, "enabled": 40, "timed": 40},
                {"category": "stage/sql", "total": 30, "enabled": 0, "timed": 0},
                {"category": "memory/sql", "total": 20, "enabled": 10, "timed": 0},
            ],
            # setup_consumers
            [
                {"NAME": "events_statements_current", "ENABLED": "YES"},
                {"NAME": "events_statements_history", "ENABLED": "YES"},
                {"NAME": "events_waits_current", "ENABLED": "YES"},
                {"NAME": "events_waits_history", "ENABLED": "NO"},
            ],
        ])

        handler = PerfSchemaConfigToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["performance_schema_enabled"] is True
        assert parsed["memory_usage"]["total_mb"] == 100.0
        assert parsed["tool_readiness"]["slow_query_analysis"] is True

    @pytest.mark.asyncio
    async def test_run_tool_disabled(self):
        """Test with performance_schema disabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "performance_schema": "OFF",
        })
        driver.get_server_status = AsyncMock(return_value={})

        handler = PerfSchemaConfigToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["performance_schema_enabled"] is False
        assert len(parsed["recommendations"]) > 0
        assert parsed["tool_readiness"]["slow_query_analysis"] is False


class TestOptimizerConfigToolHandler:
    """Tests for OptimizerConfigToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = OptimizerConfigToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "review_optimizer_config"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test optimizer config review."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "optimizer_switch": "index_merge=on,index_merge_union=on,mrr=off,batched_key_access=off,derived_merge=on",
            "optimizer_search_depth": "62",
            "optimizer_prune_level": "1",
            "optimizer_trace": "enabled=off",
            "optimizer_trace_max_mem_size": "1048576",
            "eq_range_index_dive_limit": "200",
            "range_optimizer_max_mem_size": "8388608",
            "max_join_size": "18446744073709551615",
            "join_buffer_size": "262144",
            "sort_buffer_size": "262144",
            "read_rnd_buffer_size": "262144",
        })
        # Cost model query may fail (MySQL 5.7)
        driver.execute_query = AsyncMock(side_effect=Exception("Table not found"))

        handler = OptimizerConfigToolHandler(driver)
        result = await handler.run_tool({"include_cost_model": True})

        parsed = json.loads(result[0].text)
        assert parsed["optimizer_switches"]["index_merge"] == "on"
        assert parsed["optimizer_switches"]["mrr"] == "off"
        assert parsed["key_settings"]["optimizer_search_depth"] == "62"
        # Should recommend MRR and BKA
        recs_text = " ".join(parsed["recommendations"])
        assert "mrr" in recs_text.lower() or "Multi-Range" in recs_text

    @pytest.mark.asyncio
    async def test_run_tool_with_cost_model(self):
        """Test optimizer config with cost model available."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "optimizer_switch": "index_merge=on,derived_merge=on,mrr=on,batched_key_access=on",
            "optimizer_search_depth": "62",
            "optimizer_prune_level": "1",
        })
        driver.execute_query = AsyncMock(side_effect=[
            # server_cost
            [{"cost_name": "disk_temptable_create_cost", "cost_value": None, "default_value": 20.0}],
            # engine_cost
            [{"engine_name": "default", "cost_name": "io_block_read_cost", "cost_value": None, "default_value": 1.0}],
        ])

        handler = OptimizerConfigToolHandler(driver)
        result = await handler.run_tool({"include_cost_model": True})

        parsed = json.loads(result[0].text)
        assert "server_cost" in parsed["cost_model"]
        assert "engine_cost" in parsed["cost_model"]


# =============================================================================
# Schema & Binlog Tool Tests
# =============================================================================


class TestSchemaProfilingToolHandler:
    """Tests for SchemaProfilingToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = SchemaProfilingToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "profile_schema_sizes"

    @pytest.mark.asyncio
    async def test_run_tool(self):
        """Test schema profiling with mock data."""
        driver = create_mock_sql_driver()
        driver.execute_query = AsyncMock(side_effect=[
            # Database sizes
            [
                {
                    "db_name": "myapp",
                    "table_count": 25,
                    "data_size": 1073741824,
                    "index_size": 536870912,
                    "total_size": 1610612736,
                    "free_space": 104857600,
                    "total_rows": 5000000,
                }
            ],
            # Largest tables
            [
                {
                    "TABLE_SCHEMA": "myapp",
                    "TABLE_NAME": "events",
                    "ENGINE": "InnoDB",
                    "TABLE_ROWS": 3000000,
                    "AVG_ROW_LENGTH": 256,
                    "DATA_LENGTH": 768000000,
                    "INDEX_LENGTH": 384000000,
                    "total_size": 1152000000,
                    "DATA_FREE": 50000000,
                }
            ],
        ])

        handler = SchemaProfilingToolHandler(driver)
        result = await handler.run_tool({"top_n": 10})

        parsed = json.loads(result[0].text)
        assert len(parsed["database_sizes"]) == 1
        assert parsed["database_sizes"][0]["database"] == "myapp"
        assert len(parsed["largest_tables"]) == 1
        assert parsed["largest_tables"][0]["table"] == "events"
        assert parsed["summary"]["total_databases"] == 1


class TestBinlogAnalysisToolHandler:
    """Tests for BinlogAnalysisToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = BinlogAnalysisToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "analyze_binlog"

    @pytest.mark.asyncio
    async def test_run_tool_enabled(self):
        """Test with binary logging enabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "log_bin": "ON",
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "sync_binlog": "1",
            "binlog_cache_size": "32768",
            "max_binlog_size": "1073741824",
            "binlog_expire_logs_seconds": "604800",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON",
        })
        driver.get_server_status = AsyncMock(return_value={
            "Binlog_cache_use": "5000",
            "Binlog_cache_disk_use": "50",
            "Binlog_bytes_written": "1073741824",
            "Uptime": "86400",
        })
        driver.execute_query = AsyncMock(return_value=[
            {"Log_name": "binlog.000001", "File_size": 536870912},
            {"Log_name": "binlog.000002", "File_size": 268435456},
        ])

        handler = BinlogAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["binlog_enabled"] is True
        assert parsed["configuration"]["binlog_format"] == "ROW"
        assert len(parsed["binlog_files"]) == 2
        assert parsed["throughput"]["total_binlog_files"] == 2

    @pytest.mark.asyncio
    async def test_run_tool_disabled(self):
        """Test with binary logging disabled."""
        driver = create_mock_sql_driver()
        driver.get_server_variables = AsyncMock(return_value={
            "log_bin": "OFF",
        })
        driver.get_server_status = AsyncMock(return_value={})

        handler = BinlogAnalysisToolHandler(driver)
        result = await handler.run_tool({})

        parsed = json.loads(result[0].text)
        assert parsed["binlog_enabled"] is False
        assert len(parsed["recommendations"]) > 0


class TestGlobalStatusSnapshotToolHandler:
    """Tests for GlobalStatusSnapshotToolHandler."""

    def test_tool_definition(self):
        """Test tool definition."""
        driver = create_mock_sql_driver()
        handler = GlobalStatusSnapshotToolHandler(driver)

        definition = handler.get_tool_definition()
        assert definition.name == "get_global_status_snapshot"

    @pytest.mark.asyncio
    async def test_run_tool_all(self):
        """Test global status snapshot with all categories."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Uptime": "86400",
            "Questions": "1000000",
            "Queries": "1000000",
            "Com_select": "600000",
            "Com_insert": "200000",
            "Com_update": "150000",
            "Com_delete": "50000",
            "Slow_queries": "100",
            "Threads_connected": "50",
            "Threads_running": "5",
            "Bytes_received": "5368709120",
            "Bytes_sent": "10737418240",
            "Innodb_buffer_pool_reads": "1000",
            "Innodb_buffer_pool_read_requests": "10000000",
            "Handler_read_key": "5000000",
            "Handler_read_rnd_next": "2000000",
        })

        handler = GlobalStatusSnapshotToolHandler(driver)
        result = await handler.run_tool({"category": "all"})

        parsed = json.loads(result[0].text)
        assert parsed["uptime_seconds"] == 86400
        assert "Questions" in parsed["counters"]
        assert "Com_select" in parsed["counters"]
        assert "Questions" in parsed["rates_per_second"]
        assert parsed["computed"]["read_pct"] == 60.0

    @pytest.mark.asyncio
    async def test_run_tool_throughput_only(self):
        """Test snapshot with throughput category only."""
        driver = create_mock_sql_driver()
        driver.get_server_status = AsyncMock(return_value={
            "Uptime": "3600",
            "Questions": "50000",
            "Queries": "50000",
            "Com_select": "40000",
            "Com_insert": "5000",
            "Com_update": "3000",
            "Com_delete": "2000",
            "Slow_queries": "5",
            "Bytes_received": "1073741824",
            "Bytes_sent": "2147483648",
        })

        handler = GlobalStatusSnapshotToolHandler(driver)
        result = await handler.run_tool({"category": "throughput"})

        parsed = json.loads(result[0].text)
        assert "Questions" in parsed["counters"]
        assert "Com_select" in parsed["counters"]
        # Should NOT include handler keys
        assert "Handler_read_key" not in parsed["counters"]


def test_safe_drop_index_falls_back_on_malicious_name():
    from mysqltuner_mcp.tools.tools_index import _safe_drop_index
    out = _safe_drop_index("foo`; DROP TABLE x; --", "users")
    assert out.startswith("-- skipped:")
    assert "DROP TABLE x" in out  # echoed inside a comment, NOT executable
    assert ";" not in out.split("--", 2)[0]  # nothing executable before the comment


def test_safe_table_ref_falls_back_on_malicious_name():
    from mysqltuner_mcp.tools.tools_engines import _safe_table_ref
    out = _safe_table_ref("db", "users`; DROP TABLE x; --")
    assert out.startswith("-- skipped:")



@pytest.mark.asyncio
async def test_analyze_query_rejects_multi_statement(mock_sql_driver):
    handler = AnalyzeQueryToolHandler(mock_sql_driver)
    result = await handler.run_tool({"query": "SELECT 1; DROP TABLE x"})
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"
    assert "Only one statement" in data["message"]


@pytest.mark.asyncio
async def test_analyze_query_rejects_comment_bypass(mock_sql_driver):
    handler = AnalyzeQueryToolHandler(mock_sql_driver)
    result = await handler.run_tool({"query": "SELECT 1 /*;*/; DROP TABLE x"})
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"


@pytest.mark.asyncio
async def test_analyze_query_rejects_ddl(mock_sql_driver):
    handler = AnalyzeQueryToolHandler(mock_sql_driver)
    result = await handler.run_tool({"query": "CREATE TABLE evil (x INT)"})
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"
    assert "not permitted" in data["message"]


@pytest.mark.asyncio
async def test_analyze_query_explain_allows_update_without_confirm(mock_sql_driver):
    """Plain EXPLAIN on UPDATE is allowed — EXPLAIN does not execute."""
    mock_sql_driver.execute_query = AsyncMock(return_value=[])
    handler = AnalyzeQueryToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query": "UPDATE users SET x = 1",
        "analyze": False,
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    mock_sql_driver.execute_query.assert_called_once()


@pytest.mark.asyncio
async def test_analyze_query_explain_analyze_update_requires_confirm(mock_sql_driver):
    handler = AnalyzeQueryToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query": "UPDATE users SET x = 1",
        "analyze": True,
        "confirm_write": False,
    })
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"
    assert "confirm_write" in data["message"]


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_empty_when_no_waits(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[])
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["roots"] == []
    assert data["edges"] == []
    assert data["cycles"] == []
    assert data["summary"]["total_waiters"] == 0


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_linear_chain(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "orders",
         "wait_seconds": 5, "blocker_query": "UPDATE orders SET ...",
         "waiter_query": "UPDATE orders WHERE id=1"},
        {"blocker_trx_id": "T2", "blocker_thread_id": 2, "waiter_trx_id": "T3",
         "waiter_thread_id": 3, "lock_type": "RECORD", "table_name": "orders",
         "wait_seconds": 2, "blocker_query": "UPDATE orders WHERE id=1",
         "waiter_query": "UPDATE orders WHERE id=2"},
    ])
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    root_ids = {r["trx_id"] for r in data["roots"]}
    assert root_ids == {"T1"}
    assert len(data["edges"]) == 2
    assert data["cycles"] == []
    assert data["summary"]["longest_chain_depth"] == 3


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_detects_cycle(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"blocker_trx_id": "T2", "blocker_thread_id": 2, "waiter_trx_id": "T1",
         "waiter_thread_id": 1, "lock_type": "RECORD", "table_name": "users",
         "wait_seconds": 1, "blocker_query": "q2", "waiter_query": "q1"},
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "users",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q2"},
    ])
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert len(data["cycles"]) >= 1


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_8_0_only_error(mock_sql_driver):
    import pymysql.err
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(
        side_effect=pymysql.err.ProgrammingError(
            1146, "Table 'performance_schema.data_lock_waits' doesn't exist"
        )
    )
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["error_type"] == "ValueError"
    assert "MySQL 8.0+" in data["message"]


@pytest.mark.asyncio
async def test_compare_explain_plans_missing_required_args(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler
    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({"query_a": "SELECT 1"})  # missing query_b
    data = json.loads(result[0].text)
    assert data["error_type"] == "ValueError"
    assert "query_b" in data["message"]


@pytest.mark.asyncio
async def test_compare_explain_plans_rejects_bad_query_a(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler
    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "DROP TABLE x",
        "query_b": "SELECT 1",
    })
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"


@pytest.mark.asyncio
async def test_compare_explain_plans_rejects_bad_query_b(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler
    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT 1",
        "query_b": "SELECT 1; DROP TABLE x",
    })
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"


@pytest.mark.asyncio
async def test_compare_explain_plans_detects_full_scan_change(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler

    plan_a = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "users", "access_type": "ALL",
            "rows_examined_per_scan": 10000, "filtered": 10
        }}
    })}
    plan_b = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "users", "access_type": "ref",
            "key": "idx_email", "rows_examined_per_scan": 1, "filtered": 100
        }}
    })}
    mock_sql_driver.execute_query = AsyncMock(side_effect=[[plan_a], [plan_b]])

    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT * FROM users WHERE email = 'x'",
        "query_b": "SELECT * FROM users WHERE email = 'x'",
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["verdict"] == "B is better"
    assert any("full scan" in r.lower() for r in data["rationale"])


@pytest.mark.asyncio
async def test_compare_explain_plans_no_significant_difference(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler

    plan = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "users", "access_type": "ref",
            "key": "PRIMARY", "rows_examined_per_scan": 1, "filtered": 100
        }}
    })}
    mock_sql_driver.execute_query = AsyncMock(side_effect=[[plan], [plan]])

    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT * FROM users WHERE id = 1",
        "query_b": "SELECT * FROM users WHERE id = 1",
    })
    data = json.loads(result[0].text)
    assert data["verdict"] == "no significant difference"


@pytest.mark.asyncio
async def test_table_io_hotspots_returns_top_n_sorted(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"FILE_NAME": "/var/lib/mysql/testdb/orders.ibd",
         "total_read_bytes": 1000, "total_write_bytes": 2000,
         "total_read_timer": 2e12, "total_write_timer": 4e12,
         "read_count": 100, "write_count": 200},
        {"FILE_NAME": "/var/lib/mysql/testdb/users.ibd",
         "total_read_bytes": 100, "total_write_bytes": 200,
         "total_read_timer": 1e11, "total_write_timer": 2e11,
         "read_count": 10, "write_count": 20},
    ])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({"limit": 5})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["tables"][0]["table"] == "orders"
    assert data["tables"][1]["table"] == "users"


@pytest.mark.asyncio
async def test_table_io_hotspots_excludes_system_schemas(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"FILE_NAME": "/var/lib/mysql/mysql/user.ibd",
         "total_read_bytes": 1000, "total_write_bytes": 1000,
         "total_read_timer": 1e12, "total_write_timer": 1e12,
         "read_count": 1, "write_count": 1},
    ])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["tables"] == []


def test_table_io_hotspots_filename_parser():
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    assert TableIoHotspotsToolHandler._parse_filename(
        "/var/lib/mysql/testdb/orders.ibd"
    ) == ("testdb", "orders")
    assert TableIoHotspotsToolHandler._parse_filename(
        "./testdb/users.ibd"
    ) == ("testdb", "users")
    assert TableIoHotspotsToolHandler._parse_filename("") == ("", "")
    assert TableIoHotspotsToolHandler._parse_filename("noslash.ibd") == ("", "")


@pytest.mark.asyncio
async def test_table_io_hotspots_empty(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["tables"] == []
    assert data["summary"]["table_count"] == 0


@pytest.mark.asyncio
async def test_table_io_hotspots_recommends_when_top_dominates(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    # One huge table + small ones -> top dominates
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"FILE_NAME": "/var/lib/mysql/testdb/big.ibd",
         "total_read_bytes": 1, "total_write_bytes": 1,
         "total_read_timer": 100e12, "total_write_timer": 100e12,
         "read_count": 1, "write_count": 1},
        {"FILE_NAME": "/var/lib/mysql/testdb/small.ibd",
         "total_read_bytes": 1, "total_write_bytes": 1,
         "total_read_timer": 1e9, "total_write_timer": 1e9,
         "read_count": 1, "write_count": 1},
    ])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert any("Top table dominates" in r for r in data["recommendations"])


@pytest.mark.asyncio
async def test_redo_log_pressure_uses_redo_log_capacity_when_present(monkeypatch):
    """8.0.30+ path: innodb_redo_log_capacity is the source of truth."""
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(side_effect=[
        {"innodb_redo_log_capacity": "2147483648"},  # capacity (2 GB)
    ])
    # Two INNODB_METRICS samples
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 1000, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 100, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 1000000, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 100, "STATUS": "enabled"}],
    ])

    # Skip the real 2-second sleep
    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["redo_log_capacity_bytes"] == 2147483648
    assert data["lsn_write_rate_bytes_per_sec"] > 0


@pytest.mark.asyncio
async def test_redo_log_pressure_falls_back_to_legacy_vars(monkeypatch):
    """5.7 / pre-8.0.30 path: innodb_log_file_size * innodb_log_files_in_group."""
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(side_effect=[
        # innodb_redo_log_capacity not present or 0
        {},
        {"innodb_log_files_in_group": "2", "innodb_log_file_size": "536870912"},
    ])
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 1000, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["redo_log_capacity_bytes"] == 2 * 536870912


@pytest.mark.asyncio
async def test_redo_log_pressure_undersized_on_high_checkpoint_pct(monkeypatch):
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(return_value={"innodb_redo_log_capacity": "100"})
    # checkpoint age 80% of 100
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 1000, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 80, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 1000, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 80, "STATUS": "enabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["verdict"] == "undersized"


@pytest.mark.asyncio
async def test_redo_log_pressure_healthy(monkeypatch):
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(return_value={"innodb_redo_log_capacity": "2147483648"})
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 100, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 500000, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 100, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 500000, "STATUS": "enabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["verdict"] == "healthy"


@pytest.mark.asyncio
async def test_redo_log_pressure_insufficient_data_when_metrics_disabled(monkeypatch):
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(return_value={"innodb_redo_log_capacity": "2147483648"})
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "disabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "disabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["verdict"] == "insufficient_data"
    assert data["recommendation"] is None


@pytest.mark.asyncio
async def test_redo_log_pressure_lsn_sampling_uses_two_calls(monkeypatch):
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    driver.get_server_variables = AsyncMock(return_value={"innodb_redo_log_capacity": "100"})
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 100, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    await handler.run_tool({})
    # Two INNODB_METRICS sample queries
    assert driver.execute_query.await_count == 2


@pytest.mark.asyncio
async def test_temp_table_spills_empty_when_nothing_spilling(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_statements import TempTableSpillsInProgressToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[])
    handler = TempTableSpillsInProgressToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["active_spills"] == []
    assert data["summary"]["count"] == 0


@pytest.mark.asyncio
async def test_temp_table_spills_lists_active_with_pct(mock_sql_driver):
    from mysqltuner_mcp.tools.tools_statements import TempTableSpillsInProgressToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"THREAD_ID": 42, "SQL_TEXT": "SELECT * FROM orders ORDER BY total",
         "EVENT_NAME": "stage/sql/Creating sort index",
         "WORK_ESTIMATED": 1000, "WORK_COMPLETED": 250,
         "TIMER_WAIT": 2_000_000_000_000},  # 2 sec in picoseconds
    ])
    handler = TempTableSpillsInProgressToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert len(data["active_spills"]) == 1
    spill = data["active_spills"][0]
    assert spill["thread_id"] == 42
    assert spill["pct_complete"] == 25.0
    assert spill["elapsed_sec"] == 2.0


@pytest.mark.asyncio
async def test_temp_table_spills_handles_zero_estimated(mock_sql_driver):
    """WORK_ESTIMATED can be 0; pct must not divide by zero."""
    from mysqltuner_mcp.tools.tools_statements import TempTableSpillsInProgressToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"THREAD_ID": 1, "SQL_TEXT": "SELECT 1",
         "EVENT_NAME": "stage/sql/copy to tmp table",
         "WORK_ESTIMATED": 0, "WORK_COMPLETED": 0,
         "TIMER_WAIT": 0},
    ])
    handler = TempTableSpillsInProgressToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["active_spills"][0]["pct_complete"] is None


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_diamond_dag(mock_sql_driver):
    """Regression: backtracking _chain_depth must report correct longest
    chain when two paths converge at a common descendant.

    Graph (T1 is root):
        T1 -> T2 -> T4
        T1 -> T3 -> T4

    longest_chain_depth should be 3 (T1 -> T2 -> T4 or T1 -> T3 -> T4),
    not 2 (which would happen if T4 were marked visited on the first
    branch and skipped on the second).
    """
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "x",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q2"},
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T3",
         "waiter_thread_id": 3, "lock_type": "RECORD", "table_name": "x",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q3"},
        {"blocker_trx_id": "T2", "blocker_thread_id": 2, "waiter_trx_id": "T4",
         "waiter_thread_id": 4, "lock_type": "RECORD", "table_name": "x",
         "wait_seconds": 1, "blocker_query": "q2", "waiter_query": "q4"},
        {"blocker_trx_id": "T3", "blocker_thread_id": 3, "waiter_trx_id": "T4",
         "waiter_thread_id": 4, "lock_type": "RECORD", "table_name": "x",
         "wait_seconds": 1, "blocker_query": "q3", "waiter_query": "q4"},
    ])
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["summary"]["longest_chain_depth"] == 3


@pytest.mark.asyncio
async def test_redo_log_pressure_capacity_zero_is_insufficient_data(monkeypatch):
    """Regression: capacity==0 must yield insufficient_data, not 'undersized'
    with a 'recommendation = 0' bytes.
    """
    from unittest.mock import AsyncMock
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler

    driver = AsyncMock()
    # Neither innodb_redo_log_capacity nor legacy vars resolve -> capacity == 0
    driver.get_server_variables = AsyncMock(side_effect=[{}, {}])
    driver.execute_query = AsyncMock(side_effect=[
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
        [{"NAME": "log_lsn_current", "COUNT": 0, "STATUS": "enabled"},
         {"NAME": "log_lsn_checkpoint_age", "COUNT": 0, "STATUS": "enabled"}],
    ])

    async def fake_sleep(_):
        return
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    handler = InnoDBRedoLogPressureToolHandler(driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert data["verdict"] == "insufficient_data"
    assert data["recommendation"] is None
    assert data["redo_log_capacity_bytes"] == 0


@pytest.mark.asyncio
async def test_compare_explain_plans_handles_lowercase_explain_column(mock_sql_driver):
    """Regression: some drivers return the EXPLAIN column lowercase;
    accessing res[0]['EXPLAIN'] would KeyError. Use a case-insensitive fallback.
    """
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler

    plan = {"explain": json.dumps({  # NOTE: lowercase key
        "query_block": {"table": {
            "table_name": "users", "access_type": "ref",
            "key": "PRIMARY", "rows_examined_per_scan": 1, "filtered": 100
        }}
    })}
    mock_sql_driver.execute_query = AsyncMock(side_effect=[[plan], [plan]])

    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT 1",
        "query_b": "SELECT 1",
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["verdict"] == "no significant difference"


@pytest.mark.asyncio
async def test_compare_explain_plans_ignores_trivial_row_difference(mock_sql_driver):
    """Regression: a 1-vs-0 rows-examined difference must NOT be reported
    as 'better' — apply both a 20% relative threshold AND a 10-row absolute floor.
    """
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler

    plan_a = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "x", "access_type": "ref", "key": "PRIMARY",
            "rows_examined_per_scan": 1, "filtered": 100
        }}
    })}
    plan_b = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "x", "access_type": "ref", "key": "PRIMARY",
            "rows_examined_per_scan": 0, "filtered": 100
        }}
    })}
    mock_sql_driver.execute_query = AsyncMock(side_effect=[[plan_a], [plan_b]])

    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT 1",
        "query_b": "SELECT 1",
    })
    data = json.loads(result[0].text)
    assert data["verdict"] == "no significant difference"


@pytest.mark.asyncio
async def test_compare_explain_plans_meaningful_row_difference_still_wins(mock_sql_driver):
    """Sanity inverse: 1000 vs 100 rows examined IS significant; B should win."""
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler

    plan_a = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "x", "access_type": "ref", "key": "k1",
            "rows_examined_per_scan": 1000, "filtered": 100
        }}
    })}
    plan_b = {"EXPLAIN": json.dumps({
        "query_block": {"table": {
            "table_name": "x", "access_type": "ref", "key": "k2",
            "rows_examined_per_scan": 100, "filtered": 100
        }}
    })}
    mock_sql_driver.execute_query = AsyncMock(side_effect=[[plan_a], [plan_b]])

    handler = CompareExplainPlansToolHandler(mock_sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT 1",
        "query_b": "SELECT 1",
    })
    data = json.loads(result[0].text)
    assert data["verdict"] == "B is better"


def test_table_io_hotspots_strips_partition_suffix():
    """Regression: MySQL partition files are named e.g. orders#p#p1.ibd;
    the parser must return 'orders', not 'orders#p#p1', so per-partition
    I/O aggregates back to the logical table.
    """
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    # lowercase MySQL partition suffix
    assert TableIoHotspotsToolHandler._parse_filename(
        "/var/lib/mysql/testdb/orders#p#p1.ibd"
    ) == ("testdb", "orders")
    # uppercase suffix (older MySQL versions)
    assert TableIoHotspotsToolHandler._parse_filename(
        "/var/lib/mysql/testdb/orders#P#p0.ibd"
    ) == ("testdb", "orders")
    # subpartition
    assert TableIoHotspotsToolHandler._parse_filename(
        "/var/lib/mysql/testdb/orders#P#p0#SP#sp0.ibd"
    ) == ("testdb", "orders")
    # No partition suffix: existing behavior unchanged
    assert TableIoHotspotsToolHandler._parse_filename(
        "/var/lib/mysql/testdb/users.ibd"
    ) == ("testdb", "users")


@pytest.mark.asyncio
async def test_table_io_hotspots_aggregates_partitions(mock_sql_driver):
    """Regression: I/O from multiple partitions of the same table must be
    aggregated into ONE entry — both the name normalized AND the metrics
    summed — not just deduped to same name with separate rows.
    """
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"FILE_NAME": "/var/lib/mysql/testdb/orders#p#p1.ibd",
         "total_read_bytes": 1000, "total_write_bytes": 1000,
         "total_read_timer": 1e12, "total_write_timer": 1e12,
         "read_count": 100, "write_count": 100},
        {"FILE_NAME": "/var/lib/mysql/testdb/orders#p#p2.ibd",
         "total_read_bytes": 2000, "total_write_bytes": 2000,
         "total_read_timer": 2e12, "total_write_timer": 2e12,
         "read_count": 200, "write_count": 200},
    ])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    # Exactly ONE row for the logical "orders" table, not one-per-partition
    assert len(data["tables"]) == 1, (
        f"expected single aggregated row; got {len(data['tables'])}"
    )
    row = data["tables"][0]
    assert row["table"] == "orders"
    assert row["schema"] == "testdb"
    # Metrics must be summed across partitions
    assert row["total_read_bytes"] == 3000  # 1000 + 2000
    assert row["total_write_bytes"] == 3000
    assert row["total_read_latency_sec"] == 3.0  # (1e12 + 2e12) / 1e12
    assert row["total_write_latency_sec"] == 3.0
    # avg latency uses summed counts: 3e12 ps / 300 reads / 1e6 = 10000 us
    assert row["avg_read_latency_us"] == 10000.0


@pytest.mark.asyncio
async def test_table_io_hotspots_keeps_distinct_tables_separate(mock_sql_driver):
    """Sanity: aggregation must NOT collapse genuinely-distinct tables."""
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"FILE_NAME": "/var/lib/mysql/testdb/orders.ibd",
         "total_read_bytes": 1000, "total_write_bytes": 1000,
         "total_read_timer": 1e12, "total_write_timer": 1e12,
         "read_count": 100, "write_count": 100},
        {"FILE_NAME": "/var/lib/mysql/testdb/users.ibd",
         "total_read_bytes": 2000, "total_write_bytes": 2000,
         "total_read_timer": 2e12, "total_write_timer": 2e12,
         "read_count": 200, "write_count": 200},
        # Same table NAME in a different schema must stay separate
        {"FILE_NAME": "/var/lib/mysql/otherdb/orders.ibd",
         "total_read_bytes": 500, "total_write_bytes": 500,
         "total_read_timer": 5e11, "total_write_timer": 5e11,
         "read_count": 50, "write_count": 50},
    ])
    handler = TableIoHotspotsToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert len(data["tables"]) == 3
    by_key = {(t["schema"], t["table"]): t for t in data["tables"]}
    assert ("testdb", "orders") in by_key
    assert ("testdb", "users") in by_key
    assert ("otherdb", "orders") in by_key


@pytest.mark.asyncio
async def test_analyze_lock_wait_graph_dedupes_duplicate_edges(mock_sql_driver):
    """Regression: multiple lock-wait rows between the same blocker/waiter
    pair must not inflate blocks_count or trigger redundant DFS.

    Setup: T1 blocks T2 across THREE different lock waits on different
    tables (a realistic scenario when one long transaction holds locks
    on multiple rows another transaction needs). The structural graph
    is still T1 -> T2, so blocks_count must be 1 (not 3).
    """
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    mock_sql_driver.execute_query = AsyncMock(return_value=[
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "orders",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q2"},
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "users",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q2"},
        {"blocker_trx_id": "T1", "blocker_thread_id": 1, "waiter_trx_id": "T2",
         "waiter_thread_id": 2, "lock_type": "RECORD", "table_name": "products",
         "wait_seconds": 1, "blocker_query": "q1", "waiter_query": "q2"},
    ])
    handler = LockWaitGraphToolHandler(mock_sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    # The raw edges list preserves every wait event (3 total — per-wait detail)
    assert len(data["edges"]) == 3
    # But the structural graph has T1 -> T2 ONCE, so blocks_count is 1
    roots_by_id = {r["trx_id"]: r for r in data["roots"]}
    assert roots_by_id["T1"]["blocks_count"] == 1
    # And longest_chain_depth is 2 (T1 -> T2), not double-counted
    assert data["summary"]["longest_chain_depth"] == 2
