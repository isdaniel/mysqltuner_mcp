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
        ]

        for handler in handlers:
            annotations = handler.get_annotations()
            assert "title" in annotations
            assert "readOnlyHint" in annotations
            assert "destructiveHint" in annotations
            assert annotations["readOnlyHint"] is True
            assert annotations["destructiveHint"] is False
