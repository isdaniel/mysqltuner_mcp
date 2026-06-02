"""Integration tests for diagnostic tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_diagnostic import (
    ConnectionAnalysisToolHandler,
    OptimizerConfigToolHandler,
    PerfSchemaConfigToolHandler,
    TableLockAnalysisToolHandler,
    TempTableAnalysisToolHandler,
)


@pytest.mark.asyncio
async def test_analyze_connections(sql_driver):
    handler = ConnectionAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_table_locks(sql_driver):
    handler = TableLockAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_temp_tables(sql_driver):
    handler = TempTableAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_check_perf_schema_config(sql_driver):
    handler = PerfSchemaConfigToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_review_optimizer_config(sql_driver):
    handler = OptimizerConfigToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


from tests.integration._compat import requires_mysql


@pytest.mark.asyncio
@requires_mysql(min_version="8.0.0")
async def test_analyze_lock_wait_graph(sql_driver, mysql_version):
    import json as _json
    from mysqltuner_mcp.tools.tools_diagnostic import LockWaitGraphToolHandler
    handler = LockWaitGraphToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = _json.loads(result[0].text)
    assert "error_type" not in data
    # Idle test server: no live lock waits expected
    assert data["roots"] == []
    assert data["edges"] == []
    assert data["summary"]["total_waiters"] == 0
