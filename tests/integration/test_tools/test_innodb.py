"""Integration tests for innodb tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_innodb import (
    InnoDBBufferPoolToolHandler,
    InnoDBStatusToolHandler,
    InnoDBTransactionsToolHandler,
)


@pytest.mark.asyncio
async def test_get_innodb_status(sql_driver):
    handler = InnoDBStatusToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_buffer_pool(sql_driver):
    handler = InnoDBBufferPoolToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_innodb_transactions(sql_driver):
    handler = InnoDBTransactionsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_innodb_redo_log_pressure(sql_driver):
    from mysqltuner_mcp.tools.tools_innodb import InnoDBRedoLogPressureToolHandler
    handler = InnoDBRedoLogPressureToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["verdict"] in ("healthy", "undersized", "oversized", "insufficient_data")
    assert "redo_log_capacity_bytes" in data
