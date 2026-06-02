"""Integration tests for memory tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_memory import (
    MemoryByHostToolHandler,
    MemoryCalculationsToolHandler,
    TableMemoryUsageToolHandler,
)


@pytest.mark.asyncio
async def test_calculate_memory_usage(sql_driver):
    handler = MemoryCalculationsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_memory_by_host(sql_driver):
    handler = MemoryByHostToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_table_memory_usage(sql_driver):
    handler = TableMemoryUsageToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
