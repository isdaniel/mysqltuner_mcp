"""Integration tests for engines tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_engines import (
    AutoIncrementAnalysisToolHandler,
    FragmentedTablesToolHandler,
    StorageEngineAnalysisToolHandler,
)


@pytest.mark.asyncio
async def test_analyze_storage_engines(sql_driver):
    handler = StorageEngineAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_fragmented_tables(sql_driver):
    handler = FragmentedTablesToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_auto_increment(sql_driver):
    handler = AutoIncrementAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
