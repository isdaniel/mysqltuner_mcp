"""Integration tests for schema + binlog + global-status tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_schema import (
    BinlogAnalysisToolHandler,
    GlobalStatusSnapshotToolHandler,
    SchemaProfilingToolHandler,
)


@pytest.mark.asyncio
async def test_profile_schema_sizes(sql_driver):
    handler = SchemaProfilingToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_binlog(sql_driver):
    handler = BinlogAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_global_status_snapshot(sql_driver):
    handler = GlobalStatusSnapshotToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
