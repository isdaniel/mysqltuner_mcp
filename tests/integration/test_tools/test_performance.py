"""Integration tests for performance tools: get_slow_queries, analyze_query, get_table_stats."""

import json

import pytest

from mysqltuner_mcp.tools.tools_performance import (
    AnalyzeQueryToolHandler,
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
)


@pytest.mark.asyncio
async def test_get_slow_queries(sql_driver):
    handler = GetSlowQueriesToolHandler(sql_driver)
    result = await handler.run_tool({"limit": 5})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert "queries" in data and "total_queries" in data
    assert data["total_queries"] >= 1


@pytest.mark.asyncio
async def test_analyze_query_select(sql_driver):
    handler = AnalyzeQueryToolHandler(sql_driver)
    result = await handler.run_tool({
        "query": "SELECT id, email FROM users WHERE email = 'user1@example.com'",
        "format": "json",
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["plan"] is not None
    assert data["analyze_mode"] is False


@pytest.mark.asyncio
async def test_get_table_stats(sql_driver):
    handler = TableStatsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert "tables" in data and "summary" in data
    table_names = {t["name"] for t in data["tables"]}
    assert {"users", "orders", "products"}.issubset(table_names)


@pytest.mark.asyncio
async def test_compare_explain_plans_returns_verdict(sql_driver):
    from mysqltuner_mcp.tools.tools_performance import CompareExplainPlansToolHandler
    handler = CompareExplainPlansToolHandler(sql_driver)
    result = await handler.run_tool({
        "query_a": "SELECT id FROM users WHERE id = 1",
        "query_b": "SELECT id FROM users WHERE email = 'user1@example.com'",
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["verdict"] in (
        "A is better", "B is better", "no significant difference"
    )
    assert isinstance(data["rationale"], list)


@pytest.mark.asyncio
async def test_get_table_io_hotspots(sql_driver):
    from mysqltuner_mcp.tools.tools_performance import TableIoHotspotsToolHandler
    handler = TableIoHotspotsToolHandler(sql_driver)
    result = await handler.run_tool({"limit": 10})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert "tables" in data and "summary" in data
    for t in data["tables"]:
        assert "schema" in t and "table" in t
        assert "total_read_latency_sec" in t
