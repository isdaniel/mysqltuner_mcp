"""Integration tests for health tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_health import (
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
    SettingsReviewToolHandler,
    WaitEventsToolHandler,
)


@pytest.mark.asyncio
async def test_check_database_health(sql_driver):
    handler = DatabaseHealthToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert "score" in data or "health_score" in data or "checks" in data


@pytest.mark.asyncio
async def test_get_active_queries(sql_driver):
    handler = ActiveQueriesToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_review_settings(sql_driver):
    handler = SettingsReviewToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_wait_events(sql_driver):
    handler = WaitEventsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
