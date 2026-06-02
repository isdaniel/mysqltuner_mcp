"""Integration tests for index tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_index import (
    IndexRecommendationsToolHandler,
    IndexStatsToolHandler,
    UnusedIndexesToolHandler,
)


@pytest.mark.asyncio
async def test_get_index_recommendations(sql_driver):
    handler = IndexRecommendationsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_find_unused_indexes_finds_seeded_unused_and_redundant(sql_driver):
    handler = UnusedIndexesToolHandler(sql_driver)
    result = await handler.run_tool({"min_size_mb": 0})
    data = json.loads(result[0].text)
    assert "error_type" not in data

    unused_names = {idx["index_name"] for idx in data.get("unused_indexes", [])}
    redundant_names = {r["redundant_index"] for r in data.get("redundant_indexes", [])}
    duplicate_names = {d["duplicate_index"] for d in data.get("duplicate_indexes", [])}
    seen = unused_names | redundant_names | duplicate_names
    assert "idx_created_at_unused" in seen or "idx_user_id_dup" in seen, (
        f"expected to find idx_created_at_unused or idx_user_id_dup; got {seen}"
    )


@pytest.mark.asyncio
async def test_get_index_stats(sql_driver):
    handler = IndexStatsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
