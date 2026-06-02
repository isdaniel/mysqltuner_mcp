"""Fail fast if warmup didn't populate performance_schema."""

import pytest


@pytest.mark.asyncio
async def test_perf_schema_has_orders_digest(sql_driver):
    count = await sql_driver.execute_scalar(
        "SELECT COUNT(*) FROM performance_schema.events_statements_summary_by_digest "
        "WHERE DIGEST_TEXT LIKE '%FROM `orders`%' "
        "   OR DIGEST_TEXT LIKE '%FROM orders%'"
    )
    assert count > 0, "warmup_queries did not register any orders digests"


@pytest.mark.asyncio
async def test_perf_schema_has_sleep_digest(sql_driver):
    count = await sql_driver.execute_scalar(
        "SELECT COUNT(*) FROM performance_schema.events_statements_summary_by_digest "
        "WHERE DIGEST_TEXT LIKE '%SLEEP%'"
    )
    assert count > 0, "warmup SLEEP query did not register a digest"
