"""Fail fast if the seed schema didn't load the expected row counts.

Every other integration test assumes these counts. If this test fails,
fix the seed before debugging anything else.
"""

import pytest


@pytest.mark.asyncio
async def test_users_count(sql_driver):
    count = await sql_driver.execute_scalar("SELECT COUNT(*) FROM users")
    assert count == 100


@pytest.mark.asyncio
async def test_orders_count(sql_driver):
    count = await sql_driver.execute_scalar("SELECT COUNT(*) FROM orders")
    assert count == 1000


@pytest.mark.asyncio
async def test_products_count(sql_driver):
    count = await sql_driver.execute_scalar("SELECT COUNT(*) FROM products")
    assert count == 50


@pytest.mark.asyncio
async def test_expected_indexes_exist(sql_driver):
    rows = await sql_driver.execute_query(
        "SELECT INDEX_NAME FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'"
    )
    names = {r["INDEX_NAME"] for r in rows}
    assert "idx_user_status" in names
    assert "idx_created_at_unused" in names
    assert "idx_user_id_dup" in names
