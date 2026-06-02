"""End-to-end security tests against a real MySQL server.

Verify the security guards from sub-project #1 behave correctly when
exposed to a real MySQL backend (not just mocks).
"""

import json

import pytest

from mysqltuner_mcp.tools.tools_performance import AnalyzeQueryToolHandler


@pytest.mark.asyncio
async def test_analyze_query_rejects_ddl_without_side_effect(sql_driver):
    """A DROP TABLE attempt must be rejected by sql_guard AND not executed."""
    handler = AnalyzeQueryToolHandler(sql_driver)
    result = await handler.run_tool({"query": "DROP TABLE users"})
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"

    # Verify users table still exists with full row count
    count = await sql_driver.execute_scalar("SELECT COUNT(*) FROM users")
    assert count == 100, "users table was modified despite guard rejection"


@pytest.mark.asyncio
async def test_analyze_query_rejects_multi_statement_no_side_effect(sql_driver):
    handler = AnalyzeQueryToolHandler(sql_driver)
    result = await handler.run_tool({
        "query": "SELECT 1; DROP TABLE products"
    })
    data = json.loads(result[0].text)
    assert data["error_type"] == "SqlGuardError"

    count = await sql_driver.execute_scalar("SELECT COUNT(*) FROM products")
    assert count == 50


@pytest.mark.asyncio
async def test_analyze_query_returns_real_explain_for_select(sql_driver):
    """Sanity: a real SELECT should produce a real EXPLAIN plan."""
    handler = AnalyzeQueryToolHandler(sql_driver)
    result = await handler.run_tool({
        "query": "SELECT id FROM users WHERE email = 'user1@example.com'",
        "format": "json",
    })
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["plan"] is not None


@pytest.mark.asyncio
async def test_sanitizer_redacts_real_mysql_error(sql_driver):
    """A real MySQL ProgrammingError (bad table) must be redacted before reaching the caller."""
    handler = AnalyzeQueryToolHandler(sql_driver)
    result = await handler.run_tool({
        "query": "SELECT * FROM nonexistent_table_xyz"
    })
    data = json.loads(result[0].text)
    # Tool's own try/except formats via format_error → sanitizer.
    # The exception class varies (ProgrammingError, OperationalError, etc.).
    assert "error_type" in data
    assert data["message"] == "Database error (see server logs)"
    assert "nonexistent_table_xyz" not in data["message"]
    assert len(data["trace_id"]) == 8
