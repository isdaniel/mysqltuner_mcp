"""Integration tests for statements tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_statements import (
    LongQueryTypeCollationIssuesToolHandler,
    StatementAnalysisToolHandler,
    StatementErrorsToolHandler,
    StatementsFullScansToolHandler,
    StatementsSortingToolHandler,
    StatementsTempTablesToolHandler,
)


@pytest.mark.asyncio
async def test_analyze_statements(sql_driver):
    handler = StatementAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_statements_with_temp_tables(sql_driver):
    handler = StatementsTempTablesToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_statements_with_sorting(sql_driver):
    handler = StatementsSortingToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_statements_with_full_scans(sql_driver):
    handler = StatementsFullScansToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_get_statements_with_errors(sql_driver):
    handler = StatementErrorsToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_long_queries_for_type_collation_issues(sql_driver):
    handler = LongQueryTypeCollationIssuesToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_find_temporary_table_spills_in_progress(sql_driver):
    from mysqltuner_mcp.tools.tools_statements import TempTableSpillsInProgressToolHandler
    handler = TempTableSpillsInProgressToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
    assert data["active_spills"] == []
    assert data["summary"]["count"] == 0
