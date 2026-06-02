"""Integration tests for security tools."""

import json

import pytest

from mysqltuner_mcp.tools.tools_security import (
    AuditLogToolHandler,
    SecurityAnalysisToolHandler,
    UserPrivilegesToolHandler,
)


@pytest.mark.asyncio
async def test_analyze_security(sql_driver):
    handler = SecurityAnalysisToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_analyze_user_privileges(sql_driver):
    handler = UserPrivilegesToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data


@pytest.mark.asyncio
async def test_check_audit_log(sql_driver):
    handler = AuditLogToolHandler(sql_driver)
    result = await handler.run_tool({})
    data = json.loads(result[0].text)
    assert "error_type" not in data
