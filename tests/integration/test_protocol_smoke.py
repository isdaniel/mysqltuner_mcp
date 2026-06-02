"""End-to-end protocol smoke test.

Spawns `python -m mysqltuner_mcp --mode stdio` as a subprocess against
the seeded container, drives it with the mcp Python client library,
and verifies:
- the server boots
- list_tools returns 39 tools
- call_tool round-trips a structured response for one read-only tool
"""

from __future__ import annotations

import os
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


EXPECTED_TOOL_COUNT = 44


@pytest.mark.asyncio
async def test_stdio_server_lists_and_calls_tools(mysql_uri):
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mysqltuner_mcp", "--mode", "stdio"],
        env={**os.environ, "MYSQL_URI": mysql_uri},
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            assert len(tools.tools) == EXPECTED_TOOL_COUNT, (
                f"expected {EXPECTED_TOOL_COUNT} tools, got {len(tools.tools)}"
            )

            # Round-trip one tool that returns structured JSON quickly
            result = await session.call_tool("check_perf_schema_config", {})
            assert result.content, "tool returned empty content"
            text = result.content[0].text
            # Successful tool response should NOT have an error_type/Internal-error marker
            assert "Internal error" not in text
