"""Integration tests for replication tools.

All skipped: single-container fixture cannot provide a replication topology.
Unit tests with mocks already cover the response shape.
"""

import pytest

from tests.integration._compat import requires_topology


@pytest.mark.asyncio
@requires_topology("primary + replica MySQL pair")
async def test_get_replication_status(sql_driver):
    pass


@pytest.mark.asyncio
@requires_topology("Galera cluster (>=3 nodes)")
async def test_get_galera_status(sql_driver):
    pass


@pytest.mark.asyncio
@requires_topology("MySQL Group Replication cluster")
async def test_get_group_replication_status(sql_driver):
    pass
