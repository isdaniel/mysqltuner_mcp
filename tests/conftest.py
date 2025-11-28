"""
Pytest configuration and fixtures.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_pool():
    """Create a mock database pool."""
    pool = MagicMock()
    pool.acquire = MagicMock()

    # Create async context manager for acquire
    async_cm = AsyncMock()
    async_cm.__aenter__ = AsyncMock(return_value=MagicMock())
    async_cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = async_cm

    return pool


@pytest.fixture
def mock_sql_driver(mock_pool):
    """Create a mock SQL driver."""
    from mysqltuner_mcp.services import SqlDriver

    driver = SqlDriver(mock_pool)
    return driver


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    conn = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall = AsyncMock(return_value=[])
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.description = [("col1",), ("col2",)]
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock(return_value=None)

    conn.cursor = MagicMock(return_value=cursor)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)

    return conn, cursor
