"""
Unit tests for the services module.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from mysqltuner_mcp.services import DbConnPool, SqlDriver


class TestDbConnPool:
    """Tests for DbConnPool class."""

    def test_init(self):
        """Test DbConnPool initialization."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            minsize=1,
            maxsize=10
        )

        assert pool.host == "localhost"
        assert pool.port == 3306
        assert pool.user == "root"
        assert pool.password == "secret"
        assert pool.database == "testdb"
        assert pool.minsize == 1
        assert pool.maxsize == 10
        assert pool._pool is None

    def test_from_uri(self):
        """Test creating pool from URI."""
        pool = DbConnPool.from_uri("mysql://user:pass@myhost:3307/mydb")

        assert pool.host == "myhost"
        assert pool.port == 3307
        assert pool.user == "user"
        assert pool.password == "pass"
        assert pool.database == "mydb"

    def test_from_uri_with_ssl_enabled(self):
        """Test creating pool from URI with SSL enabled."""
        pool = DbConnPool.from_uri("mysql://user:pass@myhost:3307/mydb?ssl=true")

        assert pool.host == "myhost"
        assert pool.ssl_enabled is True
        assert pool.ssl_verify_cert is True  # default

    def test_from_uri_with_ssl_params(self):
        """Test creating pool from URI with full SSL configuration."""
        pool = DbConnPool.from_uri(
            "mysql://user:pass@myhost:3307/mydb"
            "?ssl=true&ssl_ca=/path/to/ca.pem"
            "&ssl_cert=/path/to/cert.pem&ssl_key=/path/to/key.pem"
            "&ssl_verify_cert=true&ssl_verify_identity=true"
        )

        assert pool.ssl_enabled is True
        assert pool.ssl_ca == "/path/to/ca.pem"
        assert pool.ssl_cert == "/path/to/cert.pem"
        assert pool.ssl_key == "/path/to/key.pem"
        assert pool.ssl_verify_cert is True
        assert pool.ssl_verify_identity is True

    def test_from_uri_with_ssl_verify_disabled(self):
        """Test creating pool from URI with SSL verification disabled."""
        pool = DbConnPool.from_uri(
            "mysql://user:pass@myhost:3307/mydb?ssl=true&ssl_verify_cert=false"
        )

        assert pool.ssl_enabled is True
        assert pool.ssl_verify_cert is False

    def test_init_with_ssl_params(self):
        """Test DbConnPool initialization with SSL parameters."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            ssl_enabled=True,
            ssl_ca="/path/to/ca.pem",
            ssl_cert="/path/to/cert.pem",
            ssl_key="/path/to/key.pem",
            ssl_verify_cert=True,
            ssl_verify_identity=True,
        )

        assert pool.ssl_enabled is True
        assert pool.ssl_ca == "/path/to/ca.pem"
        assert pool.ssl_cert == "/path/to/cert.pem"
        assert pool.ssl_key == "/path/to/key.pem"
        assert pool.ssl_verify_cert is True
        assert pool.ssl_verify_identity is True

    def test_create_ssl_context_disabled(self):
        """Test _create_ssl_context returns None when SSL is disabled."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql",
            ssl_enabled=False
        )

        ctx = pool._create_ssl_context()
        assert ctx is None

    def test_create_ssl_context_enabled(self):
        """Test _create_ssl_context creates context when SSL is enabled."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql",
            ssl_enabled=True,
            ssl_verify_cert=False
        )

        ctx = pool._create_ssl_context()
        assert ctx is not None
        import ssl
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.check_hostname is False

    def test_from_uri_defaults(self):
        """Test creating pool from URI with defaults."""
        pool = DbConnPool.from_uri("mysql://localhost/testdb")

        assert pool.host == "localhost"
        assert pool.port == 3306
        assert pool.user == "root"
        assert pool.password == ""
        assert pool.database == "testdb"

    def test_from_uri_with_encoded_password(self):
        """Test creating pool from URI with URL-encoded password."""
        # Password with special characters: p@ss:word/123
        pool = DbConnPool.from_uri("mysql://user:p%40ss%3Aword%2F123@localhost:3306/mydb")

        assert pool.password == "p@ss:word/123"

    def test_from_uri_invalid_scheme(self):
        """Test that invalid URI scheme raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DbConnPool.from_uri("postgresql://user:pass@localhost:5432/mydb")

        assert "Invalid URI scheme" in str(exc_info.value)
        assert "postgresql" in str(exc_info.value)

    def test_from_uri_missing_host(self):
        """Test that missing host raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DbConnPool.from_uri("mysql:///mydb")

        assert "Missing host" in str(exc_info.value)

    def test_from_uri_missing_database(self):
        """Test that missing database raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DbConnPool.from_uri("mysql://user:pass@localhost:3306")

        assert "Missing database" in str(exc_info.value)

    def test_from_uri_missing_database_trailing_slash(self):
        """Test that missing database with trailing slash raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DbConnPool.from_uri("mysql://user:pass@localhost:3306/")

        assert "Missing database" in str(exc_info.value)

    def test_from_uri_invalid_port(self):
        """Test that invalid port raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DbConnPool.from_uri("mysql://user:pass@localhost:99999/mydb")

        assert "port" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_initialize(self):
        """Test pool initialization."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        mock_aiomysql_pool = MagicMock()

        # Use AsyncMock with return_value for async function
        with patch("aiomysql.create_pool", new=AsyncMock(return_value=mock_aiomysql_pool)) as mock_create:
            await pool.initialize()

            mock_create.assert_called_once()
            assert pool._pool == mock_aiomysql_pool

    @pytest.mark.asyncio
    async def test_close(self):
        """Test pool close."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.close = MagicMock()
        mock_aiomysql_pool.wait_closed = AsyncMock()
        pool._pool = mock_aiomysql_pool

        await pool.close()

        mock_aiomysql_pool.close.assert_called_once()
        mock_aiomysql_pool.wait_closed.assert_called_once()

    def test_get_pool_not_initialized(self):
        """Test get_pool raises when not initialized."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        with pytest.raises(RuntimeError, match="not initialized"):
            pool.get_pool()

    def test_get_pool(self):
        """Test get_pool returns pool when initialized."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        mock_aiomysql_pool = MagicMock()
        pool._pool = mock_aiomysql_pool

        assert pool.get_pool() == mock_aiomysql_pool

    def test_size_not_initialized(self):
        """Test size property when not initialized."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        assert pool.size == 0

    def test_freesize_not_initialized(self):
        """Test freesize property when not initialized."""
        pool = DbConnPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="mysql"
        )

        assert pool.freesize == 0


class TestSqlDriver:
    """Tests for SqlDriver class."""

    def test_init(self):
        """Test SqlDriver initialization."""
        mock_pool = MagicMock()
        driver = SqlDriver(mock_pool)
        assert driver.pool == mock_pool

    @pytest.mark.asyncio
    async def test_execute_query(self):
        """Test execute_query method."""
        # Create async mocks that properly support async context manager
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ])
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        # cursor() returns the mock_cursor directly as an async context manager
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.execute_query("SELECT * FROM test")

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["name"] == "test2"

    @pytest.mark.asyncio
    async def test_execute_query_with_params(self):
        """Test execute_query with parameters."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[{"id": 1}])
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.execute_query("SELECT * FROM test WHERE id = %s", [1])

        mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %s", [1])

    @pytest.mark.asyncio
    async def test_execute_one(self):
        """Test execute_one method."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchone = AsyncMock(return_value={"id": 1, "name": "test"})
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.execute_one("SELECT * FROM test WHERE id = 1")

        assert result["id"] == 1
        assert result["name"] == "test"

    @pytest.mark.asyncio
    async def test_execute_scalar(self):
        """Test execute_scalar method."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchone = AsyncMock(return_value={"COUNT(*)": 42})
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.execute_scalar("SELECT COUNT(*) FROM test")

        # execute_scalar returns the first value of the first row
        assert result == 42

    @pytest.mark.asyncio
    async def test_execute_scalar_none(self):
        """Test execute_scalar returns None when no result."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchone = AsyncMock(return_value=None)
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.execute_scalar("SELECT value FROM missing")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_server_status(self):
        """Test get_server_status method."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[
            {"Variable_name": "Uptime", "Value": "12345"},
            {"Variable_name": "Threads_connected", "Value": "10"}
        ])
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.get_server_status()

        assert result["Uptime"] == "12345"
        assert result["Threads_connected"] == "10"

    @pytest.mark.asyncio
    async def test_get_server_variables(self):
        """Test get_server_variables method."""
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[
            {"Variable_name": "max_connections", "Value": "151"},
            {"Variable_name": "innodb_buffer_pool_size", "Value": "134217728"}
        ])
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_aiomysql_pool = MagicMock()
        mock_aiomysql_pool.acquire = MagicMock(return_value=mock_conn)

        mock_pool = MagicMock()
        mock_pool.get_pool = MagicMock(return_value=mock_aiomysql_pool)

        driver = SqlDriver(mock_pool)

        result = await driver.get_server_variables()

        assert result["max_connections"] == "151"
        assert result["innodb_buffer_pool_size"] == "134217728"
