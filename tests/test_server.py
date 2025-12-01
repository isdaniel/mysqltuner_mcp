"""
Unit tests for the server module.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import os

from mcp.types import PromptReference, ResourceTemplateReference
from mysqltuner_mcp.server import ServerConfig, MySQLTunerServer


# Test URI for consistent testing
TEST_MYSQL_URI = "mysql://root:@localhost:3306/mysql"


class TestServerConfig:
    """Tests for ServerConfig class."""

    def test_init(self):
        """Test ServerConfig initialization."""
        config = ServerConfig(
            mysql_uri="mysql://root:secret@localhost:3306/testdb",
            pool_size=10
        )

        assert config.mysql_uri == "mysql://root:secret@localhost:3306/testdb"
        assert config.pool_size == 10

    def test_init_default_pool_size(self):
        """Test ServerConfig initialization with default pool size."""
        config = ServerConfig(mysql_uri="mysql://root:@localhost:3306/mysql")

        assert config.mysql_uri == "mysql://root:@localhost:3306/mysql"
        assert config.pool_size == 5

    def test_from_env_uri(self):
        """Test loading config from MYSQL_URI environment variable."""
        with patch.dict(os.environ, {
            "MYSQL_URI": "mysql://testuser:testpass@testhost:3307/testdb",
            "MYSQL_POOL_SIZE": "15"
        }, clear=True):
            config = ServerConfig.from_env()

            assert config.mysql_uri == "mysql://testuser:testpass@testhost:3307/testdb"
            assert config.pool_size == 15

    def test_from_env_default_pool_size(self):
        """Test loading config with default pool size."""
        with patch.dict(os.environ, {
            "MYSQL_URI": "mysql://root:@localhost:3306/mysql"
        }, clear=True):
            config = ServerConfig.from_env()

            assert config.mysql_uri == "mysql://root:@localhost:3306/mysql"
            assert config.pool_size == 5

    def test_from_env_missing_uri(self):
        """Test that missing MYSQL_URI raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ServerConfig.from_env()

            assert "MYSQL_URI environment variable is required" in str(exc_info.value)


class TestMySQLTunerServer:
    """Tests for MySQLTunerServer class."""

    def test_init(self):
        """Test server initialization."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)

        assert server.config == config
        assert server.server is not None
        assert server.db_pool is None
        assert server.sql_driver is None
        assert len(server.tools) == 0

    def test_get_prompts(self):
        """Test prompt definitions."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        prompts = server._get_prompts()

        assert len(prompts) == 4

        prompt_names = [p.name for p in prompts]
        assert "optimize_slow_query" in prompt_names
        assert "health_check" in prompt_names
        assert "index_review" in prompt_names
        assert "performance_audit" in prompt_names

    def test_get_resources(self):
        """Test resource definitions."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        resources = server._get_resources()

        assert len(resources) == 3

        # Convert URIs to strings for comparison (handles AnyUrl objects)
        uris = [str(r.uri) for r in resources]
        assert "mysql://tuner/best-practices" in uris
        assert "mysql://tuner/index-guidelines" in uris
        assert "mysql://tuner/configuration-guide" in uris

    @pytest.mark.asyncio
    async def test_read_resource_best_practices(self):
        """Test reading best practices resource."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        content = await server._read_resource("mysql://tuner/best-practices")

        assert "Best Practices" in content
        assert "Query Optimization" in content

    @pytest.mark.asyncio
    async def test_read_resource_index_guidelines(self):
        """Test reading index guidelines resource."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        content = await server._read_resource("mysql://tuner/index-guidelines")

        assert "Index" in content
        assert "Composite" in content

    @pytest.mark.asyncio
    async def test_read_resource_configuration_guide(self):
        """Test reading configuration guide resource."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        content = await server._read_resource("mysql://tuner/configuration-guide")

        assert "Configuration" in content
        assert "innodb_buffer_pool_size" in content

    @pytest.mark.asyncio
    async def test_read_resource_unknown(self):
        """Test reading unknown resource."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        content = await server._read_resource("mysql://tuner/unknown")

        assert "Unknown resource" in content

    @pytest.mark.asyncio
    async def test_get_prompt_result_optimize_slow_query(self):
        """Test getting optimize_slow_query prompt."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        result = await server._get_prompt_result(
            "optimize_slow_query",
            {"query": "SELECT * FROM users"}
        )

        assert result.description is not None
        assert len(result.messages) > 0
        assert "SELECT * FROM users" in result.messages[0].content.text

    @pytest.mark.asyncio
    async def test_get_prompt_result_health_check(self):
        """Test getting health_check prompt."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        result = await server._get_prompt_result("health_check", {})

        assert result.description is not None
        assert len(result.messages) > 0

    @pytest.mark.asyncio
    async def test_get_prompt_result_unknown(self):
        """Test getting unknown prompt."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        result = await server._get_prompt_result("unknown_prompt", {})

        assert "Unknown prompt" in result.messages[0].content.text

    @pytest.mark.asyncio
    async def test_initialize(self):
        """Test server initialization."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)

        mock_pool = MagicMock()
        mock_pool.initialize = AsyncMock()

        with patch("mysqltuner_mcp.server.DbConnPool.from_uri") as mock_from_uri:
            mock_from_uri.return_value = mock_pool

            await server.initialize()

            mock_from_uri.assert_called_once_with(
                TEST_MYSQL_URI,
                minsize=1,
                maxsize=5,
                ssl_verify_cert=True,
                ssl_verify_identity=False
            )
            assert server.db_pool is not None
            assert server.sql_driver is not None
            assert len(server.tools) == 30  # All 30 tools registered

    @pytest.mark.asyncio
    async def test_shutdown(self):
        """Test server shutdown."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)

        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()
        server.db_pool = mock_pool

        await server.shutdown()

        mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_completions_prompt_health_check(self):
        """Test completion for health_check prompt focus_area argument."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = PromptReference(type="ref/prompt", name="health_check")
        argument = {"name": "focus_area", "value": ""}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert "memory" in result.completion.values
        assert "connections" in result.completion.values
        assert "queries" in result.completion.values

    @pytest.mark.asyncio
    async def test_get_completions_prompt_health_check_partial(self):
        """Test completion with partial input filters results."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = PromptReference(type="ref/prompt", name="health_check")
        argument = {"name": "focus_area", "value": "mem"}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert "memory" in result.completion.values
        # Should not include values that don't start with "mem"
        assert "connections" not in result.completion.values

    @pytest.mark.asyncio
    async def test_get_completions_prompt_optimize_slow_query(self):
        """Test completion for optimize_slow_query prompt table_name argument."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = PromptReference(type="ref/prompt", name="optimize_slow_query")
        argument = {"name": "table_name", "value": ""}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert "users" in result.completion.values
        assert "orders" in result.completion.values

    @pytest.mark.asyncio
    async def test_get_completions_prompt_index_review(self):
        """Test completion for index_review prompt schema_name argument."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = PromptReference(type="ref/prompt", name="index_review")
        argument = {"name": "schema_name", "value": ""}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert "public" in result.completion.values

    @pytest.mark.asyncio
    async def test_get_completions_resource_template_empty(self):
        """Test completion for resource template returns empty results."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = ResourceTemplateReference(type="ref/resource", uri="mysql://tuner/{template}")
        argument = {"name": "template", "value": ""}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert len(result.completion.values) == 0

    @pytest.mark.asyncio
    async def test_get_completions_unknown_prompt(self):
        """Test completion for unknown prompt returns empty results."""
        config = ServerConfig(mysql_uri=TEST_MYSQL_URI)

        server = MySQLTunerServer(config)
        ref = PromptReference(type="ref/prompt", name="unknown_prompt")
        argument = {"name": "arg", "value": ""}

        result = await server._get_completions(ref, argument, None)

        assert result.completion is not None
        assert len(result.completion.values) == 0
