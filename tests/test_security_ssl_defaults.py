"""Tests for SSL identity-verification default behaviour."""

from mysqltuner_mcp.services.db_pool import DbConnPool


def test_ssl_on_defaults_identity_verify_true():
    pool = DbConnPool.from_uri("mysql://u:p@h:3306/d?ssl=true")
    assert pool.ssl_enabled is True
    assert pool.ssl_verify_identity is True


def test_ssl_on_explicit_identity_verify_false_wins():
    pool = DbConnPool.from_uri(
        "mysql://u:p@h:3306/d?ssl=true&ssl_verify_identity=false"
    )
    assert pool.ssl_enabled is True
    assert pool.ssl_verify_identity is False


def test_ssl_off_leaves_identity_verify_default_false():
    pool = DbConnPool.from_uri("mysql://u:p@h:3306/d")
    assert pool.ssl_enabled is False
    assert pool.ssl_verify_identity is False


def test_ssl_enabled_alias_also_triggers_default_flip():
    pool = DbConnPool.from_uri("mysql://u:p@h:3306/d?ssl_enabled=true")
    assert pool.ssl_enabled is True
    assert pool.ssl_verify_identity is True


import os
from unittest.mock import patch

from mysqltuner_mcp.server import ServerConfig


def test_env_ssl_on_defaults_identity_verify_true():
    with patch.dict(os.environ, {
        "MYSQL_URI": "mysql://u:p@h:3306/d",
        "MYSQL_SSL": "true",
    }, clear=True):
        cfg = ServerConfig.from_env()
        assert cfg.ssl_enabled is True
        assert cfg.ssl_verify_identity is True


def test_env_explicit_off_wins():
    with patch.dict(os.environ, {
        "MYSQL_URI": "mysql://u:p@h:3306/d",
        "MYSQL_SSL": "true",
        "MYSQL_SSL_VERIFY_IDENTITY": "false",
    }, clear=True):
        cfg = ServerConfig.from_env()
        assert cfg.ssl_enabled is True
        assert cfg.ssl_verify_identity is False
