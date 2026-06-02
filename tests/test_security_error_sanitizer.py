"""Tests for security.error_sanitizer."""

import re

import pymysql.err
import pytest

from mysqltuner_mcp.security.error_sanitizer import sanitize_error
from mysqltuner_mcp.security.sql_guard import SqlGuardError


TRACE_RE = re.compile(r"^[0-9a-f]{8}$")


def test_sql_guard_error_message_echoed():
    out = sanitize_error(SqlGuardError("Only one statement is allowed"))
    assert out["error_type"] == "SqlGuardError"
    assert out["message"] == "Only one statement is allowed"
    assert TRACE_RE.match(out["trace_id"])


def test_value_error_message_echoed():
    out = sanitize_error(ValueError("Invalid SQL identifier"))
    assert out["error_type"] == "ValueError"
    assert out["message"] == "Invalid SQL identifier"
    assert TRACE_RE.match(out["trace_id"])


def test_key_error_message_echoed():
    out = sanitize_error(KeyError("missing_arg"))
    assert out["error_type"] == "KeyError"
    assert "missing_arg" in out["message"]
    assert TRACE_RE.match(out["trace_id"])


def test_mysql_operational_error_redacted():
    secret = "(1045, \"Access denied for user 'root'@'1.2.3.4' (using password: YES)\")"
    exc = pymysql.err.OperationalError(secret)
    out = sanitize_error(exc)
    assert out["error_type"] == "OperationalError"
    assert out["message"] == "Database error (see server logs)"
    assert "root" not in out["message"]
    assert "1.2.3.4" not in out["message"]
    assert TRACE_RE.match(out["trace_id"])


def test_arbitrary_exception_redacted():
    out = sanitize_error(RuntimeError("connection string mysql://root:secret@db/"))
    assert out["error_type"] == "RuntimeError"
    assert out["message"] == "Internal error (see server logs)"
    assert "secret" not in out["message"]
    assert TRACE_RE.match(out["trace_id"])


def test_trace_ids_are_unique():
    a = sanitize_error(ValueError("x"))["trace_id"]
    b = sanitize_error(ValueError("x"))["trace_id"]
    assert a != b


def test_logs_full_exception_with_trace_id(caplog):
    import logging
    caplog.set_level(logging.ERROR, logger="mysqltuner_mcp")
    out = sanitize_error(RuntimeError("internal secret"))
    assert any(
        out["trace_id"] in record.message and "internal secret" in record.message
        for record in caplog.records
    )
