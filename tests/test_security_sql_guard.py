"""Tests for security.sql_guard."""

import pytest

from mysqltuner_mcp.security.sql_guard import (
    SqlGuardError,
    assert_safe_explain_target,
)


def _ok(query: str, *, analyze: bool = False, confirm_write: bool = False) -> None:
    assert_safe_explain_target(query, analyze=analyze, confirm_write=confirm_write)


def _bad(query: str, *, analyze: bool = False, confirm_write: bool = False,
         match: str | None = None) -> None:
    with pytest.raises(SqlGuardError, match=match or ""):
        assert_safe_explain_target(query, analyze=analyze, confirm_write=confirm_write)


class TestAccepts:
    def test_simple_select(self):
        _ok("SELECT 1")

    def test_select_with_params(self):
        _ok("SELECT * FROM users WHERE id = %s")

    def test_cte(self):
        _ok("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte")

    def test_select_trailing_semicolon_and_whitespace(self):
        _ok("SELECT 1;   ")

    def test_update_without_analyze(self):
        _ok("UPDATE users SET active = 1 WHERE id = 1")

    def test_update_with_analyze_and_confirm(self):
        _ok("UPDATE users SET active = 1 WHERE id = 1",
            analyze=True, confirm_write=True)

    def test_delete_without_analyze(self):
        _ok("DELETE FROM users WHERE id = 1")

    def test_insert_without_analyze(self):
        _ok("INSERT INTO users (id) VALUES (1)")

    def test_replace_without_analyze(self):
        _ok("REPLACE INTO users (id) VALUES (1)")


class TestRejectsMultiStatement:
    def test_plain_two_statements(self):
        _bad("SELECT 1; SELECT 2", match="Only one statement")

    def test_block_comment_bypass(self):
        _bad("SELECT 1 /*;*/; DROP TABLE x", match="Only one statement")

    def test_line_comment_bypass_dash(self):
        _bad("SELECT 1 -- ;\nDROP TABLE x", match="Only one statement")

    def test_line_comment_bypass_hash(self):
        _bad("SELECT 1 # ;\nDROP TABLE x", match="Only one statement")

    def test_null_byte_separator(self):
        _bad("SELECT 1;\x00DROP TABLE x", match="Only one statement")


class TestRejectsDdl:
    @pytest.mark.parametrize("verb", [
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", "GRANT", "REVOKE",
    ])
    def test_ddl_verbs(self, verb: str):
        _bad(f"{verb} TABLE x (id INT)", match="not permitted")


class TestWriteRequiresConfirm:
    @pytest.mark.parametrize("query", [
        "UPDATE users SET x = 1",
        "DELETE FROM users",
        "INSERT INTO users (id) VALUES (1)",
        "REPLACE INTO users (id) VALUES (1)",
    ])
    def test_write_with_analyze_requires_confirm(self, query: str):
        _bad(query, analyze=True, confirm_write=False,
             match="confirm_write")

    def test_select_with_analyze_does_not_require_confirm(self):
        _ok("SELECT 1", analyze=True, confirm_write=False)


class TestRejectsEmpty:
    def test_empty(self):
        _bad("", match="empty")

    def test_whitespace_only(self):
        _bad("   \n\t  ", match="empty")

    def test_comment_only(self):
        _bad("/* just a comment */", match="empty")
