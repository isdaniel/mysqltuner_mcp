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


class TestStringLiteralBypassRegression:
    r"""Regression tests for the state-machine comment stripper.

    Earlier regex-based stripper (`/\*.*?\*/`) was bypassable by hiding
    a comment-open inside a string literal, e.g.
        SELECT '/*'; DROP TABLE x; /* */
    The regex matched from the inside-string `/*` to the trailing `*/`,
    erasing the DROP. The state machine respects string literals.
    """

    def test_block_open_in_single_quote_does_not_strip(self):
        # The earlier stripper turned this into one statement; must now
        # be detected as multi-statement.
        _bad("SELECT '/*'; DROP TABLE users; /* */",
             match="Only one statement")

    def test_block_open_in_double_quote_does_not_strip(self):
        _bad('SELECT "/*"; DROP TABLE users; /* */',
             match="Only one statement")

    def test_block_open_in_backtick_does_not_strip(self):
        _bad("SELECT `/*`; DROP TABLE users; /* */",
             match="Only one statement")

    def test_line_comment_marker_in_string_does_not_split(self):
        # `--` inside a string must not split the statement
        _ok("SELECT 'foo -- bar'")

    def test_hash_in_string_does_not_split(self):
        _ok("SELECT 'foo # bar'")

    def test_semicolon_in_string_does_not_split(self):
        # `;` inside a string literal must NOT split
        _ok("SELECT 'a;b;c' AS x")

    def test_escaped_quote_in_string_does_not_terminate(self):
        # `''` and `\'` are both single-quote escapes; the statement
        # remains a single SELECT.
        _ok("SELECT 'it''s fine'")
        _ok("SELECT 'it\'s fine'")

    def test_unterminated_block_comment_consumes_rest(self):
        # Unterminated /* is a strong injection signal; reject outright.
        _bad("SELECT 1 /* never closed", match="Unterminated")

    def test_real_multi_statement_outside_strings_still_rejected(self):
        _bad("SELECT 1; SELECT 2", match="Only one statement")


class TestNoBackslashEscapesBypassRegression:
    r"""Regression tests for the backslash-escape parser-mismatch bypass.

    When the MySQL server runs with NO_BACKSLASH_ESCAPES enabled, the
    backslash is a literal character. A payload like
        SELECT 'abc\'; DROP TABLE users; --'
    is parsed as TWO statements by the server. An earlier version of
    the guard honoured \ as an in-string escape, saw the payload as
    one statement, and let the smuggled DROP through.

    The current state machine intentionally does NOT honour \ escapes;
    only the mode-independent doubled-quote (`''`, `""`, `` `` ``)
    escapes are recognized.
    """

    def test_backslash_quote_does_not_escape_in_single_quoted(self):
        # Backslash before the closing quote must NOT extend the string;
        # the DROP must be visible as a second statement.
        q = "SELECT 'abc" + chr(92) + "'; DROP TABLE users; --'"
        _bad(q, match="Only one statement")

    def test_backslash_quote_does_not_escape_in_double_quoted(self):
        q = 'SELECT "abc' + chr(92) + '"; DROP TABLE users; --"'
        _bad(q, match="Only one statement")


class TestMySQLExecutableCommentBypassRegression:
    """Regression tests for the /*! ... */ executable-comment bypass.

    MySQL conditionally executes the contents of `/*! ... */` and
    `/*!NNNNN ... */` comments (parsed-as-SQL when the server version
    matches the optional gate). A naive comment stripper that treats
    them as regular block comments lets an attacker smuggle DDL/DML
    past this guard. We reject any /*! outright.
    """

    def test_executable_comment_after_select_rejected(self):
        _bad("SELECT 1; /*!50001 DROP TABLE users */",
             match="executable comments")

    def test_standalone_executable_comment_rejected(self):
        _bad("/*!50001 DROP TABLE users */ SELECT 1",
             match="executable comments")

    def test_executable_comment_embedded_in_select_rejected(self):
        _bad("SELECT 1 /*!50001 ; DROP TABLE users */",
             match="executable comments")

    def test_executable_comment_no_version_gate_rejected(self):
        _bad("/*! DROP TABLE x */ SELECT 1",
             match="executable comments")

    def test_regular_block_comment_still_allowed(self):
        # Sanity: a NON-executable block comment is still fine
        _ok("SELECT 1 /* regular comment */")
