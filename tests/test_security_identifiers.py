"""Tests for security.identifiers."""

import pytest

from mysqltuner_mcp.security.identifiers import IDENT_RE, quote_ident


class TestQuoteIdent:
    def test_simple_ascii(self):
        assert quote_ident("foo") == "`foo`"

    def test_with_underscore(self):
        assert quote_ident("foo_bar") == "`foo_bar`"

    def test_with_digits(self):
        assert quote_ident("col1") == "`col1`"

    def test_with_dollar_sign(self):
        assert quote_ident("foo$bar") == "`foo$bar`"

    def test_max_length_64(self):
        name = "a" * 64
        assert quote_ident(name) == f"`{name}`"

    def test_rejects_backtick(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("foo`bar")

    def test_rejects_null_byte(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("foo\x00bar")

    def test_rejects_single_quote(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("foo'bar")

    def test_rejects_double_quote(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident('foo"bar')

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("foo;DROP TABLE x")

    def test_rejects_space(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("foo bar")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("")

    def test_rejects_65_chars(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident("a" * 65)

    def test_rejects_non_string(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident(123)  # type: ignore[arg-type]

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_ident(None)  # type: ignore[arg-type]

    def test_error_message_does_not_echo_input(self):
        """Ensure malicious input is not reflected back in the error message."""
        bad = "foo`; DROP TABLE users; --"
        with pytest.raises(ValueError) as ei:
            quote_ident(bad)
        assert bad not in str(ei.value)


class TestIdentRe:
    def test_pattern_anchored(self):
        assert IDENT_RE.match("foo") is not None
        assert IDENT_RE.match("foo\nbar") is None  # newline rejected
