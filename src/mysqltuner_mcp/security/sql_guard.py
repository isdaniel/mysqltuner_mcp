"""Guard for user-supplied SQL fed to analyze_query.

Strategy:
1. Strip block comments and line comments (-- and #).
2. Reject if more than one non-empty statement remains.
3. Reject if leading verb is not in the allowlist.
4. If statement is a write AND analyze=True AND confirm_write=False, reject.

Notes:
- We do not try to parse SQL - we only enforce single-statement and verb
  allowlist after comment stripping. The downstream MySQL parser remains
  the authority on syntax.
- EXPLAIN itself does NOT execute the wrapped statement, even for writes.
- EXPLAIN ANALYZE (MySQL 8.0.18+) DOES execute. That is the only path
  that requires confirm_write for write verbs.
"""

from __future__ import annotations

import re

ALLOWED_VERBS = frozenset({
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE",
})
WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE"})

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_DASH = re.compile(r"--[^\n]*")
_LINE_COMMENT_HASH = re.compile(r"#[^\n]*")
_LEADING_VERB = re.compile(r"\A\s*([A-Za-z]+)")


class SqlGuardError(Exception):
    """Raised when a user-supplied SQL statement is not safe to pass to EXPLAIN."""


def _strip_comments(query: str) -> str:
    # Replace line comments with ';' so any statement-terminator hidden inside
    # a comment still produces a boundary (defeats `SELECT 1 -- ;\nDROP ...`
    # style bypasses). Block comments become a single space so adjacent
    # tokens stay separated but no new boundary is introduced.
    query = _BLOCK_COMMENT.sub(" ", query)
    query = _LINE_COMMENT_DASH.sub(";", query)
    query = _LINE_COMMENT_HASH.sub(";", query)
    return query


def _split_statements(query: str) -> list[str]:
    # Split on ';' AFTER comment stripping; treat NUL as a statement separator
    # too (defensive - some clients drop NUL-terminated buffers verbatim).
    parts = re.split(r"[;\x00]", query)
    return [p for p in (s.strip() for s in parts) if p]


def assert_safe_explain_target(
    query: str,
    *,
    analyze: bool,
    confirm_write: bool,
) -> None:
    """Validate user SQL before wrapping in EXPLAIN.

    Raises:
        SqlGuardError: with a stable, safe message describing the rejection.
    """
    if not isinstance(query, str):
        raise SqlGuardError("Query must be a string")

    stripped = _strip_comments(query)
    statements = _split_statements(stripped)

    if not statements:
        raise SqlGuardError("Query is empty after stripping comments")

    if len(statements) > 1:
        raise SqlGuardError("Only one statement is allowed")

    match = _LEADING_VERB.match(statements[0])
    if not match:
        raise SqlGuardError("Could not identify SQL verb")

    verb = match.group(1).upper()

    if verb not in ALLOWED_VERBS:
        raise SqlGuardError(
            f"Statement type '{verb}' is not permitted"
        )

    if analyze and verb in WRITE_VERBS and not confirm_write:
        raise SqlGuardError(
            "Write statement requires confirm_write=true when analyze=true"
        )
