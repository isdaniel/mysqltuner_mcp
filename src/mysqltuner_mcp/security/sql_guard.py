"""Guard for user-supplied SQL fed to analyze_query.

Strategy:
1. Tokenize the query with a state machine that respects MySQL string
   literals (', ", `) so comment-strip and statement-split decisions
   honour quoted content.
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
- The earlier regex-based stripper (``/\\*.*?\\*/`` + ``--[^\\n]*``) was
  vulnerable to a string-literal bypass: e.g. `SELECT '/*'; DROP TABLE
  users; /* */` would have its middle stripped. The state machine below
  closes that gap.
"""

from __future__ import annotations

import re

ALLOWED_VERBS = frozenset({
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE",
})
WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE"})

_LEADING_VERB = re.compile(r"\A\s*([A-Za-z]+)")


class SqlGuardError(Exception):
    """Raised when a user-supplied SQL statement is not safe to pass to EXPLAIN."""


def _tokenize_and_split(query: str) -> list[str]:
    """Strip comments + split on statement terminators in a single pass.

    Respects MySQL string-literal quoting so that ``SELECT '/*'`` does
    not falsely trigger comment stripping, and ``SELECT ';'`` does not
    falsely trigger statement splitting.

    Recognized quote styles (matching MySQL behaviour):
      - single-quoted string ``'...'`` with `''` or `\\'` escapes
      - double-quoted string ``"..."`` with `""` or `\\"` escapes
      - backtick-quoted identifier `` `...` `` with `` `` `` escapes

    Recognized comment styles:
      - block ``/* ... */`` (NOT nested)
      - line ``-- ...\\n`` (MySQL also requires whitespace after `--`,
        but we accept any to stay conservative)
      - line ``# ...\\n``

    NUL (``\\x00``) is treated as a statement terminator defensively;
    some MySQL clients drop NUL-terminated buffers verbatim and a query
    containing ``\\x00`` should never be passed to EXPLAIN as one piece.

    Comments are replaced with a single space so adjacent tokens stay
    separated; semicolons inside comments do NOT split statements;
    semicolons inside string literals do NOT split statements.

    Returns the list of non-empty stripped statements.
    """
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(query)

    while i < n:
        ch = query[i]
        nxt = query[i + 1] if i + 1 < n else ""

        # Block comment
        if ch == "/" and nxt == "*":
            # MySQL "executable comments" (/*! ... */ and /*!NNNNN ... */)
            # are conditionally parsed by the server: the content INSIDE the
            # comment is treated as SQL when the server version matches the
            # optional version-number gate (or always when there's no gate).
            # Stripping them like a regular comment lets an attacker smuggle
            # DDL/DML past this guard, e.g. `/*!50001 DROP TABLE x */ SELECT 1`.
            # Reject outright — legitimate analyze_query callers do not need
            # them.
            if i + 2 < n and query[i + 2] == "!":
                raise SqlGuardError(
                    "MySQL executable comments (/*! ... */) are not permitted"
                )
            end = query.find("*/", i + 2)
            if end == -1:
                # Unterminated block comment is a strong signal of an
                # injection attempt — reject the whole query.
                raise SqlGuardError("Unterminated block comment")
            buf.append(" ")
            i = end + 2
            continue

        # Line comment '-- ...' — flush buffer as a completed statement
        # (treating the comment as a synthetic terminator). Defeats
        # `SELECT 1 -- ;\nDROP TABLE x` style smuggling: the DROP becomes
        # a separate statement and the guard rejects multi-statement input.
        if ch == "-" and nxt == "-":
            end = query.find("\n", i + 2)
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            if end == -1:
                i = n
            else:
                i = end + 1
            continue

        # Line comment '# ...' — same treatment as '-- ...'
        if ch == "#":
            end = query.find("\n", i + 1)
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            if end == -1:
                i = n
            else:
                i = end + 1
            continue

        # Statement terminators
        if ch == ";" or ch == "\x00":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        # String literals — copy verbatim, skipping comment/split logic inside.
        #
        # We intentionally do NOT honour backslash escapes (\\' / \\"). When the
        # MySQL server runs with NO_BACKSLASH_ESCAPES enabled, backslashes are
        # literal characters and a payload like `SELECT 'abc\\'; DROP TABLE x; --'`
        # is parsed as two statements server-side. If we treat \\' as an in-string
        # escape, our guard would see ONE statement and let the smuggled DROP
        # through. Sticking to doubled-quote escapes (`''`, `""`, ` `` `) is
        # mode-independent and safe in both NO_BACKSLASH_ESCAPES on/off; the cost
        # is that strings containing literal `\'` are over-conservatively split,
        # which can only false-reject valid queries (never false-accept).
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                c = query[i]
                # Doubled-quote escape: '' inside ', "" inside ", `` inside `
                if c == quote and i + 1 < n and query[i + 1] == quote:
                    buf.append(c)
                    buf.append(c)
                    i += 2
                    continue
                buf.append(c)
                i += 1
                if c == quote:
                    break
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def assert_safe_explain_target(
    query: str,
    *,
    analyze: bool,
    confirm_write: bool,
) -> None:
    """Validate user SQL before wrapping in EXPLAIN.

    Raises:
        SqlGuardError: with a stable, safe message describing the rejection.

    Note on backslashes
    -------------------
    We reject ANY query containing a backslash (``\\``). MySQL's handling
    of backslash escapes inside string literals depends on the server's
    ``NO_BACKSLASH_ESCAPES`` SQL mode: with the mode OFF (default) ``\\'``
    is a quote escape and continues the string; with the mode ON it's a
    literal character that terminates the string at the next ``'``. Any
    Python-side parser must pick one interpretation and is therefore
    vulnerable to a parser-mismatch bypass against servers running the
    other mode. Rejecting backslashes outright closes the entire mismatch
    class; the cost is that legitimate queries using ``\\`` (LIKE patterns
    with ``\\%``, REGEXP with ``\\d``, etc.) must be rewritten — for an
    analyze_query tool that's an acceptable trade.
    """
    if not isinstance(query, str):
        raise SqlGuardError("Query must be a string")

    if "\\" in query:
        raise SqlGuardError(
            "Backslashes are not permitted in queries "
            "(eliminates NO_BACKSLASH_ESCAPES parser-mismatch bypass)"
        )

    statements = _tokenize_and_split(query)

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
