"""Security primitives for mysqltuner_mcp.

- identifiers.quote_ident: safely quote SQL identifiers.
- sql_guard.assert_safe_explain_target: validate user-supplied SQL passed to analyze_query.
- error_sanitizer.sanitize_error: strip credentials/SQL fragments from client-visible errors.
"""

from .error_sanitizer import sanitize_error
from .identifiers import IDENT_RE, quote_ident
from .sql_guard import SqlGuardError, assert_safe_explain_target

__all__ = [
    "IDENT_RE",
    "SqlGuardError",
    "assert_safe_explain_target",
    "quote_ident",
    "sanitize_error",
]
