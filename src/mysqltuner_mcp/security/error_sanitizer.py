"""Strip credentials, hostnames, and SQL fragments from client-visible errors.

Policy:
- SqlGuardError, ValueError, KeyError: messages are our own, controlled,
  and intended for the client. Echo verbatim.
- pymysql.err.MySQLError (any subclass): replace with a generic message;
  full text goes to the server log only.
- Anything else: same generic-message policy.

Every response includes a short trace_id so operators can correlate
the redacted client message with the full server-side log entry.
"""

from __future__ import annotations

import logging
import uuid

import pymysql.err

from .sql_guard import SqlGuardError

logger = logging.getLogger("mysqltuner_mcp")

_ECHO_EXCEPTIONS: tuple[type[BaseException], ...] = (
    SqlGuardError, ValueError, KeyError,
)


def sanitize_error(exc: BaseException) -> dict[str, str]:
    """Return a dict safe to return to the MCP client.

    Logs the full exception (with the trace_id) at ERROR.
    """
    trace_id = uuid.uuid4().hex[:8]
    error_type = type(exc).__name__

    if isinstance(exc, _ECHO_EXCEPTIONS):
        message = str(exc)
    elif isinstance(exc, pymysql.err.MySQLError):
        message = "Database error (see server logs)"
    else:
        message = "Internal error (see server logs)"

    logger.error(
        "tool error trace_id=%s type=%s detail=%r",
        trace_id, error_type, str(exc),
        exc_info=True,
    )

    return {
        "error_type": error_type,
        "message": message,
        "trace_id": trace_id,
    }
