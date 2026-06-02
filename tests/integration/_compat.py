"""Version-aware skip predicates for the integration suite.

Usage:
    from tests.integration._compat import requires_mysql

    @requires_mysql(min_version="8.0.18")
    async def test_explain_analyze_works(mysql_version, sql_driver):
        ...

Compares the active container's SELECT VERSION() string (e.g. "8.0.36"
or "5.7.44-log") against a minimum semver triple.
"""

from __future__ import annotations

import functools
import re
from typing import Callable

import pytest


_SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")


def _parse(version: str) -> tuple[int, int, int]:
    m = _SEMVER_RE.match(version)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _meets(actual: str, minimum: str) -> bool:
    return _parse(actual) >= _parse(minimum)


def requires_mysql(min_version: str) -> Callable:
    """Decorator that skips the test if the active server is older than min_version.

    The decorated test MUST request the `mysql_version` fixture as a kwarg.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            mv = kwargs.get("mysql_version")
            if mv is None:
                raise RuntimeError(
                    f"{func.__name__} uses @requires_mysql but does not request "
                    f"the mysql_version fixture"
                )
            if not _meets(mv, min_version):
                pytest.skip(f"requires MySQL >= {min_version} (running {mv})")
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def requires_topology(_reason: str) -> Callable:
    """Decorator that always skips with a topology-related reason.

    Used for replication / Galera / group-replication tools that need
    multi-node topology, which the single-container fixture does not provide.
    """

    def decorator(func):
        return pytest.mark.skip(reason=f"requires multi-node topology: {_reason}")(func)

    return decorator
