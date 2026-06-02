"""Warmup queries to populate performance_schema with predictable digests.

Run once per session after seed.sql is loaded. Issues a handful of
queries that exercise:
- a slow query (so get_slow_queries has at least one entry)
- a full table scan (so get_statements_with_full_scans has an entry)
- a sort + group-by (so get_statements_with_sorting has an entry)
- normal indexed reads (so digests for users/orders show up)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mysqltuner_mcp.services import SqlDriver


WARMUP_SQL = [
    # Slow query - SLEEP causes timer wait, satisfies get_slow_queries
    "SELECT SLEEP(0.3)",
    # Full table scan on products.category (no index)
    "SELECT id, name FROM products WHERE category = 'books'",
    "SELECT id, name FROM products WHERE category = 'electronics'",
    # Sort + group-by, likely temp table
    "SELECT user_id, COUNT(*) AS c FROM orders GROUP BY user_id, status ORDER BY c DESC LIMIT 10",
    # Normal indexed reads
    "SELECT id, email FROM users WHERE id = 1",
    "SELECT id, email FROM users WHERE id = 50",
    "SELECT id, email FROM users WHERE email = 'user1@example.com'",
    "SELECT o.id, o.total FROM orders o WHERE o.user_id = 1 AND o.status = 'shipped'",
    "SELECT o.id, o.total FROM orders o WHERE o.user_id = 50 AND o.status = 'delivered'",
    # A query that examines many rows but returns few (efficiency_ratio data)
    "SELECT COUNT(*) FROM orders WHERE total > 100",
]


async def run_warmup(driver: "SqlDriver") -> None:
    """Execute warmup queries against the seeded database."""
    for sql in WARMUP_SQL:
        try:
            await driver.execute_query(sql)
        except Exception:
            # warmup is best-effort; downstream tests assert on what we got
            pass
