"""Pytest fixtures for the integration test suite.

Spawns a MySQL container (via testcontainers), loads seed.sql, runs warmup
queries, and yields a configured SqlDriver. Session-scoped so the container
is reused across all tests.

Parameterization:
- By default the fixture sweeps over MySQL 5.7, 8.0, and 8.4. Running all
  three locally is slow; for development use:
      MYSQL_TEST_IMAGE=mysql:8.0 pytest tests/integration -v
- CI sets MYSQL_TEST_IMAGE per matrix shard.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from testcontainers.mysql import MySqlContainer

from mysqltuner_mcp.services import DbConnPool, SqlDriver
from tests.integration.fixtures.warmup_queries import run_warmup


DEFAULT_IMAGES = ("mysql:5.7", "mysql:8.0", "mysql:8.4")


def _resolve_images() -> tuple[str, ...]:
    """Pick which MySQL image(s) the fixture parameterizes over.

    If MYSQL_TEST_IMAGE is set in the environment, use only that one
    (CI matrix shard mode). Otherwise sweep all defaults (local dev).
    """
    env = os.environ.get("MYSQL_TEST_IMAGE", "").strip()
    if env:
        return (env,)
    return DEFAULT_IMAGES


@pytest.fixture(scope="session", params=_resolve_images())
def mysql_container(request: pytest.FixtureRequest) -> Iterator[MySqlContainer]:
    """Start a MySQL container; tear down at session end."""
    image = request.param
    with MySqlContainer(image, username="test", password="test", dbname="test") as container:
        yield container


@pytest.fixture(scope="session")
def mysql_uri(mysql_container: MySqlContainer) -> str:
    """Return a mysql:// URI compatible with our DbConnPool.from_uri."""
    raw = mysql_container.get_connection_url()
    # Strip the +pymysql driver hint - our DbConnPool only accepts mysql://
    uri = raw.replace("mysql+pymysql://", "mysql://")
    # Grant the privileges the MCP tools need (performance_schema /
    # information_schema / PROCESS). Then load the seed.
    _grant_privileges(mysql_container)
    _load_seed(uri)
    return uri


def _grant_privileges(container: MySqlContainer) -> None:
    """Connect as root and grant perf/info_schema + PROCESS to the test user."""
    import pymysql

    conn = pymysql.connect(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(3306)),
        user="root",
        password=container.root_password,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            user = container.username
            # performance_schema is grantable; information_schema is virtual
            # and implicitly accessible — granting it raises ER_DBACCESS_DENIED.
            cur.execute(f"GRANT SELECT ON performance_schema.* TO '{user}'@'%'")
            cur.execute(f"GRANT PROCESS ON *.* TO '{user}'@'%'")
            # sys schema is built on top of performance_schema; some MCP
            # tools query it directly.
            try:
                cur.execute(f"GRANT SELECT ON sys.* TO '{user}'@'%'")
            except Exception:
                # sys schema may not exist on 5.7 minimal install
                pass
            # mysql.* read is needed by analyze_user_privileges and similar
            # tools that introspect the grants tables.
            cur.execute(f"GRANT SELECT ON mysql.* TO '{user}'@'%'")
            cur.execute("FLUSH PRIVILEGES")
    finally:
        conn.close()


def _load_seed(uri: str) -> None:
    """Load seed.sql synchronously via pymysql (one-shot, easier than aiomysql)."""
    import re
    import urllib.parse as up

    import pymysql

    seed_path = Path(__file__).parent / "fixtures" / "seed.sql"
    sql_text = seed_path.read_text(encoding="utf-8")

    # Strip line comments then split on ';'
    stripped = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in stripped.split(";") if s.strip()]

    p = up.urlparse(uri)
    conn = pymysql.connect(
        host=p.hostname,
        port=p.port or 3306,
        user=p.username,
        password=p.password or "",
        database=(p.path.lstrip("/") or "test"),
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


@pytest_asyncio.fixture
async def seeded_pool(mysql_uri: str) -> AsyncIterator[DbConnPool]:
    """Function-scoped DbConnPool bound to the current event loop.

    aiomysql pools are loop-bound, so we cannot session-scope a single pool
    across pytest-asyncio's per-test event loops. The container and seed
    are session-scoped; the pool is cheap to recreate per test.

    Warmup runs once per session via a module-level flag so digests
    accumulate in performance_schema across tests.
    """
    pool = DbConnPool.from_uri(mysql_uri, minsize=1, maxsize=5)
    await pool.initialize()
    driver = SqlDriver(pool)
    await _ensure_warmup(driver)
    try:
        yield pool
    finally:
        await pool.close()


_warmup_done = False


async def _ensure_warmup(driver: SqlDriver) -> None:
    """Run warmup queries once per session (digests persist server-side)."""
    global _warmup_done
    if _warmup_done:
        return
    await run_warmup(driver)
    _warmup_done = True


@pytest_asyncio.fixture
async def sql_driver(seeded_pool: DbConnPool) -> SqlDriver:
    """A SqlDriver wrapping the seeded pool."""
    return SqlDriver(seeded_pool)


@pytest_asyncio.fixture
async def mysql_version(sql_driver: SqlDriver) -> str:
    """SELECT VERSION() result for _compat skip predicates."""
    return await sql_driver.get_server_version()
