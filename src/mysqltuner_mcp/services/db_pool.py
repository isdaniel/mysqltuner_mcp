"""Database connection pool for MySQL using aiomysql."""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any, Optional, Union

import aiomysql
from aiomysql import Pool

logger = logging.getLogger("mysqltuner_mcp")


class DbConnPool:
    """
    MySQL database connection pool wrapper using aiomysql.

    Provides async connection management with configurable pool size
    and connection parameters.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "",
        minsize: int = 1,
        maxsize: int = 10,
        charset: str = "utf8mb4",
        autocommit: bool = True,
        ssl_enabled: bool = False,
        ssl_ca: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        ssl_verify_cert: bool = True,
        ssl_verify_identity: bool = False,
    ):
        """
        Initialize the connection pool configuration.

        Args:
            host: MySQL server hostname
            port: MySQL server port
            user: Database username
            password: Database password
            database: Default database name
            minsize: Minimum number of connections in pool
            maxsize: Maximum number of connections in pool
            charset: Character set for connections
            autocommit: Enable autocommit mode
            ssl_enabled: Enable SSL/TLS connection
            ssl_ca: Path to CA certificate file
            ssl_cert: Path to client certificate file
            ssl_key: Path to client private key file
            ssl_verify_cert: Verify server certificate (default: True)
            ssl_verify_identity: Verify server hostname matches certificate (default: False)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.minsize = minsize
        self.maxsize = maxsize
        self.charset = charset
        self.autocommit = autocommit
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_verify_identity = ssl_verify_identity
        self._pool: Optional[Pool] = None

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> "DbConnPool":
        """
        Create a DbConnPool from a MySQL connection URI.

        URI format: mysql://user:password@host:port/database?ssl=true&ssl_ca=/path/to/ca.pem

        Supported query parameters for SSL:
            - ssl: Enable SSL (true/false/1/0)
            - ssl_ca: Path to CA certificate file
            - ssl_cert: Path to client certificate file
            - ssl_key: Path to client private key file
            - ssl_verify_cert: Verify server certificate (true/false, default: true)
            - ssl_verify_identity: Verify server hostname (true/false, default: false)

        Args:
            uri: MySQL connection URI
            **kwargs: Additional pool configuration options

        Returns:
            DbConnPool instance

        Raises:
            ValueError: If the URI is malformed or missing required components
        """
        import urllib.parse

        try:
            parsed = urllib.parse.urlparse(uri)
        except Exception as e:
            raise ValueError(f"Failed to parse URI: {e}") from e

        # Validate URI scheme
        if parsed.scheme not in ("mysql", "mysql+aiomysql"):
            raise ValueError(
                f"Invalid URI scheme: '{parsed.scheme}'. "
                f"Expected 'mysql' or 'mysql+aiomysql'. "
                f"URI format: mysql://user:password@host:port/database"
            )

        # Validate host is present
        if not parsed.hostname:
            raise ValueError(
                f"Missing host in URI: '{uri}'. "
                f"URI format: mysql://user:password@host:port/database"
            )

        # Validate port if specified
        if parsed.port is not None and not (1 <= parsed.port <= 65535):
            raise ValueError(
                f"Invalid port number: {parsed.port}. "
                f"Port must be between 1 and 65535."
            )

        # Validate database is present
        database = parsed.path.lstrip("/") if parsed.path else ""
        if not database:
            raise ValueError(
                f"Missing database name in URI: '{uri}'. "
                f"URI format: mysql://user:password@host:port/database"
            )

        # Parse query parameters for SSL configuration
        query_params = urllib.parse.parse_qs(parsed.query)

        def get_bool_param(name: str, default: bool = False) -> bool:
            """Parse a boolean query parameter."""
            values = query_params.get(name, [])
            if not values:
                return default
            val = values[0].lower()
            return val in ("true", "1", "yes", "on")

        def get_str_param(name: str, default: Optional[str] = None) -> Optional[str]:
            """Parse a string query parameter."""
            values = query_params.get(name, [])
            if not values:
                return default
            return urllib.parse.unquote(values[0])

        config = {
            "host": parsed.hostname,
            "port": parsed.port or 3306,
            "user": parsed.username or "root",
            "password": urllib.parse.unquote(parsed.password) if parsed.password else "",
            "database": database,
            "ssl_enabled": get_bool_param("ssl") or get_bool_param("ssl_enabled"),
            "ssl_ca": get_str_param("ssl_ca"),
            "ssl_cert": get_str_param("ssl_cert"),
            "ssl_key": get_str_param("ssl_key"),
            "ssl_verify_cert": get_bool_param("ssl_verify_cert", default=True),
            "ssl_verify_identity": get_bool_param("ssl_verify_identity", default=False),
        }
        config.update(kwargs)

        return cls(**config)

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """
        Create an SSL context for secure MySQL connections.

        Returns:
            SSLContext if SSL is enabled, None otherwise
        """
        if not self.ssl_enabled:
            return None

        # Create SSL context
        ssl_context = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=self.ssl_ca
        )

        # Configure certificate verification
        if not self.ssl_verify_cert:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.check_hostname = self.ssl_verify_identity

        # Load client certificate if provided
        if self.ssl_cert and self.ssl_key:
            ssl_context.load_cert_chain(
                certfile=self.ssl_cert,
                keyfile=self.ssl_key
            )
        elif self.ssl_cert:
            ssl_context.load_cert_chain(certfile=self.ssl_cert)

        return ssl_context

    async def initialize(self) -> None:
        """
        Initialize the connection pool.

        Creates the aiomysql pool with the configured parameters.
        """
        if self._pool is not None:
            return

        try:
            # Build connection kwargs
            conn_kwargs = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "password": self.password,
                "db": self.database,
                "minsize": self.minsize,
                "maxsize": self.maxsize,
                "charset": self.charset,
                "autocommit": self.autocommit,
            }

            # Add SSL context if enabled
            ssl_context = self._create_ssl_context()
            if ssl_context:
                conn_kwargs["ssl"] = ssl_context
                logger.info("SSL/TLS enabled for MySQL connection")

            self._pool = await aiomysql.create_pool(**conn_kwargs)
            logger.info(f"MySQL connection pool initialized: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to create MySQL connection pool: {str(e)}")
            raise

    async def close(self) -> None:
        """
        Close the connection pool.

        Gracefully closes all connections in the pool.
        """
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("MySQL connection pool closed")

    def get_pool(self) -> Pool:
        """
        Get the underlying aiomysql pool.

        Returns:
            The aiomysql Pool instance

        Raises:
            RuntimeError: If the pool is not initialized
        """
        if self._pool is None:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
        return self._pool

    async def acquire(self):
        """
        Acquire a connection from the pool.

        Returns:
            A context manager for the connection
        """
        if self._pool is None:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
        return self._pool.acquire()

    @property
    def size(self) -> int:
        """Get current pool size."""
        if self._pool is None:
            return 0
        return self._pool.size

    @property
    def freesize(self) -> int:
        """Get number of free connections in pool."""
        if self._pool is None:
            return 0
        return self._pool.freesize
