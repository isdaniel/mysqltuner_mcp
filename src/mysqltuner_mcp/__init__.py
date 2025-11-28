"""
MySQL Tuner MCP - A Model Context Protocol server for MySQL performance tuning.

Provides tools for:
- Query performance analysis
- Index recommendations
- Database health monitoring
- Configuration review
"""

from .server import MySQLTunerServer, ServerConfig, main, run

__version__ = "0.0.1"
__author__ = "DanielShih"
__email__ = "dog830228@gmail.com"

__all__ = [
    "MySQLTunerServer",
    "ServerConfig",
    "main",
    "run",
    "__version__",
]
