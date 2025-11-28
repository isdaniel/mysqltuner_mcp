"""
Entry point for running mysqltuner_mcp as a module.

Usage:
    python -m mysqltuner_mcp
"""

from .server import run

if __name__ == "__main__":
    run()
