"""
Base tool handler class for MCP tools.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool


class ToolHandler(ABC):
    """
    Abstract base class for MCP tool handlers.

    All tool implementations should inherit from this class and implement
    the required methods.
    """

    # Tool metadata - should be overridden in subclasses
    name: str = ""
    title: str = ""
    description: str = ""

    # Tool behavior hints
    read_only_hint: bool = True
    destructive_hint: bool = False
    idempotent_hint: bool = True
    open_world_hint: bool = False

    @abstractmethod
    def get_tool_definition(self) -> Tool:
        """
        Get the MCP Tool definition for this handler.

        Returns:
            Tool object describing this tool's interface
        """
        pass

    @abstractmethod
    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """
        Execute the tool with the given arguments.

        Args:
            arguments: Dictionary of tool arguments

        Returns:
            Sequence of TextContent results
        """
        pass

    def get_annotations(self) -> dict[str, Any]:
        """
        Get tool annotations for MCP.

        Returns:
            Dictionary of tool annotations
        """
        return {
            "title": self.title,
            "readOnlyHint": self.read_only_hint,
            "destructiveHint": self.destructive_hint,
            "idempotentHint": self.idempotent_hint,
            "openWorldHint": self.open_world_hint,
        }

    def format_json_result(self, data: Any, indent: int = 2) -> Sequence[TextContent]:
        """
        Format a result as JSON TextContent.

        Args:
            data: Data to serialize as JSON
            indent: JSON indentation level

        Returns:
            Sequence containing a single TextContent with JSON
        """
        return [
            TextContent(
                type="text",
                text=json.dumps(data, indent=indent, default=str)
            )
        ]

    def format_text_result(self, text: str) -> Sequence[TextContent]:
        """
        Format a result as plain text TextContent.

        Args:
            text: Text content

        Returns:
            Sequence containing a single TextContent
        """
        return [
            TextContent(
                type="text",
                text=text
            )
        ]

    def format_error(self, error: Exception) -> Sequence[TextContent]:
        """
        Format an error as TextContent.

        Args:
            error: Exception to format

        Returns:
            Sequence containing error TextContent
        """
        return [
            TextContent(
                type="text",
                text=json.dumps({
                    "error": str(error),
                    "type": type(error).__name__
                }, indent=2)
            )
        ]

    def validate_required_args(
        self,
        arguments: dict[str, Any],
        required: list[str]
    ) -> None:
        """
        Validate that required arguments are present.

        Args:
            arguments: Provided arguments
            required: List of required argument names

        Raises:
            ValueError: If a required argument is missing
        """
        missing = [arg for arg in required if arg not in arguments]
        if missing:
            raise ValueError(f"Missing required arguments: {', '.join(missing)}")
