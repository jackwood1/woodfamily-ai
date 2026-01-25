from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from .list_tools import add_item, create_list, get_list
from ..storage.base import ListStore


ToolHandler = Callable[[Dict[str, Any]], Dict[str, Any]]


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    schema: Dict[str, Any]
    handler: ToolHandler


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, ToolDefinition] = {}

    def register(self, tool: ToolDefinition) -> None:
        self._tools[tool.name] = tool

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return [tool.schema for tool in self._tools.values()]

    def call(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name not in self._tools:
            return {"status": "error", "error": "unknown_tool", "tool_name": name}
        return self._tools[name].handler(args)

    def list_tools(self) -> List[str]:
        return list(self._tools.keys())


def build_list_tool_registry(store: ListStore) -> ToolRegistry:
    registry = ToolRegistry()

    registry.register(
        ToolDefinition(
            name="create_list",
            description="Create a named list.",
            schema={
                "type": "function",
                "function": {
                    "name": "create_list",
                    "description": "Create a named list.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                        },
                        "required": ["name"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: create_list(store, name=args["name"]),
        )
    )

    registry.register(
        ToolDefinition(
            name="add_item",
            description="Add an item to a list.",
            schema={
                "type": "function",
                "function": {
                    "name": "add_item",
                    "description": "Add an item to a list.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "list_name": {"type": "string"},
                            "item": {"type": "string"},
                        },
                        "required": ["list_name", "item"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: add_item(
                store, list_name=args["list_name"], item=args["item"]
            ),
        )
    )

    registry.register(
        ToolDefinition(
            name="get_list",
            description="Get all items in a list.",
            schema={
                "type": "function",
                "function": {
                    "name": "get_list",
                    "description": "Get all items in a list.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "list_name": {"type": "string"},
                        },
                        "required": ["list_name"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: get_list(store, list_name=args["list_name"]),
        )
    )

    return registry
