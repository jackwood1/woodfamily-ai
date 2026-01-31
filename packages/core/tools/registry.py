from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from .list_tools import (
    add_item,
    clear_all_lists,
    clear_list,
    create_list,
    delete_list,
    get_list,
    list_lists,
    remove_item,
    update_item,
)
from .google_tools import (
    calendar_create_event,
    calendar_list_logged,
    calendar_list_upcoming,
    gmail_get_message,
    gmail_list_unread,
)
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

    def has_tool(self, name: str) -> bool:
        return name in self._tools

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
            name="gmail_list_unread",
            description="List unread Gmail messages.",
            schema={
                "type": "function",
                "function": {
                    "name": "gmail_list_unread",
                    "description": "List unread Gmail messages.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer"},
                            "query": {"type": "string"},
                        },
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: gmail_list_unread(
                limit=args.get("limit", 10),
                query=args.get("query"),
            ),
        )
    )

    registry.register(
        ToolDefinition(
            name="gmail_get_message",
            description="Get a Gmail message by id.",
            schema={
                "type": "function",
                "function": {
                    "name": "gmail_get_message",
                    "description": "Get a Gmail message by id.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "message_id": {"type": "string"},
                        },
                        "required": ["message_id"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: gmail_get_message(args["message_id"]),
        )
    )

    registry.register(
        ToolDefinition(
            name="calendar_list_upcoming",
            description="List upcoming calendar events.",
            schema={
                "type": "function",
                "function": {
                    "name": "calendar_list_upcoming",
                    "description": "List upcoming calendar events.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer"},
                            "from_iso": {"type": "string"},
                        },
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: calendar_list_upcoming(
                limit=args.get("limit", 10),
                from_iso=args.get("from_iso"),
            ),
        )
    )

    registry.register(
        ToolDefinition(
            name="calendar_list_logged",
            description="List logged calendar events created by the assistant.",
            schema={
                "type": "function",
                "function": {
                    "name": "calendar_list_logged",
                    "description": "List logged calendar events created by the assistant.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer"},
                        },
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: calendar_list_logged(limit=args.get("limit", 20)),
        )
    )

    registry.register(
        ToolDefinition(
            name="calendar_create_event",
            description="Create a calendar event.",
            schema={
                "type": "function",
                "function": {
                    "name": "calendar_create_event",
                    "description": "Create a calendar event.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "summary": {"type": "string"},
                            "start_iso": {"type": "string"},
                            "end_iso": {"type": "string"},
                            "description": {"type": "string"},
                        },
                        "required": ["summary", "start_iso", "end_iso"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: calendar_create_event(
                summary=args["summary"],
                start_iso=args["start_iso"],
                end_iso=args["end_iso"],
                description=args.get("description"),
            ),
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

    registry.register(
        ToolDefinition(
            name="list_lists",
            description="List all existing lists.",
            schema={
                "type": "function",
                "function": {
                    "name": "list_lists",
                    "description": "List all existing lists.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: list_lists(store),
        )
    )

    registry.register(
        ToolDefinition(
            name="delete_list",
            description="Delete a list and its items.",
            schema={
                "type": "function",
                "function": {
                    "name": "delete_list",
                    "description": "Delete a list and its items.",
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
            handler=lambda args: delete_list(store, list_name=args["list_name"]),
        )
    )

    registry.register(
        ToolDefinition(
            name="clear_list",
            description="Remove all items from a list.",
            schema={
                "type": "function",
                "function": {
                    "name": "clear_list",
                    "description": "Remove all items from a list.",
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
            handler=lambda args: clear_list(store, list_name=args["list_name"]),
        )
    )

    registry.register(
        ToolDefinition(
            name="clear_all_lists",
            description="Remove all lists and items.",
            schema={
                "type": "function",
                "function": {
                    "name": "clear_all_lists",
                    "description": "Remove all lists and items.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: clear_all_lists(store),
        )
    )

    registry.register(
        ToolDefinition(
            name="remove_item",
            description="Remove an item from a list.",
            schema={
                "type": "function",
                "function": {
                    "name": "remove_item",
                    "description": "Remove an item from a list.",
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
            handler=lambda args: remove_item(
                store, list_name=args["list_name"], item=args["item"]
            ),
        )
    )

    registry.register(
        ToolDefinition(
            name="update_item",
            description="Update an item in a list.",
            schema={
                "type": "function",
                "function": {
                    "name": "update_item",
                    "description": "Update an item in a list.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "list_name": {"type": "string"},
                            "old_item": {"type": "string"},
                            "new_item": {"type": "string"},
                        },
                        "required": ["list_name", "old_item", "new_item"],
                        "additionalProperties": False,
                    },
                },
            },
            handler=lambda args: update_item(
                store,
                list_name=args["list_name"],
                old_item=args["old_item"],
                new_item=args["new_item"],
            ),
        )
    )

    return registry
