from __future__ import annotations

import os
from typing import List, Optional

try:
    from mcp.server.fastmcp import FastMCP
except Exception as exc:  # pragma: no cover - optional dependency resolution
    FastMCP = None
    _MCP_IMPORT_ERROR = exc

from packages.core.calendar.client import CalendarEvent, default_google_client


def _build_mcp() -> "FastMCP":
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    return FastMCP("HomeOpsCalendarAgent", json_response=True)


mcp = _build_mcp()


@mcp.tool()
def list_events(start: str, end: str) -> List[dict]:
    """List calendar events between ISO-8601 start/end."""
    client = default_google_client()
    events = client.list_events(start, end)
    return [
        {
            "id": event.id,
            "title": event.title,
            "start": event.start,
            "end": event.end,
            "location": event.location,
            "description": event.description,
        }
        for event in events
    ]


@mcp.tool()
def get_event(event_id: str) -> Optional[dict]:
    """Get a calendar event by id."""
    client = default_google_client()
    event = client.get_event(event_id)
    if event is None:
        return None
    return {
        "id": event.id,
        "title": event.title,
        "start": event.start,
        "end": event.end,
        "location": event.location,
        "description": event.description,
    }


def main() -> None:
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
