from __future__ import annotations

from typing import Any, Dict, Optional

from packages.core.google.calendar import create_event, list_upcoming
from packages.core.google.gmail import GoogleNotConnected, get_message, list_messages


def _not_connected() -> Dict[str, Any]:
    return {"error": "google_not_connected", "message": "Please connect Google first."}


def gmail_list_unread(limit: int = 10, query: Optional[str] = None) -> Dict[str, Any]:
    try:
        q = query or "is:unread"
        messages = list_messages(limit=limit, query=q)
        return {"status": "ok", "messages": messages}
    except GoogleNotConnected:
        return _not_connected()


def gmail_get_message(message_id: str) -> Dict[str, Any]:
    try:
        message = get_message(message_id)
        return {"status": "ok", "message": message}
    except GoogleNotConnected:
        return _not_connected()


def calendar_list_upcoming(limit: int = 10, from_iso: Optional[str] = None) -> Dict[str, Any]:
    try:
        events = list_upcoming(limit=limit, from_iso=from_iso)
        return {"status": "ok", "events": events}
    except GoogleNotConnected:
        return _not_connected()


def calendar_create_event(
    summary: str, start_iso: str, end_iso: str, description: Optional[str] = None
) -> Dict[str, Any]:
    try:
        event = create_event(summary=summary, start_iso=start_iso, end_iso=end_iso, description=description)
        return {"status": "ok", "event": event}
    except GoogleNotConnected:
        return _not_connected()
