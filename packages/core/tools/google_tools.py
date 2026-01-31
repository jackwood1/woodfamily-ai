from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, Optional

from packages.core.google.calendar import create_event, list_upcoming
from packages.core.google.gmail import GoogleNotConnected, get_message, list_messages
from packages.core.storage.base import CalendarEventState
from packages.core.storage.sqlite import SQLiteListStore


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


def calendar_list_logged(limit: int = 20) -> Dict[str, Any]:
    try:
        store = _event_store()
        events = store.list_calendar_events(limit=limit)
        return {
            "status": "ok",
            "events": [
                {
                    "event_id": event.event_id,
                    "summary": event.summary,
                    "start_iso": event.start_iso,
                    "end_iso": event.end_iso,
                    "description": event.description,
                    "html_link": event.html_link,
                    "source": event.source,
                }
                for event in events
            ],
        }
    except Exception as exc:
        return {"status": "error", "error": "log_read_failed", "message": str(exc)}


def calendar_create_event(
    summary: str, start_iso: str, end_iso: str, description: Optional[str] = None
) -> Dict[str, Any]:
    try:
        normalized_start = _normalize_calendar_datetime(start_iso)
        normalized_end = _normalize_calendar_datetime(end_iso)
        event = create_event(
            summary=summary,
            start_iso=normalized_start,
            end_iso=normalized_end,
            description=description,
        )
        logged, log_error = _log_calendar_event(event)
        response = {"status": "ok", "event": event, "logged": logged}
        if log_error:
            response["log_error"] = log_error
        return response
    except GoogleNotConnected:
        return _not_connected()


_TIME_ONLY_RE = re.compile(
    r"^(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?\s*(?P<ampm>am|pm)?(?P<tz>Z|[+-]\d{2}:\d{2})?$",
    re.IGNORECASE,
)


def _normalize_calendar_datetime(value: str) -> str:
    value = value.strip()
    if not value:
        return value
    if "T" in value:
        date_part, time_part = value.split("T", 1)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_part):
            return value
        return f"{_today_date()}T{time_part}"
    match = _TIME_ONLY_RE.match(value)
    if not match:
        return value
    time_part = _normalize_time_match(match)
    return f"{_today_date()}T{time_part}"


def _normalize_time_match(match: re.Match) -> str:
    hour = int(match.group("hour"))
    minute = match.group("minute")
    second = match.group("second") or "00"
    ampm = (match.group("ampm") or "").lower()
    tz = match.group("tz") or ""
    if ampm == "pm" and hour < 12:
        hour += 12
    if ampm == "am" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute}:{second}{tz}"


def _today_date() -> str:
    return datetime.now().date().isoformat()


def _log_calendar_event(event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    event_id = event.get("id")
    if not event_id:
        return False, "missing_event_id"
    start = (event.get("start") or {}).get("dateTime") or ""
    end = (event.get("end") or {}).get("dateTime") or ""
    summary = event.get("summary") or ""
    description = event.get("description")
    html_link = event.get("htmlLink")
    timestamp = datetime.now().isoformat()
    try:
        store = _event_store()
        store.upsert_calendar_event(
            CalendarEventState(
                event_id=event_id,
                summary=summary,
                start_iso=start,
                end_iso=end,
                description=description,
                html_link=html_link,
                source="google",
                created_at=timestamp,
                updated_at=timestamp,
            ),
            raw_payload=event,
        )
        return True, None
    except Exception as exc:
        return False, str(exc)


def _event_store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    default_path = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_path)
    return SQLiteListStore(db_path=db_path)
