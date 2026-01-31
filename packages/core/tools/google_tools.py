from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from packages.core.google.calendar import (
    create_event,
    delete_event,
    list_events,
    list_upcoming,
    update_event,
)
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
                    "recurrence": event.recurrence,
                    "source": event.source,
                }
                for event in events
            ],
        }
    except Exception as exc:
        return {"status": "error", "error": "log_read_failed", "message": str(exc)}


def calendar_find_events(
    query: Optional[str] = None,
    from_iso: Optional[str] = None,
    to_iso: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    try:
        events = list_events(limit=limit, query=query, from_iso=from_iso, to_iso=to_iso)
        return {"status": "ok", "events": events}
    except GoogleNotConnected:
        return _not_connected()


def calendar_create_event(
    summary: str,
    start_iso: str,
    end_iso: str,
    description: Optional[str] = None,
    recurrence: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    try:
        normalized_start = _normalize_calendar_datetime(start_iso)
        normalized_end = _normalize_calendar_datetime(end_iso)
        recurrence_rules = _normalize_recurrence(recurrence)
        conflict = _check_calendar_conflict(normalized_start, normalized_end)
        if conflict:
            return {
                "status": "error",
                "error": "calendar_conflict",
                "conflicts": conflict,
            }
        event = create_event(
            summary=summary,
            start_iso=normalized_start,
            end_iso=normalized_end,
            description=description,
            recurrence=recurrence_rules,
        )
        logged, log_error = _log_calendar_event(event)
        response = {"status": "ok", "event": event, "logged": logged}
        if log_error:
            response["log_error"] = log_error
        return response
    except GoogleNotConnected:
        return _not_connected()


def calendar_update_event(
    event_id: str,
    summary: Optional[str] = None,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    description: Optional[str] = None,
    recurrence: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    try:
        normalized_start = _normalize_calendar_datetime(start_iso) if start_iso else None
        normalized_end = _normalize_calendar_datetime(end_iso) if end_iso else None
        recurrence_rules = _normalize_recurrence(recurrence)
        event = update_event(
            event_id=event_id,
            summary=summary,
            start_iso=normalized_start,
            end_iso=normalized_end,
            description=description,
            recurrence=recurrence_rules,
        )
        logged, log_error = _log_calendar_event(event)
        response = {"status": "ok", "event": event, "logged": logged}
        if log_error:
            response["log_error"] = log_error
        return response
    except GoogleNotConnected:
        return _not_connected()


def calendar_delete_event(event_id: str) -> Dict[str, Any]:
    try:
        delete_event(event_id)
        store = _event_store()
        store.delete_calendar_event(event_id)
        return {"status": "ok", "event_id": event_id}
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


def _normalize_recurrence(value: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        return value or None
    raw = value.strip()
    if not raw:
        return []
    if raw.upper().startswith("RRULE:"):
        return [raw]
    if "FREQ=" in raw.upper():
        return [f"RRULE:{raw}"]
    return [raw]


def _check_calendar_conflict(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    start_dt = _parse_iso_datetime(start_iso)
    end_dt = _parse_iso_datetime(end_iso)
    if not start_dt or not end_dt:
        return []
    if end_dt <= start_dt:
        return []
    query_start = start_dt.isoformat()
    query_end = (end_dt + timedelta(seconds=1)).isoformat()
    events = list_events(limit=10, from_iso=query_start, to_iso=query_end)
    conflicts = []
    for event in events:
        ev_start = _event_datetime(event.get("start") or {})
        ev_end = _event_datetime(event.get("end") or {})
        if not ev_start or not ev_end:
            continue
        if ev_start < end_dt and ev_end > start_dt:
            conflicts.append(
                {
                    "id": event.get("id"),
                    "summary": event.get("summary"),
                    "start": event.get("start"),
                    "end": event.get("end"),
                    "htmlLink": event.get("htmlLink"),
                }
            )
    return conflicts


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def _event_datetime(payload: Dict[str, Any]) -> Optional[datetime]:
    date_time = payload.get("dateTime")
    if date_time:
        return _parse_iso_datetime(date_time)
    date_only = payload.get("date")
    if date_only:
        return _parse_iso_datetime(f"{date_only}T00:00:00")
    return None


def _log_calendar_event(event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    event_id = event.get("id")
    if not event_id:
        return False, "missing_event_id"
    start = (event.get("start") or {}).get("dateTime") or ""
    end = (event.get("end") or {}).get("dateTime") or ""
    summary = event.get("summary") or ""
    description = event.get("description")
    html_link = event.get("htmlLink")
    recurrence = event.get("recurrence")
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
                recurrence=recurrence,
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
