from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import httpx

from .gmail import GoogleNotConnected
from .oauth import get_valid_access_token


CALENDAR_BASE_URL = "https://www.googleapis.com/calendar/v3"
DEFAULT_TZ_OFFSET = os.getenv("CALENDAR_DEFAULT_TZ_OFFSET", "-05:00")


def _headers() -> Dict[str, str]:
    token = get_valid_access_token()
    if not token:
        raise GoogleNotConnected("google_not_connected")
    return {"Authorization": f"Bearer {token}"}


def list_upcoming(limit: int = 10, from_iso: Optional[str] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "maxResults": limit,
        "singleEvents": "true",
        "orderBy": "startTime",
    }
    if from_iso:
        params["timeMin"] = from_iso
    response = httpx.get(
        f"{CALENDAR_BASE_URL}/calendars/primary/events",
        headers=_headers(),
        params=params,
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("items", [])


def list_events(
    limit: int = 10,
    query: Optional[str] = None,
    from_iso: Optional[str] = None,
    to_iso: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "maxResults": limit,
        "singleEvents": "true",
        "orderBy": "startTime",
    }
    if query:
        params["q"] = query
    if from_iso:
        params["timeMin"] = from_iso
    if to_iso:
        params["timeMax"] = to_iso
    response = httpx.get(
        f"{CALENDAR_BASE_URL}/calendars/primary/events",
        headers=_headers(),
        params=params,
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("items", [])


def create_event(
    summary: str,
    start_iso: str,
    end_iso: str,
    description: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
) -> Dict[str, Any]:
    start_iso = _ensure_timezone(start_iso)
    end_iso = _ensure_timezone(end_iso)
    payload: Dict[str, Any] = {
        "summary": summary,
        "start": {"dateTime": start_iso},
        "end": {"dateTime": end_iso},
    }
    if description:
        payload["description"] = description
    if recurrence:
        payload["recurrence"] = recurrence
    response = httpx.post(
        f"{CALENDAR_BASE_URL}/calendars/primary/events",
        headers=_headers(),
        json=payload,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def update_event(
    event_id: str,
    summary: Optional[str] = None,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    description: Optional[str] = None,
    recurrence: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if summary is not None:
        payload["summary"] = summary
    if description is not None:
        payload["description"] = description
    if start_iso is not None:
        payload["start"] = {"dateTime": _ensure_timezone(start_iso)}
    if end_iso is not None:
        payload["end"] = {"dateTime": _ensure_timezone(end_iso)}
    if recurrence is not None:
        payload["recurrence"] = recurrence
    response = httpx.patch(
        f"{CALENDAR_BASE_URL}/calendars/primary/events/{event_id}",
        headers=_headers(),
        json=payload,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def delete_event(event_id: str) -> None:
    response = httpx.delete(
        f"{CALENDAR_BASE_URL}/calendars/primary/events/{event_id}",
        headers=_headers(),
        timeout=15,
    )
    response.raise_for_status()


def _ensure_timezone(value: str) -> str:
    if "T" not in value:
        return value
    _, time_part = value.split("T", 1)
    if "Z" in time_part or "+" in time_part or "-" in time_part:
        return value
    return f"{value}{DEFAULT_TZ_OFFSET}"
