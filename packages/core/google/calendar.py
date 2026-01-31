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


def create_event(
    summary: str,
    start_iso: str,
    end_iso: str,
    description: Optional[str] = None,
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
    response = httpx.post(
        f"{CALENDAR_BASE_URL}/calendars/primary/events",
        headers=_headers(),
        json=payload,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def _ensure_timezone(value: str) -> str:
    if "T" not in value:
        return value
    _, time_part = value.split("T", 1)
    if "Z" in time_part or "+" in time_part or "-" in time_part:
        return value
    return f"{value}{DEFAULT_TZ_OFFSET}"
