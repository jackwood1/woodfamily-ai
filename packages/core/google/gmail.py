from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional

import httpx

from .oauth import get_valid_access_token, load_tokens


class GoogleNotConnected(Exception):
    pass


GMAIL_BASE_URL = "https://gmail.googleapis.com/gmail/v1/users/me"


def _headers() -> Dict[str, str]:
    token = get_valid_access_token()
    if not token:
        raise GoogleNotConnected("google_not_connected")
    return {"Authorization": f"Bearer {token}"}


def list_messages(limit: int = 10, query: Optional[str] = None) -> List[Dict[str, Any]]:
    params = {"maxResults": limit}
    if query:
        params["q"] = query
    response = httpx.get(
        f"{GMAIL_BASE_URL}/messages", headers=_headers(), params=params, timeout=15
    )
    response.raise_for_status()
    return response.json().get("messages", [])


def get_message(message_id: str) -> Dict[str, Any]:
    response = httpx.get(
        f"{GMAIL_BASE_URL}/messages/{message_id}",
        headers=_headers(),
        params={"format": "full"},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    headers = payload.get("payload", {}).get("headers", [])
    header_map = {h.get("name", "").lower(): h.get("value") for h in headers}
    return {
        "id": payload.get("id"),
        "threadId": payload.get("threadId"),
        "from": header_map.get("from"),
        "subject": header_map.get("subject"),
        "date": header_map.get("date"),
        "snippet": payload.get("snippet"),
        "bodyText": _extract_body(payload.get("payload", {})),
    }


def _extract_body(payload: Dict[str, Any]) -> str:
    parts = payload.get("parts") or []
    if payload.get("body", {}).get("data"):
        return _decode(payload["body"]["data"])
    for part in parts:
        mime = part.get("mimeType", "")
        body = part.get("body", {}).get("data")
        if mime == "text/plain" and body:
            return _decode(body)
    for part in parts:
        body = part.get("body", {}).get("data")
        if body:
            return _decode(body)
    return ""


def _decode(data: str) -> str:
    padded = data.replace("-", "+").replace("_", "/")
    padded += "=" * ((4 - len(padded) % 4) % 4)
    return base64.b64decode(padded).decode("utf-8", errors="replace")
