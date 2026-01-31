from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.events",
    "openid",
    "email",
    "profile",
]


@dataclass
class GoogleTokens:
    access_token: str
    refresh_token: str
    expiry: int
    scopes: List[str]
    email: Optional[str]
    subject: Optional[str]


def _data_dir() -> str:
    return os.getenv("GOOGLE_DATA_DIR", "apps/api/data")


def _token_path() -> str:
    return os.getenv("GOOGLE_TOKEN_PATH", os.path.join(_data_dir(), "google_tokens.json"))


def _state_path() -> str:
    return os.getenv("GOOGLE_STATE_PATH", os.path.join(_data_dir(), "google_state.json"))


def _client_id() -> str:
    return os.getenv("GOOGLE_CLIENT_ID", "")


def _client_secret() -> str:
    return os.getenv("GOOGLE_CLIENT_SECRET", "")


def _redirect_uri() -> str:
    return os.getenv("GOOGLE_REDIRECT_URI", "")


def _scopes() -> List[str]:
    raw = os.getenv("GOOGLE_OAUTH_SCOPES", "")
    if raw:
        return raw.split()
    return DEFAULT_SCOPES


def _save_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def generate_state() -> str:
    state = uuid.uuid4().hex
    _save_json(_state_path(), {"state": state, "created_at": int(time.time())})
    return state


def validate_state(state: str) -> bool:
    stored = _load_json(_state_path())
    if not stored:
        return False
    return stored.get("state") == state


def build_auth_url(state: str) -> str:
    params = {
        "client_id": _client_id(),
        "redirect_uri": _redirect_uri(),
        "response_type": "code",
        "scope": " ".join(_scopes()),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return httpx.URL("https://accounts.google.com/o/oauth2/v2/auth").copy_add_params(
        params
    ).to_str()


def exchange_code(code: str) -> Dict[str, Any]:
    payload = {
        "code": code,
        "client_id": _client_id(),
        "client_secret": _client_secret(),
        "redirect_uri": _redirect_uri(),
        "grant_type": "authorization_code",
    }
    response = httpx.post(GOOGLE_TOKEN_URL, data=payload, timeout=15)
    response.raise_for_status()
    return response.json()


def refresh_access_token(refresh_token: str) -> Dict[str, Any]:
    payload = {
        "client_id": _client_id(),
        "client_secret": _client_secret(),
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    response = httpx.post(GOOGLE_TOKEN_URL, data=payload, timeout=15)
    response.raise_for_status()
    return response.json()


def fetch_userinfo(access_token: str) -> Dict[str, Any]:
    response = httpx.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def load_tokens() -> Optional[GoogleTokens]:
    payload = _load_json(_token_path())
    if not payload:
        return None
    return GoogleTokens(
        access_token=payload.get("access_token", ""),
        refresh_token=payload.get("refresh_token", ""),
        expiry=int(payload.get("expiry", 0)),
        scopes=payload.get("scopes", []),
        email=payload.get("email"),
        subject=payload.get("subject"),
    )


def save_tokens(
    access_token: str,
    refresh_token: str,
    expires_in: int,
    scopes: List[str],
    email: Optional[str] = None,
    subject: Optional[str] = None,
) -> None:
    expiry = int(time.time()) + int(expires_in)
    _save_json(
        _token_path(),
        {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expiry": expiry,
            "scopes": scopes,
            "email": email,
            "subject": subject,
        },
    )


def delete_tokens() -> None:
    path = _token_path()
    if os.path.exists(path):
        os.remove(path)


def get_valid_access_token() -> Optional[str]:
    tokens = load_tokens()
    if not tokens:
        return None
    if tokens.expiry > int(time.time()) + 60:
        return tokens.access_token
    if not tokens.refresh_token:
        return None
    refreshed = refresh_access_token(tokens.refresh_token)
    access_token = refreshed.get("access_token")
    expires_in = refreshed.get("expires_in", 3600)
    save_tokens(
        access_token=access_token,
        refresh_token=tokens.refresh_token,
        expires_in=expires_in,
        scopes=tokens.scopes,
        email=tokens.email,
        subject=tokens.subject,
    )
    return access_token
