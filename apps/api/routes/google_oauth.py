from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse

from packages.core.google.oauth import (
    build_auth_url,
    delete_tokens,
    exchange_code,
    fetch_userinfo,
    generate_state,
    load_tokens,
    save_tokens,
    validate_state,
)


router = APIRouter(prefix="/api/integrations/google", tags=["google"])


@router.get("/start")
def start_oauth():
    if not os.getenv("GOOGLE_CLIENT_ID") or not os.getenv("GOOGLE_REDIRECT_URI"):
        raise HTTPException(status_code=400, detail="Google OAuth not configured")
    state = generate_state()
    url = build_auth_url(state)
    return RedirectResponse(url)


@router.get("/callback")
def oauth_callback(code: str, state: str):
    if not validate_state(state):
        raise HTTPException(status_code=400, detail="Invalid state")

    if not os.getenv("GOOGLE_CLIENT_ID") or not os.getenv("GOOGLE_CLIENT_SECRET"):
        raise HTTPException(status_code=400, detail="Google OAuth not configured")

    token_response = exchange_code(code)
    access_token = token_response.get("access_token")
    refresh_token = token_response.get("refresh_token")
    expires_in = token_response.get("expires_in", 3600)
    scope = token_response.get("scope", "")
    scopes = scope.split() if scope else []

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing access token")

    email = None
    subject = None
    try:
        userinfo = fetch_userinfo(access_token)
        email = userinfo.get("email")
        subject = userinfo.get("sub")
    except Exception:
        pass

    existing = load_tokens()
    if not refresh_token and existing:
        refresh_token = existing.refresh_token

    if not refresh_token:
        raise HTTPException(status_code=400, detail="Missing refresh token")

    save_tokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
        scopes=scopes,
        email=email,
        subject=subject,
    )

    redirect_url = os.getenv("GOOGLE_OAUTH_SUCCESS_REDIRECT", "http://localhost:3000")
    return RedirectResponse(f"{redirect_url}?google=connected")


@router.get("/status")
def status():
    tokens = load_tokens()
    if not tokens:
        return {"connected": False}
    return {
        "connected": True,
        "email": tokens.email,
        "scopes": tokens.scopes,
        "expiry": tokens.expiry,
    }


@router.post("/disconnect")
def disconnect():
    delete_tokens()
    return {"connected": False}
