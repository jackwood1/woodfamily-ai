from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from packages.core.google.gmail import GoogleNotConnected, get_message, list_messages


router = APIRouter(prefix="/api/integrations/gmail", tags=["gmail"])


@router.get("/unread")
def list_unread(limit: int = 10, query: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        q = query or "is:unread"
        return list_messages(limit=limit, query=q)
    except GoogleNotConnected:
        raise HTTPException(status_code=400, detail="google_not_connected")


@router.get("/messages/{message_id}")
def get_message_by_id(message_id: str) -> Dict[str, Any]:
    try:
        return get_message(message_id)
    except GoogleNotConnected:
        raise HTTPException(status_code=400, detail="google_not_connected")
