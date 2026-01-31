from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from packages.core.google.calendar import create_event, list_upcoming
from packages.core.google.gmail import GoogleNotConnected


router = APIRouter(prefix="/api/integrations/calendar", tags=["calendar"])


@router.get("/upcoming")
def upcoming(limit: int = 10, from_iso: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        return list_upcoming(limit=limit, from_iso=from_iso)
    except GoogleNotConnected:
        raise HTTPException(status_code=400, detail="google_not_connected")


@router.post("/events")
def create(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return create_event(
            summary=payload.get("summary", ""),
            start_iso=payload.get("start_iso", ""),
            end_iso=payload.get("end_iso", ""),
            description=payload.get("description"),
        )
    except GoogleNotConnected:
        raise HTTPException(status_code=400, detail="google_not_connected")
