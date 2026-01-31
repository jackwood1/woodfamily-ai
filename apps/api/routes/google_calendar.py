from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from apps.api.schemas.calendar import CalendarCreateRequest
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
def create(payload: CalendarCreateRequest) -> Dict[str, Any]:
    try:
        return create_event(
            summary=payload.summary,
            start_iso=payload.start,
            end_iso=payload.end,
            description=payload.description,
        )
    except GoogleNotConnected:
        raise HTTPException(status_code=400, detail="google_not_connected")
