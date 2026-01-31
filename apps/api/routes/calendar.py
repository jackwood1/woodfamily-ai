from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException

from apps.api.schemas.calendar import (
    CalendarCreateRequest,
    CalendarEventResponse,
    CalendarEventsRequest,
)
from packages.core.calendar.client import default_google_client


router = APIRouter(prefix="/calendar", tags=["calendar"])


@router.post("/events", response_model=List[CalendarEventResponse])
def list_events(payload: CalendarEventsRequest) -> List[CalendarEventResponse]:
    client = default_google_client()
    try:
        events = client.list_events(payload.start, payload.end)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [
        CalendarEventResponse(
            id=event.id,
            title=event.title,
            start=event.start,
            end=event.end,
            location=event.location,
            description=event.description,
        )
        for event in events
    ]


@router.get("/events/{event_id}", response_model=CalendarEventResponse)
def get_event(event_id: str) -> CalendarEventResponse:
    client = default_google_client()
    event = client.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return CalendarEventResponse(
        id=event.id,
        title=event.title,
        start=event.start,
        end=event.end,
        location=event.location,
        description=event.description,
    )


@router.post("/events", response_model=CalendarEventResponse)
def create_event(payload: CalendarCreateRequest) -> CalendarEventResponse:
    client = default_google_client()
    try:
        event = client.create_event(
            summary=payload.summary,
            start_iso=payload.start,
            end_iso=payload.end,
            description=payload.description,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CalendarEventResponse(
        id=event.id,
        title=event.title,
        start=event.start,
        end=event.end,
        location=event.location,
        description=event.description,
    )
