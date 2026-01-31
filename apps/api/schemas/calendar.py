from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class CalendarEventsRequest(BaseModel):
    start: str = Field(..., description="ISO-8601 start datetime")
    end: str = Field(..., description="ISO-8601 end datetime")


class CalendarEventResponse(BaseModel):
    id: str
    title: str
    start: str
    end: str
    location: Optional[str]
    description: Optional[str]
