from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel


class ThreadSummaryResponse(BaseModel):
    thread_id: str
    summary: str


class ThreadDetailResponse(BaseModel):
    thread_id: str
    summary: str
    recent_messages: List[Dict[str, Any]]
