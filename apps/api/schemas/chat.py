from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    reply: str
    tool_calls: List[Dict[str, Any]]
