from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class HintCreateRequest(BaseModel):
    hint_type: str
    value: str


class HintUpdateRequest(BaseModel):
    hint_type: str
    value: str
    new_value: str


class HintResponse(BaseModel):
    hint_type: str
    value: str


class HintListResponse(BaseModel):
    status: str
    hints: List[HintResponse]


class HintDeleteResponse(BaseModel):
    status: str
    removed: bool
    hint_type: str
    value: str


class HintQuery(BaseModel):
    hint_type: Optional[str] = None
