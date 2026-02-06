from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter

from apps.api.schemas.hints import (
    HintCreateRequest,
    HintDeleteResponse,
    HintListResponse,
    HintResponse,
    HintUpdateRequest,
)
from packages.core.bowling.hints import (
    add_bowling_hint,
    list_bowling_hints,
    remove_bowling_hint,
)
from packages.core.storage.sqlite import SQLiteListStore


router = APIRouter(prefix="/api/hints", tags=["hints"])


def _store() -> SQLiteListStore:
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "lists.db")
    return SQLiteListStore(db_path=db_path)


@router.post("", response_model=HintResponse)
def create(payload: HintCreateRequest) -> HintResponse:
    result = add_bowling_hint(_store(), payload.hint_type, payload.value)
    return HintResponse(hint_type=result["hint_type"], value=result["value"])


@router.get("", response_model=HintListResponse)
def list_all(hint_type: Optional[str] = None) -> HintListResponse:
    result = list_bowling_hints(_store(), hint_type=hint_type)
    hints = [
        HintResponse(hint_type=hint["hint_type"], value=hint["value"])
        for hint in result.get("hints", [])
    ]
    return HintListResponse(status="ok", hints=hints)


@router.put("", response_model=HintResponse)
def update(payload: HintUpdateRequest) -> HintResponse:
    store = _store()
    remove_bowling_hint(store, payload.hint_type, payload.value)
    result = add_bowling_hint(store, payload.hint_type, payload.new_value)
    return HintResponse(hint_type=result["hint_type"], value=result["value"])


@router.delete("", response_model=HintDeleteResponse)
def delete(hint_type: str, value: str) -> HintDeleteResponse:
    result = remove_bowling_hint(_store(), hint_type, value)
    return HintDeleteResponse(
        status="ok",
        removed=result["removed"],
        hint_type=result["hint_type"],
        value=result["value"],
    )
