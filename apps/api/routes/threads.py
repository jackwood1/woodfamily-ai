from __future__ import annotations

import os
from typing import List

from fastapi import APIRouter, HTTPException

from apps.api.schemas.threads import ThreadDetailResponse, ThreadSummaryResponse
from packages.core.storage.sqlite import SQLiteListStore


router = APIRouter(prefix="/threads", tags=["threads"])


def _store() -> SQLiteListStore:
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "lists.db")
    return SQLiteListStore(db_path=db_path)


@router.get("", response_model=List[ThreadSummaryResponse])
def list_threads(limit: int = 20) -> List[ThreadSummaryResponse]:
    threads = _store().list_threads(limit=limit)
    return [
        ThreadSummaryResponse(thread_id=thread.thread_id, summary=thread.summary)
        for thread in threads
    ]


@router.get("/{thread_id}", response_model=ThreadDetailResponse)
def get_thread(thread_id: str) -> ThreadDetailResponse:
    thread = _store().get_thread(thread_id)
    if thread is None:
        raise HTTPException(status_code=404, detail="thread_not_found")
    return ThreadDetailResponse(
        thread_id=thread.thread_id,
        summary=thread.summary,
        recent_messages=thread.recent_messages,
    )
