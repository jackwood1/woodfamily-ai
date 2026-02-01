from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from packages.core.storage.sqlite import SQLiteListStore


router = APIRouter(prefix="/api/debug", tags=["debug"])


def _is_debug_enabled() -> bool:
    return os.getenv("HOME_OPS_DEBUG", "false").lower() == "true"


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(base_dir, "data")
    db_path = os.getenv("HOME_OPS_DB_PATH", os.path.join(data_dir, "lists.db"))
    return SQLiteListStore(db_path=db_path)


@router.get("/db")
def db_snapshot(limit: int = 100) -> Dict[str, List[Dict[str, Any]]]:
    if not _is_debug_enabled():
        raise HTTPException(status_code=404, detail="not_found")
    return _store().debug_snapshot(limit=limit)


class DebugSqlRequest(BaseModel):
    query: str


@router.post("/sql")
def run_sql(payload: DebugSqlRequest) -> Dict[str, Any]:
    if not _is_debug_enabled():
        raise HTTPException(status_code=404, detail="not_found")
    query = payload.query.strip()
    if not _is_readonly_query(query):
        raise HTTPException(status_code=400, detail="readonly_queries_only")
    rows = _store().debug_query(query)
    return {"rows": rows}


def _is_readonly_query(query: str) -> bool:
    lowered = query.lstrip().lower()
    return lowered.startswith("select") or lowered.startswith("pragma")
