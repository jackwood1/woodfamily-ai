from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import APIRouter

from apps.api.schemas.chat import ChatRequest, ChatResponse
from packages.core.agent import HomeOpsAgent
from packages.core.llm.openai_client import OpenAIClient
from packages.core.storage.sqlite import SQLiteListStore


router = APIRouter()


def _build_agent() -> HomeOpsAgent:
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data")
    )
    db_path = os.path.join(data_dir, "lists.db")
    store = SQLiteListStore(db_path=db_path)
    llm = OpenAIClient()
    return HomeOpsAgent(store=store, llm=llm)


_AGENT = _build_agent()


@router.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest) -> Dict[str, Any]:
    return _AGENT.chat(payload.message)
