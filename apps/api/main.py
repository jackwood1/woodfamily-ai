from __future__ import annotations

import logging

from fastapi import FastAPI

from apps.api.routes.chat import router as chat_router


logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Home Ops Copilot API")
app.include_router(chat_router)
