from __future__ import annotations

import logging

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from apps.api.routes.chat import router as chat_router
from apps.api.observability import init_observability


logging.basicConfig(level=logging.INFO)

init_observability()
app = FastAPI(title="Home Ops Copilot API")
FastAPIInstrumentor.instrument_app(app)
app.include_router(chat_router)
