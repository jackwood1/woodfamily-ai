from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except Exception:  # pragma: no cover - optional dependency resolution
    FastAPIInstrumentor = None

from apps.api.routes.chat import router as chat_router
from apps.api.observability import init_observability


logging.basicConfig(level=logging.INFO)

init_observability()
app = FastAPI(title="Home Ops Copilot API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
if FastAPIInstrumentor is not None:
    FastAPIInstrumentor.instrument_app(app)
else:
    logging.getLogger("home_ops.api").warning(
        "OpenTelemetry instrumentation not available. "
        "Install observability dependencies to enable tracing."
    )
app.include_router(chat_router)
