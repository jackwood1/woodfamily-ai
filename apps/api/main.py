from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except Exception:  # pragma: no cover - optional dependency resolution
    FastAPIInstrumentor = None

from apps.api.routes.bowling import router as bowling_router
from apps.api.routes.chat import router as chat_router
from apps.api.routes.calendar import router as calendar_router
from apps.api.routes.gmail import router as gmail_router
from apps.api.routes.debug import router as debug_router
from apps.api.routes.google_calendar import router as google_calendar_router
from apps.api.routes.google_oauth import router as google_oauth_router
from apps.api.routes.reminders import router as reminders_router
from apps.api.routes.threads import router as threads_router
from apps.api.reminders_scheduler import start_scheduler
from packages.core.storage.sqlite import SQLiteListStore
from apps.api.observability import init_observability
from packages.core.logging_config import configure_logging


configure_logging()

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
app.include_router(calendar_router)
app.include_router(bowling_router)
app.include_router(gmail_router)
app.include_router(debug_router)
app.include_router(google_calendar_router)
app.include_router(google_oauth_router)
app.include_router(reminders_router)
app.include_router(threads_router)

_SCHEDULER = None


@app.on_event("startup")
def _start_reminder_scheduler() -> None:
    global _SCHEDULER
    if os.getenv("REMINDERS_SCHEDULER_ENABLED", "true").lower() != "true":
        return
    if _SCHEDULER is not None:
        return
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    db_path = os.path.join(data_dir, "lists.db")
    store = SQLiteListStore(db_path=db_path)
    _SCHEDULER = start_scheduler(store)
