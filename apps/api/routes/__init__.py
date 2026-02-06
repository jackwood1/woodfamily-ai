from .bowling import router as bowling_router
from .chat import router as chat_router
from .calendar import router as calendar_router
from .hints import router as hints_router
from .gmail import router as gmail_router
from .google_calendar import router as google_calendar_router
from .google_oauth import router as google_oauth_router
from .reminders import router as reminders_router
from .threads import router as threads_router
from .debug import router as debug_router

__all__ = [
    "bowling_router",
    "chat_router",
    "calendar_router",
    "hints_router",
    "debug_router",
    "gmail_router",
    "google_calendar_router",
    "google_oauth_router",
    "reminders_router",
    "threads_router",
]
