from datetime import datetime

from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools import google_tools
from packages.core.tools.google_tools import (
    calendar_list_logged,
    calendar_list_upcoming,
    gmail_list_unread,
)


def test_google_tools_not_connected(tmp_path, monkeypatch):
    token_path = tmp_path / "google_tokens.json"
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", str(token_path))

    gmail = gmail_list_unread()
    assert gmail["error"] == "google_not_connected"

    calendar = calendar_list_upcoming()
    assert calendar["error"] == "google_not_connected"


def test_calendar_create_event_normalizes_time_only(monkeypatch):
    captured = {}

    def fake_create_event(summary, start_iso, end_iso, description=None):
        captured["summary"] = summary
        captured["start_iso"] = start_iso
        captured["end_iso"] = end_iso
        captured["description"] = description
        return {"id": "evt"}

    monkeypatch.setattr(google_tools, "create_event", fake_create_event)

    response = google_tools.calendar_create_event(
        summary="Go Bowling",
        start_iso="5:30pm",
        end_iso="7:30pm",
        description="Bowling at Portland Bowling.",
    )

    today = datetime.now().date().isoformat()
    assert response["status"] == "ok"
    assert captured["start_iso"].startswith(f"{today}T17:30:00")
    assert captured["end_iso"].startswith(f"{today}T19:30:00")


def test_calendar_create_event_logs_event(tmp_path, monkeypatch):
    db_path = tmp_path / "lists.db"
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(db_path))

    def fake_create_event(summary, start_iso, end_iso, description=None):
        return {
            "id": "evt-123",
            "summary": summary,
            "description": description,
            "htmlLink": "https://example.com/event",
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
        }

    monkeypatch.setattr(google_tools, "create_event", fake_create_event)

    response = google_tools.calendar_create_event(
        summary="Go Bowling",
        start_iso="2026-02-01T17:30:00-05:00",
        end_iso="2026-02-01T19:30:00-05:00",
        description="Bowling at Portland Bowling.",
    )

    assert response["status"] == "ok"
    assert response["logged"] is True

    store = SQLiteListStore(db_path=str(db_path))
    logged = store.get_calendar_event("evt-123")
    assert logged is not None
    assert logged.summary == "Go Bowling"

    logged_events = calendar_list_logged(limit=5)
    assert logged_events["status"] == "ok"
    assert any(event["event_id"] == "evt-123" for event in logged_events["events"])
