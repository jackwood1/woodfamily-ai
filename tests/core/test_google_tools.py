from datetime import datetime

from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools import google_tools
from packages.core.tools.google_tools import (
    calendar_delete_event,
    calendar_find_events,
    calendar_list_logged,
    calendar_list_upcoming,
    calendar_update_event,
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

    def fake_create_event(summary, start_iso, end_iso, description=None, recurrence=None):
        captured["summary"] = summary
        captured["start_iso"] = start_iso
        captured["end_iso"] = end_iso
        captured["description"] = description
        return {"id": "evt"}

    monkeypatch.setattr(google_tools, "create_event", fake_create_event)
    monkeypatch.setattr(google_tools, "list_events", lambda *args, **kwargs: [])

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

    def fake_create_event(summary, start_iso, end_iso, description=None, recurrence=None):
        return {
            "id": "evt-123",
            "summary": summary,
            "description": description,
            "htmlLink": "https://example.com/event",
            "recurrence": recurrence,
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
        }

    monkeypatch.setattr(google_tools, "create_event", fake_create_event)
    monkeypatch.setattr(google_tools, "list_events", lambda *args, **kwargs: [])

    response = google_tools.calendar_create_event(
        summary="Go Bowling",
        start_iso="2026-02-01T17:30:00-05:00",
        end_iso="2026-02-01T19:30:00-05:00",
        description="Bowling at Portland Bowling.",
        recurrence="RRULE:FREQ=WEEKLY;BYDAY=MO",
    )

    assert response["status"] == "ok"
    assert response["logged"] is True

    store = SQLiteListStore(db_path=str(db_path))
    logged = store.get_calendar_event("evt-123")
    assert logged is not None
    assert logged.summary == "Go Bowling"
    assert logged.recurrence == ["RRULE:FREQ=WEEKLY;BYDAY=MO"]

    logged_events = calendar_list_logged(limit=5)
    assert logged_events["status"] == "ok"
    assert any(event["event_id"] == "evt-123" for event in logged_events["events"])


def test_calendar_create_event_detects_conflict(monkeypatch):
    def fake_list_events(limit=10, query=None, from_iso=None, to_iso=None):
        return [
            {
                "id": "evt-conflict",
                "summary": "Busy",
                "start": {"dateTime": "2026-02-01T17:00:00-05:00"},
                "end": {"dateTime": "2026-02-01T18:00:00-05:00"},
            }
        ]

    def fake_create_event(*args, **kwargs):
        raise AssertionError("create_event should not be called when conflict exists")

    monkeypatch.setattr(google_tools, "list_events", fake_list_events)
    monkeypatch.setattr(google_tools, "create_event", fake_create_event)

    response = google_tools.calendar_create_event(
        summary="Go Bowling",
        start_iso="2026-02-01T17:30:00-05:00",
        end_iso="2026-02-01T19:30:00-05:00",
        description="Bowling at Portland Bowling.",
    )

    assert response["status"] == "error"
    assert response["error"] == "calendar_conflict"
    assert response["conflicts"][0]["id"] == "evt-conflict"


def test_calendar_create_event_allows_no_conflict(monkeypatch):
    def fake_list_events(limit=10, query=None, from_iso=None, to_iso=None):
        return []

    def fake_create_event(summary, start_iso, end_iso, description=None, recurrence=None):
        return {
            "id": "evt-ok",
            "summary": summary,
            "recurrence": recurrence,
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
        }

    monkeypatch.setattr(google_tools, "list_events", fake_list_events)
    monkeypatch.setattr(google_tools, "create_event", fake_create_event)

    response = google_tools.calendar_create_event(
        summary="Dinner",
        start_iso="2026-02-01T20:00:00-05:00",
        end_iso="2026-02-01T21:00:00-05:00",
        recurrence=["RRULE:FREQ=DAILY;COUNT=3"],
    )

    assert response["status"] == "ok"
    assert response["event"]["id"] == "evt-ok"


def test_calendar_find_events(monkeypatch):
    def fake_list_events(limit=10, query=None, from_iso=None, to_iso=None):
        return [{"id": "evt-1", "summary": "Checkup"}]

    monkeypatch.setattr(google_tools, "list_events", fake_list_events)

    response = calendar_find_events(query="Checkup", limit=1)
    assert response["status"] == "ok"
    assert response["events"][0]["id"] == "evt-1"


def test_calendar_find_events_with_range(monkeypatch):
    captured = {}

    def fake_list_events(limit=10, query=None, from_iso=None, to_iso=None):
        captured["limit"] = limit
        captured["from_iso"] = from_iso
        captured["to_iso"] = to_iso
        return []

    monkeypatch.setattr(google_tools, "list_events", fake_list_events)

    response = calendar_find_events(
        from_iso="2026-02-01T00:00:00-05:00",
        to_iso="2026-02-02T00:00:00-05:00",
        limit=3,
    )
    assert response["status"] == "ok"
    assert captured["limit"] == 3
    assert captured["from_iso"] == "2026-02-01T00:00:00-05:00"
    assert captured["to_iso"] == "2026-02-02T00:00:00-05:00"


def test_calendar_update_and_delete_event(tmp_path, monkeypatch):
    db_path = tmp_path / "lists.db"
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(db_path))

    def fake_update_event(
        event_id,
        summary=None,
        start_iso=None,
        end_iso=None,
        description=None,
        recurrence=None,
    ):
        return {
            "id": event_id,
            "summary": summary or "Updated",
            "description": description,
            "htmlLink": "https://example.com/event",
            "recurrence": recurrence,
            "start": {"dateTime": start_iso or "2026-02-01T10:00:00-05:00"},
            "end": {"dateTime": end_iso or "2026-02-01T11:00:00-05:00"},
        }

    def fake_delete_event(event_id):
        return None

    monkeypatch.setattr(google_tools, "update_event", fake_update_event)
    monkeypatch.setattr(google_tools, "delete_event", fake_delete_event)

    update_response = calendar_update_event(
        event_id="evt-999",
        summary="Updated Event",
        start_iso="2026-02-01T10:00:00-05:00",
        end_iso="2026-02-01T11:00:00-05:00",
        recurrence="FREQ=WEEKLY;BYDAY=FR",
    )
    assert update_response["status"] == "ok"
    assert update_response["logged"] is True

    store = SQLiteListStore(db_path=str(db_path))
    assert store.get_calendar_event("evt-999") is not None

    delete_response = calendar_delete_event(event_id="evt-999")
    assert delete_response["status"] == "ok"
    assert store.get_calendar_event("evt-999") is None


def test_calendar_update_event_normalizes_time_only(monkeypatch):
    captured = {}

    def fake_update_event(
        event_id, summary=None, start_iso=None, end_iso=None, description=None, recurrence=None
    ):
        captured["start_iso"] = start_iso
        captured["end_iso"] = end_iso
        return {
            "id": event_id,
            "summary": summary or "Updated",
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
        }

    monkeypatch.setattr(google_tools, "update_event", fake_update_event)

    response = calendar_update_event(
        event_id="evt-555",
        start_iso="9:15am",
        end_iso="10:00am",
    )

    today = datetime.now().date().isoformat()
    assert response["status"] == "ok"
    assert captured["start_iso"].startswith(f"{today}T09:15:00")
    assert captured["end_iso"].startswith(f"{today}T10:00:00")
