from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import calendar as calendar_module


class DummyCalendarClient:
    def list_events(self, start_iso: str, end_iso: str):
        return [
            type(
                "Event",
                (),
                {
                    "id": "evt1",
                    "title": "Meeting",
                    "start": start_iso,
                    "end": end_iso,
                    "location": None,
                    "description": None,
                },
            )()
        ]

    def get_event(self, event_id: str):
        if event_id != "evt1":
            return None
        return type(
            "Event",
            (),
            {
                "id": "evt1",
                "title": "Meeting",
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-01T01:00:00Z",
                "location": None,
                "description": None,
            },
        )()


def test_calendar_list_events(monkeypatch):
    monkeypatch.setattr(calendar_module, "default_google_client", lambda: DummyCalendarClient())
    client = TestClient(app)

    response = client.post(
        "/calendar/events",
        json={"start": "2024-01-01T00:00:00Z", "end": "2024-01-02T00:00:00Z"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["id"] == "evt1"


def test_calendar_get_event(monkeypatch):
    monkeypatch.setattr(calendar_module, "default_google_client", lambda: DummyCalendarClient())
    client = TestClient(app)

    response = client.get("/calendar/events/evt1")
    assert response.status_code == 200
    assert response.json()["id"] == "evt1"
