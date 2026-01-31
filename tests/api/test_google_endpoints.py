from fastapi.testclient import TestClient

from apps.api.main import app
from packages.core.google import oauth as oauth_module


def test_google_oauth_invalid_state(monkeypatch):
    client = TestClient(app)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")
    response = client.get("/api/integrations/google/callback?code=abc&state=bad")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid state"


def test_google_oauth_not_configured():
    client = TestClient(app)
    response = client.get("/api/integrations/google/start")
    assert response.status_code == 400


def test_gmail_unread_not_connected(monkeypatch, tmp_path):
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", str(tmp_path / "google_tokens.json"))
    client = TestClient(app)
    response = client.get("/api/integrations/gmail/unread")
    assert response.status_code == 400
    assert response.json()["detail"] == "google_not_connected"


def test_calendar_upcoming_not_connected(monkeypatch, tmp_path):
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", str(tmp_path / "google_tokens.json"))
    client = TestClient(app)
    response = client.get("/api/integrations/calendar/upcoming")
    assert response.status_code == 400
    assert response.json()["detail"] == "google_not_connected"
