import json
import os

from fastapi.testclient import TestClient

from apps.api.main import app


def test_google_status_and_disconnect(monkeypatch, tmp_path):
    token_path = tmp_path / "google_tokens.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "token",
                "refresh_token": "refresh",
                "expiry": 9999999999,
                "scopes": ["scope1"],
                "email": "user@example.com",
                "subject": "sub",
            }
        )
    )
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", str(token_path))

    client = TestClient(app)

    status = client.get("/api/integrations/google/status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["connected"] is True
    assert payload["email"] == "user@example.com"

    disconnect = client.post("/api/integrations/google/disconnect")
    assert disconnect.status_code == 200
    assert disconnect.json()["connected"] is False
    assert not os.path.exists(token_path)
