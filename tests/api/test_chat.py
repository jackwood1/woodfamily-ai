from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import chat as chat_module


class DummyAgent:
    def chat(self, message: str):
        return {
            "reply": f"Echo: {message}",
            "tool_calls": [{"name": "noop", "args": {}, "result": {}}],
        }


def test_chat_route(monkeypatch):
    monkeypatch.setattr(chat_module, "_AGENT", DummyAgent())
    client = TestClient(app)

    response = client.post("/chat", json={"message": "hello"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["reply"] == "Echo: hello"
    assert payload["tool_calls"][0]["name"] == "noop"
