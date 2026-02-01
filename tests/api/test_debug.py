from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import debug as debug_module


class DummyStore:
    def debug_snapshot(self, limit: int = 100):
        return {"lists": [{"name": "groceries"}], "items": []}


def test_debug_db_disabled(monkeypatch):
    monkeypatch.delenv("HOME_OPS_DEBUG", raising=False)
    client = TestClient(app)
    response = client.get("/api/debug/db")
    assert response.status_code == 404


def test_debug_db_enabled(monkeypatch):
    monkeypatch.setenv("HOME_OPS_DEBUG", "true")
    monkeypatch.setattr(debug_module, "_store", lambda: DummyStore())
    client = TestClient(app)
    response = client.get("/api/debug/db")
    assert response.status_code == 200
    assert response.json()["lists"][0]["name"] == "groceries"


def test_debug_sql_rejects_write(monkeypatch):
    monkeypatch.setenv("HOME_OPS_DEBUG", "true")
    monkeypatch.setattr(debug_module, "_store", lambda: DummyStore())
    client = TestClient(app)
    response = client.post("/api/debug/sql", json={"query": "DELETE FROM items"})
    assert response.status_code == 400


def test_debug_sql_select(monkeypatch):
    class SqlStore:
        def debug_query(self, query: str):
            return [{"name": "groceries"}]

    monkeypatch.setenv("HOME_OPS_DEBUG", "true")
    monkeypatch.setattr(debug_module, "_store", lambda: SqlStore())
    client = TestClient(app)
    response = client.post("/api/debug/sql", json={"query": "SELECT * FROM lists"})
    assert response.status_code == 200
    assert response.json()["rows"][0]["name"] == "groceries"
