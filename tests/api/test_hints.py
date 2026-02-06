from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import hints as hints_module
from packages.core.storage.sqlite import SQLiteListStore


def test_hints_crud(monkeypatch, tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    monkeypatch.setattr(hints_module, "_store", lambda: store)
    client = TestClient(app)

    create_resp = client.post(
        "/api/hints", json={"hint_type": "custom", "value": "Beer Frame"}
    )
    assert create_resp.status_code == 200
    assert create_resp.json() == {"hint_type": "custom", "value": "Beer Frame"}

    list_resp = client.get("/api/hints")
    assert list_resp.status_code == 200
    assert list_resp.json()["status"] == "ok"
    assert list_resp.json()["hints"] == [
        {"hint_type": "custom", "value": "Beer Frame"}
    ]

    update_resp = client.put(
        "/api/hints",
        json={
            "hint_type": "custom",
            "value": "Beer Frame",
            "new_value": "Gino",
        },
    )
    assert update_resp.status_code == 200
    assert update_resp.json() == {"hint_type": "custom", "value": "Gino"}

    filtered_resp = client.get("/api/hints", params={"hint_type": "custom"})
    assert filtered_resp.status_code == 200
    assert filtered_resp.json()["hints"] == [
        {"hint_type": "custom", "value": "Gino"}
    ]

    delete_resp = client.delete(
        "/api/hints", params={"hint_type": "custom", "value": "Gino"}
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json()["removed"] is True
