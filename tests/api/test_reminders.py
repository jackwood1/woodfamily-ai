from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import reminders as reminders_module
from packages.core.storage.sqlite import SQLiteListStore


def test_reminders_crud(monkeypatch, tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    monkeypatch.setattr(reminders_module, "_store", lambda: store)

    client = TestClient(app)

    create_resp = client.post(
        "/reminders",
        json={
            "title": "Pay rent",
            "description": "Monthly rent",
            "cron": "0 9 1 * *",
            "timezone": "UTC",
            "email": "test@example.com",
        },
    )
    assert create_resp.status_code == 200
    reminder = create_resp.json()
    assert reminder["id"]
    assert reminder["active"] is True

    list_resp = client.get("/reminders")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1

    get_resp = client.get(f"/reminders/{reminder['id']}")
    assert get_resp.status_code == 200

    update_resp = client.patch(
        f"/reminders/{reminder['id']}",
        json={"title": "Pay rent updated"},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["title"] == "Pay rent updated"

    complete_resp = client.post(f"/reminders/{reminder['id']}/complete")
    assert complete_resp.status_code == 200
    assert complete_resp.json()["active"] is False

    delete_resp = client.delete(f"/reminders/{reminder['id']}")
    assert delete_resp.status_code == 200
