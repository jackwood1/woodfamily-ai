from packages.core.storage.base import BowlingHintState
from packages.core.storage.sqlite import SQLiteListStore


def test_sqlite_store_create_add_get(tmp_path):
    db_path = tmp_path / "lists.db"
    store = SQLiteListStore(db_path=str(db_path))

    assert store.create_list(" Groceries ") is True
    assert store.create_list("groceries") is False

    store.add_item("groceries", " milk ")
    store.add_item("groceries", "eggs")
    store.add_item("groceries", "MILK")

    items = store.get_list("GROCERIES")
    assert items is not None
    assert [item.item for item in items] == ["milk", "eggs"]


def test_sqlite_store_add_missing_list_raises(tmp_path):
    db_path = tmp_path / "lists.db"
    store = SQLiteListStore(db_path=str(db_path))

    try:
        store.add_item("missing", "item")
        raised = False
    except ValueError:
        raised = True

    assert raised is True


def test_sqlite_store_bowling_hints(tmp_path):
    db_path = tmp_path / "lists.db"
    store = SQLiteListStore(db_path=str(db_path))

    store.upsert_bowling_hint(
        BowlingHintState(
            hint_type="bowler",
            value="Gino",
            created_at="2026-01-01T00:00:00",
            updated_at="2026-01-01T00:00:00",
        )
    )
    store.upsert_bowling_hint(
        BowlingHintState(
            hint_type="team",
            value="Beer Frame",
            created_at="2026-01-01T00:00:00",
            updated_at="2026-01-01T00:00:00",
        )
    )

    bowler_hints = store.list_bowling_hints("bowler")
    assert len(bowler_hints) == 1
    assert bowler_hints[0].value == "Gino"

    team_hints = store.list_bowling_hints("team")
    assert len(team_hints) == 1
    assert team_hints[0].value == "Beer Frame"

    removed = store.delete_bowling_hint("bowler", "Gino")
    assert removed is True
