from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools.list_tools import add_item, create_list, get_list


def test_list_tools_happy_path(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    result = create_list(store, "chores")
    assert result["status"] == "created"

    result = add_item(store, "chores", "laundry")
    assert result["status"] == "ok"
    assert result["list_created"] is False
    assert result["deduped"] is False

    result = get_list(store, "chores")
    assert result["status"] == "ok"
    assert result["items"] == ["laundry"]


def test_list_tools_missing_list(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    result = add_item(store, "missing", "item")
    assert result["status"] == "ok"
    assert result["list_created"] is True
    assert result["deduped"] is False

    result = get_list(store, "missing")
    assert result["status"] == "ok"
    assert result["items"] == ["item"]


def test_list_tools_normalize_and_dedupe(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    result = create_list(store, "  Groceries  ")
    assert result["list_name"] == "groceries"

    first = add_item(store, "Groceries", "  Milk  ")
    assert first["deduped"] is False

    second = add_item(store, "groceries", "milk")
    assert second["deduped"] is True

    items = get_list(store, "GROCERIES")
    assert items["items"] == ["Milk"]
