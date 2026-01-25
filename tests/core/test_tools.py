from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools.list_tools import add_item, create_list, get_list


def test_list_tools_happy_path(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    result = create_list(store, "chores")
    assert result["status"] == "created"

    result = add_item(store, "chores", "laundry")
    assert result["status"] == "ok"

    result = get_list(store, "chores")
    assert result["status"] == "ok"
    assert result["items"] == ["laundry"]


def test_list_tools_missing_list(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    result = add_item(store, "missing", "item")
    assert result["status"] == "error"
    assert result["error"] == "list_not_found"

    result = get_list(store, "missing")
    assert result["status"] == "error"
    assert result["error"] == "list_not_found"
