from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools.list_tools import (
    add_item,
    clear_all_lists,
    clear_list,
    create_list,
    delete_list,
    get_list,
    list_lists,
    remove_item,
    update_item,
)


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


def test_list_tools_create_list_normalizes_name(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))

    first = create_list(store, "  Groceries  ")
    assert first["status"] == "created"
    assert first["list_name"] == "groceries"

    second = create_list(store, "GROCERIES")
    assert second["status"] == "exists"


def test_list_tools_remove_item(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    add_item(store, "groceries", "milk")

    removed = remove_item(store, "groceries", "milk")
    assert removed["status"] == "ok"

    missing = remove_item(store, "groceries", "milk")
    assert missing["status"] == "not_found"


def test_list_tools_update_item(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    add_item(store, "groceries", "milk")

    updated = update_item(store, "groceries", "milk", "oat milk")
    assert updated["status"] == "ok"

    items = get_list(store, "groceries")
    assert items["items"] == ["oat milk"]


def test_list_tools_list_lists(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    create_list(store, "chores")

    result = list_lists(store)
    assert result["status"] == "ok"
    assert result["lists"] == ["chores", "groceries"]


def test_list_tools_delete_list(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    add_item(store, "groceries", "milk")

    deleted = delete_list(store, "groceries")
    assert deleted["status"] == "ok"

    missing = delete_list(store, "groceries")
    assert missing["status"] == "not_found"


def test_list_tools_clear_list(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    add_item(store, "groceries", "milk")

    cleared = clear_list(store, "groceries")
    assert cleared["status"] == "ok"

    items = get_list(store, "groceries")
    assert items["items"] == []

    missing = clear_list(store, "missing")
    assert missing["status"] == "not_found"


def test_list_tools_clear_all_lists(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    create_list(store, "groceries")
    create_list(store, "chores")
    add_item(store, "groceries", "milk")

    result = clear_all_lists(store)
    assert result["status"] == "ok"
    assert result["deleted_lists"] == 2

    remaining = list_lists(store)
    assert remaining["lists"] == []
