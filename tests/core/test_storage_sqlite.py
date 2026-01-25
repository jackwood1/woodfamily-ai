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
