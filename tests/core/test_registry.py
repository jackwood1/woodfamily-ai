from packages.core.storage.sqlite import SQLiteListStore
from packages.core.tools.registry import build_list_tool_registry


def test_registry_lists_and_schemas(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    registry = build_list_tool_registry(store)

    tool_names = registry.list_tools()
    assert set(tool_names) == {
        "create_list",
        "add_item",
        "get_list",
        "list_lists",
        "remove_item",
        "update_item",
        "delete_list",
        "clear_list",
        "clear_all_lists",
        "gmail_list_unread",
        "gmail_get_message",
        "calendar_list_upcoming",
        "calendar_list_logged",
        "calendar_create_event",
    }
    assert registry.has_tool("create_list") is True
    assert registry.has_tool("unknown") is False

    schemas = registry.get_tool_schemas()
    schema_names = {schema["function"]["name"] for schema in schemas}
    assert schema_names == {
        "create_list",
        "add_item",
        "get_list",
        "list_lists",
        "remove_item",
        "update_item",
        "delete_list",
        "clear_list",
        "clear_all_lists",
        "gmail_list_unread",
        "gmail_get_message",
        "calendar_list_upcoming",
        "calendar_list_logged",
        "calendar_create_event",
    }


def test_registry_unknown_tool_returns_error(tmp_path):
    store = SQLiteListStore(db_path=str(tmp_path / "lists.db"))
    registry = build_list_tool_registry(store)

    result = registry.call("unknown_tool", {})
    assert result["status"] == "error"
    assert result["error"] == "unknown_tool"
