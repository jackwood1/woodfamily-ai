from __future__ import annotations

from typing import Any, Dict

from ..storage.base import ListStore


def _normalize_list_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _normalize_item(item: str) -> str:
    return " ".join(item.strip().split())


def _normalize_item_key(item: str) -> str:
    return _normalize_item(item).lower()


def create_list(store: ListStore, name: str) -> Dict[str, Any]:
    list_name = _normalize_list_name(name)
    created = store.create_list(list_name)
    return {
        "status": "created" if created else "exists",
        "list_name": list_name,
    }


def add_item(store: ListStore, list_name: str, item: str) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    normalized_item = _normalize_item(item)
    item_key = normalized_item.lower()

    items = store.get_list(normalized_list)
    list_created = False
    if items is None:
        store.create_list(normalized_list)
        list_created = True
        items = []

    if any(_normalize_item_key(existing.item) == item_key for existing in items):
        return {
            "status": "ok",
            "list_name": normalized_list,
            "item": normalized_item,
            "list_created": list_created,
            "deduped": True,
        }

    store.add_item(normalized_list, normalized_item)
    return {
        "status": "ok",
        "list_name": normalized_list,
        "item": normalized_item,
        "list_created": list_created,
        "deduped": False,
    }


def get_list(store: ListStore, list_name: str) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    items = store.get_list(normalized_list)
    if items is None:
        return {
            "status": "error",
            "error": "list_not_found",
            "list_name": normalized_list,
        }
    return {
        "status": "ok",
        "list_name": normalized_list,
        "items": [item.item for item in items],
    }


def remove_item(store: ListStore, list_name: str, item: str) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    normalized_item = _normalize_item(item)
    removed = store.remove_item(normalized_list, normalized_item)
    return {
        "status": "ok" if removed else "not_found",
        "list_name": normalized_list,
        "item": normalized_item,
    }


def update_item(
    store: ListStore, list_name: str, old_item: str, new_item: str
) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    normalized_old = _normalize_item(old_item)
    normalized_new = _normalize_item(new_item)
    updated = store.update_item(normalized_list, normalized_old, normalized_new)
    return {
        "status": "ok" if updated else "not_found",
        "list_name": normalized_list,
        "old_item": normalized_old,
        "new_item": normalized_new,
    }


def list_lists(store: ListStore) -> Dict[str, Any]:
    lists = store.list_lists()
    return {
        "status": "ok",
        "lists": lists,
    }


def delete_list(store: ListStore, list_name: str) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    deleted = store.delete_list(normalized_list)
    return {
        "status": "ok" if deleted else "not_found",
        "list_name": normalized_list,
    }


def clear_list(store: ListStore, list_name: str) -> Dict[str, Any]:
    normalized_list = _normalize_list_name(list_name)
    cleared = store.clear_list(normalized_list)
    return {
        "status": "ok" if cleared else "not_found",
        "list_name": normalized_list,
    }


def clear_all_lists(store: ListStore) -> Dict[str, Any]:
    deleted_count = store.clear_all_lists()
    return {
        "status": "ok",
        "deleted_lists": deleted_count,
    }
