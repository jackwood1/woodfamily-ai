from __future__ import annotations

from typing import Any, Dict

from ..storage.base import ListStore


def create_list(store: ListStore, name: str) -> Dict[str, Any]:
    created = store.create_list(name)
    return {
        "status": "created" if created else "exists",
        "list_name": name,
    }


def add_item(store: ListStore, list_name: str, item: str) -> Dict[str, Any]:
    try:
        store.add_item(list_name, item)
        return {
            "status": "ok",
            "list_name": list_name,
            "item": item,
            "list_created": False,
        }
    except ValueError:
        store.create_list(list_name)
        store.add_item(list_name, item)
        return {
            "status": "ok",
            "list_name": list_name,
            "item": item,
            "list_created": True,
        }


def get_list(store: ListStore, list_name: str) -> Dict[str, Any]:
    items = store.get_list(list_name)
    if items is None:
        return {
            "status": "error",
            "error": "list_not_found",
            "list_name": list_name,
        }
    return {
        "status": "ok",
        "list_name": list_name,
        "items": [item.item for item in items],
    }
