from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from packages.core.storage.base import BowlingHintState, BowlingHintStore


def add_bowling_hint(
    store: BowlingHintStore, hint_type: str, value: str
) -> Dict[str, Any]:
    timestamp = datetime.now().isoformat()
    hint = BowlingHintState(
        hint_type=hint_type,
        value=value,
        created_at=timestamp,
        updated_at=timestamp,
    )
    store.upsert_bowling_hint(hint)
    return {"status": "ok", "hint_type": hint_type, "value": value}


def remove_bowling_hint(
    store: BowlingHintStore, hint_type: str, value: str
) -> Dict[str, Any]:
    removed = store.delete_bowling_hint(hint_type, value)
    return {"status": "ok", "removed": removed, "hint_type": hint_type, "value": value}


def list_bowling_hints(
    store: BowlingHintStore, hint_type: Optional[str] = None
) -> Dict[str, Any]:
    hints = store.list_bowling_hints(hint_type=hint_type)
    return {
        "status": "ok",
        "hints": [
            {"hint_type": hint.hint_type, "value": hint.value} for hint in hints
        ],
    }
