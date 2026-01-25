from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class ListItem:
    list_name: str
    item: str


class ListStore(Protocol):
    def create_list(self, name: str) -> bool:
        """Create a new list. Returns True if created, False if it exists."""

    def add_item(self, list_name: str, item: str) -> None:
        """Add an item to an existing list. Raises if list does not exist."""

    def get_list(self, list_name: str) -> Optional[List[ListItem]]:
        """Return items for a list, or None if list does not exist."""


@dataclass(frozen=True)
class ThreadState:
    thread_id: str
    summary: str
    recent_messages: List[Dict[str, Any]]


@runtime_checkable
class ThreadStore(Protocol):
    def create_thread(self, thread_id: Optional[str] = None) -> str:
        """Create or return a thread id."""

    def get_thread(self, thread_id: str) -> Optional[ThreadState]:
        """Return thread state or None if missing."""

    def update_thread(
        self, thread_id: str, summary: str, recent_messages: List[Dict[str, Any]]
    ) -> None:
        """Persist thread state."""
