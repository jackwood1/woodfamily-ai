from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol


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
