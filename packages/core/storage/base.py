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

    def remove_item(self, list_name: str, item: str) -> bool:
        """Remove an item from a list. Returns True if removed."""

    def update_item(self, list_name: str, old_item: str, new_item: str) -> bool:
        """Update an item in a list. Returns True if updated."""

    def list_lists(self) -> List[str]:
        """Return all list names."""

    def delete_list(self, list_name: str) -> bool:
        """Delete a list and its items. Returns True if deleted."""

    def clear_list(self, list_name: str) -> bool:
        """Remove all items from a list. Returns True if list exists."""

    def clear_all_lists(self) -> int:
        """Remove all lists and items. Returns number of lists deleted."""

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

    def list_threads(self, limit: int = 20) -> List[ThreadState]:
        """List recent threads."""


@dataclass(frozen=True)
class ReminderState:
    id: str
    title: str
    description: Optional[str]
    cron: str
    timezone: str
    email: Optional[str]
    sms_phone: Optional[str]
    sms_gateway_domain: Optional[str]
    active: bool
    last_sent_at: Optional[str]
    next_run_at: Optional[str]
    created_at: str
    updated_at: str


@runtime_checkable
class ReminderStore(Protocol):
    def create_reminder(self, reminder: ReminderState) -> None:
        """Persist a new reminder."""

    def update_reminder(self, reminder: ReminderState) -> None:
        """Update an existing reminder."""

    def get_reminder(self, reminder_id: str) -> Optional[ReminderState]:
        """Return reminder by id."""

    def list_reminders(self, active_only: bool = False) -> List[ReminderState]:
        """List reminders."""

    def delete_reminder(self, reminder_id: str) -> None:
        """Delete a reminder."""

    def list_due_reminders(self, now_iso: str) -> List[ReminderState]:
        """List reminders due to run."""


@dataclass(frozen=True)
class CalendarEventState:
    event_id: str
    summary: str
    start_iso: str
    end_iso: str
    description: Optional[str]
    html_link: Optional[str]
    recurrence: Optional[List[str]]
    source: str
    created_at: str
    updated_at: str


@runtime_checkable
class CalendarEventStore(Protocol):
    def upsert_calendar_event(
        self, event: CalendarEventState, raw_payload: Optional[Dict[str, Any]] = None
    ) -> None:
        """Insert or update a calendar event."""

    def get_calendar_event(self, event_id: str) -> Optional[CalendarEventState]:
        """Return a calendar event by id."""

    def list_calendar_events(self, limit: int = 20) -> List[CalendarEventState]:
        """List recent calendar events."""

    def delete_calendar_event(self, event_id: str) -> None:
        """Delete a calendar event by id."""


@dataclass(frozen=True)
class BowlingStatState:
    league_key: str
    team_name: Optional[str]
    player_name: Optional[str]
    average: Optional[int]
    handicap: Optional[int]
    wins: Optional[int]
    losses: Optional[int]
    high_game: Optional[int]
    high_series: Optional[int]
    points: Optional[float]
    raw: Dict[str, Any]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class BowlingMatchState:
    league_key: str
    match_date: Optional[str]
    match_time: Optional[str]
    lane: Optional[str]
    team_a: Optional[str]
    team_b: Optional[str]
    raw: Dict[str, Any]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class BowlingHintState:
    hint_type: str
    value: str
    created_at: str
    updated_at: str


@runtime_checkable
class BowlingStore(Protocol):
    def save_bowling_stats(self, league_key: str, stats: List[BowlingStatState]) -> None:
        """Replace bowling stats for a league."""

    def list_bowling_stats(
        self, league_key: str, team_name: Optional[str] = None, player_name: Optional[str] = None
    ) -> List[BowlingStatState]:
        """List bowling stats for a league."""

    def save_bowling_matches(self, league_key: str, matches: List[BowlingMatchState]) -> None:
        """Replace bowling matches for a league."""

    def list_bowling_matches(
        self,
        league_key: str,
        team_name: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[BowlingMatchState]:
        """List bowling matches for a league."""


@dataclass(frozen=True)
class BowlingFetchState:
    league_key: str
    last_fetch_at: str
    stats_url: Optional[str]
    schedule_url: Optional[str]
    standings_url: Optional[str]
    file_path: Optional[str]


@runtime_checkable
class BowlingFetchStore(Protocol):
    def upsert_bowling_fetch(self, fetch: BowlingFetchState) -> None:
        """Insert or update bowling fetch metadata."""

    def get_bowling_fetch(self, league_key: str) -> Optional[BowlingFetchState]:
        """Return fetch metadata for a league."""


@runtime_checkable
class BowlingHintStore(Protocol):
    def upsert_bowling_hint(self, hint: BowlingHintState) -> None:
        """Insert or update a bowling hint."""

    def delete_bowling_hint(self, hint_type: str, value: str) -> bool:
        """Delete a bowling hint. Returns True if deleted."""

    def list_bowling_hints(self, hint_type: Optional[str] = None) -> List[BowlingHintState]:
        """List bowling hints by type."""
