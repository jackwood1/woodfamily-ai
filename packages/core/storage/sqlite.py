from __future__ import annotations

import json
import os
import sqlite3
import uuid
from typing import List, Optional

from .base import CalendarEventState, ListItem, ListStore, ReminderState, ThreadState


class SQLiteListStore(ListStore):
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lists (
                    name TEXT PRIMARY KEY
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    list_name TEXT NOT NULL,
                    item TEXT NOT NULL,
                    item_norm TEXT NOT NULL,
                    FOREIGN KEY(list_name) REFERENCES lists(name)
                )
                """
            )
            self._ensure_column(conn, "items", "item_norm", "TEXT NOT NULL", "''")
            self._backfill_item_norm(conn)
            self._dedupe_items(conn)
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS items_list_name_item_norm_idx
                ON items (list_name, item_norm)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS threads (
                    id TEXT PRIMARY KEY,
                    summary TEXT NOT NULL,
                    recent_messages TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reminders (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    cron TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    email TEXT,
                    sms_phone TEXT,
                    sms_gateway_domain TEXT,
                    active INTEGER NOT NULL,
                    last_sent_at TEXT,
                    next_run_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_events (
                    event_id TEXT PRIMARY KEY,
                    summary TEXT NOT NULL,
                    start_iso TEXT NOT NULL,
                    end_iso TEXT NOT NULL,
                    description TEXT,
                    html_link TEXT,
                    source TEXT NOT NULL,
                    raw_payload TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def _ensure_column(
        self, conn: sqlite3.Connection, table: str, column: str, column_def: str, default_sql: str
    ) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column in columns:
            return
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN {column} {column_def} DEFAULT {default_sql}"
        )

    def _normalize_list_name(self, name: str) -> str:
        return " ".join(name.strip().lower().split())

    def _normalize_item(self, item: str) -> str:
        return " ".join(item.strip().split())

    def _backfill_item_norm(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE items
            SET item_norm = lower(trim(item))
            WHERE item_norm IS NULL OR item_norm = ''
            """
        )

    def _dedupe_items(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            DELETE FROM items
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM items
                GROUP BY list_name, item_norm
            )
            """
        )

    def create_list(self, name: str) -> bool:
        name = self._normalize_list_name(name)
        with self._connect() as conn:
            try:
                conn.execute("INSERT INTO lists (name) VALUES (?)", (name,))
                return True
            except sqlite3.IntegrityError:
                return False

    def add_item(self, list_name: str, item: str) -> None:
        list_name = self._normalize_list_name(list_name)
        item = self._normalize_item(item)
        item_norm = item.lower()
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM lists WHERE name = ? LIMIT 1", (list_name,)
            )
            if cur.fetchone() is None:
                raise ValueError(f"List not found: {list_name}")
            conn.execute(
                "INSERT OR IGNORE INTO items (list_name, item, item_norm) VALUES (?, ?, ?)",
                (list_name, item, item_norm),
            )

    def remove_item(self, list_name: str, item: str) -> bool:
        list_name = self._normalize_list_name(list_name)
        item_norm = self._normalize_item(item).lower()
        with self._connect() as conn:
            result = conn.execute(
                "DELETE FROM items WHERE list_name = ? AND item_norm = ?",
                (list_name, item_norm),
            )
            return result.rowcount > 0

    def update_item(self, list_name: str, old_item: str, new_item: str) -> bool:
        list_name = self._normalize_list_name(list_name)
        old_norm = self._normalize_item(old_item).lower()
        new_item_clean = self._normalize_item(new_item)
        new_norm = new_item_clean.lower()
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM items WHERE list_name = ? AND item_norm = ? LIMIT 1",
                (list_name, old_norm),
            ).fetchone()
            if exists is None:
                return False
            if old_norm == new_norm:
                conn.execute(
                    "UPDATE items SET item = ?, item_norm = ? WHERE list_name = ? AND item_norm = ?",
                    (new_item_clean, new_norm, list_name, old_norm),
                )
                return True
            conn.execute(
                "DELETE FROM items WHERE list_name = ? AND item_norm = ?",
                (list_name, new_norm),
            )
            result = conn.execute(
                "UPDATE items SET item = ?, item_norm = ? WHERE list_name = ? AND item_norm = ?",
                (new_item_clean, new_norm, list_name, old_norm),
            )
            return result.rowcount > 0

    def get_list(self, list_name: str) -> Optional[List[ListItem]]:
        list_name = self._normalize_list_name(list_name)
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM lists WHERE name = ? LIMIT 1", (list_name,)
            )
            if cur.fetchone() is None:
                return None
            rows = conn.execute(
                "SELECT item FROM items WHERE list_name = ? ORDER BY id ASC",
                (list_name,),
            ).fetchall()
            return [ListItem(list_name=list_name, item=row[0]) for row in rows]

    def list_lists(self) -> List[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name FROM lists ORDER BY name ASC"
            ).fetchall()
            return [row[0] for row in rows]

    def delete_list(self, list_name: str) -> bool:
        list_name = self._normalize_list_name(list_name)
        with self._connect() as conn:
            conn.execute("DELETE FROM items WHERE list_name = ?", (list_name,))
            result = conn.execute("DELETE FROM lists WHERE name = ?", (list_name,))
            return result.rowcount > 0

    def clear_list(self, list_name: str) -> bool:
        list_name = self._normalize_list_name(list_name)
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM lists WHERE name = ? LIMIT 1", (list_name,)
            ).fetchone()
            if exists is None:
                return False
            conn.execute("DELETE FROM items WHERE list_name = ?", (list_name,))
            return True

    def clear_all_lists(self) -> int:
        with self._connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM lists").fetchone()[0]
            conn.execute("DELETE FROM items")
            conn.execute("DELETE FROM lists")
            return count

    def create_thread(self, thread_id: Optional[str] = None) -> str:
        thread_id = thread_id or str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO threads (id, summary, recent_messages, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                """,
                (thread_id, "", "[]"),
            )
        return thread_id

    def get_thread(self, thread_id: str) -> Optional[ThreadState]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT summary, recent_messages FROM threads WHERE id = ?",
                (thread_id,),
            ).fetchone()
            if row is None:
                return None
            summary, recent_messages = row
            return ThreadState(
                thread_id=thread_id,
                summary=summary or "",
                recent_messages=json.loads(recent_messages or "[]"),
            )

    def update_thread(self, thread_id: str, summary: str, recent_messages: List[dict]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO threads (id, summary, recent_messages, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    summary = excluded.summary,
                    recent_messages = excluded.recent_messages,
                    updated_at = excluded.updated_at
                """,
                (thread_id, summary, json.dumps(recent_messages)),
            )

    def list_threads(self, limit: int = 20) -> List[ThreadState]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, summary, recent_messages
                FROM threads
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [
                ThreadState(
                    thread_id=row[0],
                    summary=row[1] or "",
                    recent_messages=json.loads(row[2] or "[]"),
                )
                for row in rows
            ]

    def create_reminder(self, reminder: ReminderState) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO reminders (
                    id, title, description, cron, timezone, email, sms_phone,
                    sms_gateway_domain, active, last_sent_at, next_run_at,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reminder.id,
                    reminder.title,
                    reminder.description,
                    reminder.cron,
                    reminder.timezone,
                    reminder.email,
                    reminder.sms_phone,
                    reminder.sms_gateway_domain,
                    1 if reminder.active else 0,
                    reminder.last_sent_at,
                    reminder.next_run_at,
                    reminder.created_at,
                    reminder.updated_at,
                ),
            )

    def update_reminder(self, reminder: ReminderState) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE reminders
                SET title = ?, description = ?, cron = ?, timezone = ?, email = ?,
                    sms_phone = ?, sms_gateway_domain = ?, active = ?,
                    last_sent_at = ?, next_run_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    reminder.title,
                    reminder.description,
                    reminder.cron,
                    reminder.timezone,
                    reminder.email,
                    reminder.sms_phone,
                    reminder.sms_gateway_domain,
                    1 if reminder.active else 0,
                    reminder.last_sent_at,
                    reminder.next_run_at,
                    reminder.updated_at,
                    reminder.id,
                ),
            )

    def get_reminder(self, reminder_id: str) -> Optional[ReminderState]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, title, description, cron, timezone, email, sms_phone,
                       sms_gateway_domain, active, last_sent_at, next_run_at,
                       created_at, updated_at
                FROM reminders
                WHERE id = ?
                """,
                (reminder_id,),
            ).fetchone()
            if row is None:
                return None
            return ReminderState(
                id=row[0],
                title=row[1],
                description=row[2],
                cron=row[3],
                timezone=row[4],
                email=row[5],
                sms_phone=row[6],
                sms_gateway_domain=row[7],
                active=bool(row[8]),
                last_sent_at=row[9],
                next_run_at=row[10],
                created_at=row[11],
                updated_at=row[12],
            )

    def list_reminders(self, active_only: bool = False) -> List[ReminderState]:
        with self._connect() as conn:
            if active_only:
                rows = conn.execute(
                    """
                    SELECT id, title, description, cron, timezone, email, sms_phone,
                           sms_gateway_domain, active, last_sent_at, next_run_at,
                           created_at, updated_at
                    FROM reminders
                    WHERE active = 1
                    ORDER BY created_at DESC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, title, description, cron, timezone, email, sms_phone,
                           sms_gateway_domain, active, last_sent_at, next_run_at,
                           created_at, updated_at
                    FROM reminders
                    ORDER BY created_at DESC
                    """
                ).fetchall()
            return [
                ReminderState(
                    id=row[0],
                    title=row[1],
                    description=row[2],
                    cron=row[3],
                    timezone=row[4],
                    email=row[5],
                    sms_phone=row[6],
                    sms_gateway_domain=row[7],
                    active=bool(row[8]),
                    last_sent_at=row[9],
                    next_run_at=row[10],
                    created_at=row[11],
                    updated_at=row[12],
                )
                for row in rows
            ]

    def delete_reminder(self, reminder_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))

    def list_due_reminders(self, now_iso: str) -> List[ReminderState]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, title, description, cron, timezone, email, sms_phone,
                       sms_gateway_domain, active, last_sent_at, next_run_at,
                       created_at, updated_at
                FROM reminders
                WHERE active = 1
                  AND next_run_at IS NOT NULL
                  AND next_run_at <= ?
                ORDER BY next_run_at ASC
                """,
                (now_iso,),
            ).fetchall()
            return [
                ReminderState(
                    id=row[0],
                    title=row[1],
                    description=row[2],
                    cron=row[3],
                    timezone=row[4],
                    email=row[5],
                    sms_phone=row[6],
                    sms_gateway_domain=row[7],
                    active=bool(row[8]),
                    last_sent_at=row[9],
                    next_run_at=row[10],
                    created_at=row[11],
                    updated_at=row[12],
                )
                for row in rows
            ]

    def upsert_calendar_event(
        self, event: CalendarEventState, raw_payload: Optional[dict] = None
    ) -> None:
        raw_json = json.dumps(raw_payload) if raw_payload else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO calendar_events (
                    event_id, summary, start_iso, end_iso, description, html_link,
                    source, raw_payload, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    summary = excluded.summary,
                    start_iso = excluded.start_iso,
                    end_iso = excluded.end_iso,
                    description = excluded.description,
                    html_link = excluded.html_link,
                    source = excluded.source,
                    raw_payload = excluded.raw_payload,
                    updated_at = excluded.updated_at
                """,
                (
                    event.event_id,
                    event.summary,
                    event.start_iso,
                    event.end_iso,
                    event.description,
                    event.html_link,
                    event.source,
                    raw_json,
                    event.created_at,
                    event.updated_at,
                ),
            )

    def get_calendar_event(self, event_id: str) -> Optional[CalendarEventState]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT event_id, summary, start_iso, end_iso, description, html_link,
                       source, created_at, updated_at
                FROM calendar_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                return None
            return CalendarEventState(
                event_id=row[0],
                summary=row[1],
                start_iso=row[2],
                end_iso=row[3],
                description=row[4],
                html_link=row[5],
                source=row[6],
                created_at=row[7],
                updated_at=row[8],
            )

    def list_calendar_events(self, limit: int = 20) -> List[CalendarEventState]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT event_id, summary, start_iso, end_iso, description, html_link,
                       source, created_at, updated_at
                FROM calendar_events
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [
                CalendarEventState(
                    event_id=row[0],
                    summary=row[1],
                    start_iso=row[2],
                    end_iso=row[3],
                    description=row[4],
                    html_link=row[5],
                    source=row[6],
                    created_at=row[7],
                    updated_at=row[8],
                )
                for row in rows
            ]

    def delete_calendar_event(self, event_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM calendar_events WHERE event_id = ?", (event_id,))
