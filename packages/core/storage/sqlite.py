from __future__ import annotations

import json
import os
import sqlite3
import uuid
from typing import List, Optional

from .base import ListItem, ListStore, ReminderState, ThreadState


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
