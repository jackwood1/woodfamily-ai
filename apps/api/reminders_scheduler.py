from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

from apps.api.notifications import send_email, send_sms_via_email
from packages.core.reminders.service import touch_reminder_sent
from packages.core.storage.sqlite import SQLiteListStore


logger = logging.getLogger("home_ops.reminders")


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _reminder_body(title: str, description: Optional[str]) -> str:
    if description:
        return f"{title}\n\n{description}"
    return title


def process_due_reminders(store: SQLiteListStore) -> None:
    due = store.list_due_reminders(_utc_now_iso())
    for reminder in due:
        subject = f"Reminder: {reminder.title}"
        body = _reminder_body(reminder.title, reminder.description)
        try:
            if reminder.email:
                send_email(reminder.email, subject, body)
            if reminder.sms_phone and reminder.sms_gateway_domain:
                send_sms_via_email(
                    reminder.sms_phone,
                    reminder.sms_gateway_domain,
                    subject,
                    body,
                )
            touch_reminder_sent(store, reminder)
        except Exception as exc:
            logger.exception("reminder_send_failed id=%s error=%s", reminder.id, exc)


def start_scheduler(store: SQLiteListStore) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        process_due_reminders,
        "interval",
        minutes=1,
        args=[store],
        id="reminders",
        replace_existing=True,
    )
    scheduler.start()
    return scheduler
