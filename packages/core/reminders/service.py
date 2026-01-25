from __future__ import annotations

import datetime as dt
import uuid
from typing import List, Optional

from apscheduler.triggers.cron import CronTrigger

from ..storage.base import ReminderState, ReminderStore


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso(value: Optional[dt.datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _next_run(cron: str, timezone: str) -> str:
    trigger = CronTrigger.from_crontab(cron, timezone=timezone)
    next_fire = trigger.get_next_fire_time(None, _utc_now())
    if next_fire is None:
        return _utc_now().isoformat()
    return next_fire.astimezone(dt.timezone.utc).isoformat()


def create_reminder(
    store: ReminderStore,
    title: str,
    description: Optional[str],
    cron: str,
    timezone: str,
    email: Optional[str],
    sms_phone: Optional[str],
    sms_gateway_domain: Optional[str],
) -> ReminderState:
    now = _utc_now()
    reminder = ReminderState(
        id=str(uuid.uuid4()),
        title=title.strip(),
        description=description.strip() if description else None,
        cron=cron.strip(),
        timezone=timezone,
        email=email.strip() if email else None,
        sms_phone=sms_phone.strip() if sms_phone else None,
        sms_gateway_domain=sms_gateway_domain.strip() if sms_gateway_domain else None,
        active=True,
        last_sent_at=None,
        next_run_at=_next_run(cron, timezone),
        created_at=_to_iso(now),
        updated_at=_to_iso(now),
    )
    store.create_reminder(reminder)
    return reminder


def update_reminder(
    store: ReminderStore,
    reminder: ReminderState,
    title: Optional[str] = None,
    description: Optional[str] = None,
    cron: Optional[str] = None,
    timezone: Optional[str] = None,
    email: Optional[str] = None,
    sms_phone: Optional[str] = None,
    sms_gateway_domain: Optional[str] = None,
    active: Optional[bool] = None,
) -> ReminderState:
    now = _utc_now()
    updated = ReminderState(
        id=reminder.id,
        title=title.strip() if title is not None else reminder.title,
        description=description.strip() if description is not None else reminder.description,
        cron=cron.strip() if cron is not None else reminder.cron,
        timezone=timezone or reminder.timezone,
        email=email.strip() if email is not None else reminder.email,
        sms_phone=sms_phone.strip() if sms_phone is not None else reminder.sms_phone,
        sms_gateway_domain=(
            sms_gateway_domain.strip()
            if sms_gateway_domain is not None
            else reminder.sms_gateway_domain
        ),
        active=active if active is not None else reminder.active,
        last_sent_at=reminder.last_sent_at,
        next_run_at=_next_run(
            cron.strip() if cron is not None else reminder.cron,
            timezone or reminder.timezone,
        ),
        created_at=reminder.created_at,
        updated_at=_to_iso(now),
    )
    store.update_reminder(updated)
    return updated


def complete_reminder(store: ReminderStore, reminder: ReminderState) -> ReminderState:
    now = _utc_now()
    updated = ReminderState(
        **{
            **reminder.__dict__,
            "active": False,
            "updated_at": _to_iso(now),
        }
    )
    store.update_reminder(updated)
    return updated


def list_reminders(store: ReminderStore, active_only: bool = False) -> List[ReminderState]:
    return store.list_reminders(active_only=active_only)


def touch_reminder_sent(store: ReminderStore, reminder: ReminderState) -> ReminderState:
    now = _utc_now()
    updated = ReminderState(
        **{
            **reminder.__dict__,
            "last_sent_at": _to_iso(now),
            "next_run_at": _next_run(reminder.cron, reminder.timezone),
            "updated_at": _to_iso(now),
        }
    )
    store.update_reminder(updated)
    return updated
