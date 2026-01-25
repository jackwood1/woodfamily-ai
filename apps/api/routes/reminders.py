from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from apps.api.schemas.reminders import (
    ReminderCreateRequest,
    ReminderResponse,
    ReminderUpdateRequest,
)
from packages.core.reminders.service import (
    complete_reminder,
    create_reminder,
    list_reminders,
    update_reminder,
)
from packages.core.storage.sqlite import SQLiteListStore


router = APIRouter(prefix="/reminders", tags=["reminders"])


def _store() -> SQLiteListStore:
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "lists.db")
    return SQLiteListStore(db_path=db_path)


def _to_response(reminder) -> ReminderResponse:
    return ReminderResponse(
        id=reminder.id,
        title=reminder.title,
        description=reminder.description,
        cron=reminder.cron,
        timezone=reminder.timezone,
        email=reminder.email,
        sms_phone=reminder.sms_phone,
        sms_gateway_domain=reminder.sms_gateway_domain,
        active=reminder.active,
        last_sent_at=reminder.last_sent_at,
        next_run_at=reminder.next_run_at,
        created_at=reminder.created_at,
        updated_at=reminder.updated_at,
    )


@router.post("", response_model=ReminderResponse)
def create(payload: ReminderCreateRequest) -> ReminderResponse:
    try:
        reminder = create_reminder(
            _store(),
            title=payload.title,
            description=payload.description,
            cron=payload.cron,
            timezone=payload.timezone,
            email=payload.email,
            sms_phone=payload.sms_phone,
            sms_gateway_domain=payload.sms_gateway_domain,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _to_response(reminder)


@router.get("", response_model=List[ReminderResponse])
def list_all(active_only: bool = False) -> List[ReminderResponse]:
    reminders = list_reminders(_store(), active_only=active_only)
    return [_to_response(reminder) for reminder in reminders]


@router.get("/{reminder_id}", response_model=ReminderResponse)
def get(reminder_id: str) -> ReminderResponse:
    reminder = _store().get_reminder(reminder_id)
    if reminder is None:
        raise HTTPException(status_code=404, detail="Reminder not found")
    return _to_response(reminder)


@router.patch("/{reminder_id}", response_model=ReminderResponse)
def update(reminder_id: str, payload: ReminderUpdateRequest) -> ReminderResponse:
    store = _store()
    reminder = store.get_reminder(reminder_id)
    if reminder is None:
        raise HTTPException(status_code=404, detail="Reminder not found")
    try:
        updated = update_reminder(
            store,
            reminder,
            title=payload.title,
            description=payload.description,
            cron=payload.cron,
            timezone=payload.timezone,
            email=payload.email,
            sms_phone=payload.sms_phone,
            sms_gateway_domain=payload.sms_gateway_domain,
            active=payload.active,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _to_response(updated)


@router.post("/{reminder_id}/complete", response_model=ReminderResponse)
def complete(reminder_id: str) -> ReminderResponse:
    store = _store()
    reminder = store.get_reminder(reminder_id)
    if reminder is None:
        raise HTTPException(status_code=404, detail="Reminder not found")
    updated = complete_reminder(store, reminder)
    return _to_response(updated)


@router.delete("/{reminder_id}")
def delete(reminder_id: str) -> Dict[str, Any]:
    store = _store()
    reminder = store.get_reminder(reminder_id)
    if reminder is None:
        raise HTTPException(status_code=404, detail="Reminder not found")
    store.delete_reminder(reminder_id)
    return {"status": "deleted", "id": reminder_id}
