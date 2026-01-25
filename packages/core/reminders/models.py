from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Reminder:
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
