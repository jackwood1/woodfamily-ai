from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ReminderCreateRequest(BaseModel):
    title: str = Field(..., min_length=1)
    description: Optional[str] = None
    cron: str = Field(..., min_length=1)
    timezone: str = Field(default="UTC", min_length=1)
    email: Optional[str] = None
    sms_phone: Optional[str] = None
    sms_gateway_domain: Optional[str] = None


class ReminderUpdateRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    cron: Optional[str] = None
    timezone: Optional[str] = None
    email: Optional[str] = None
    sms_phone: Optional[str] = None
    sms_gateway_domain: Optional[str] = None
    active: Optional[bool] = None


class ReminderResponse(BaseModel):
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
