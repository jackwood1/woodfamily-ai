from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import List, Optional, Protocol

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


SCOPES_DEFAULT = ["https://www.googleapis.com/auth/calendar.events"]


@dataclass(frozen=True)
class CalendarEvent:
    id: str
    title: str
    start: str
    end: str
    location: Optional[str]
    description: Optional[str]


class CalendarClient(Protocol):
    def list_events(self, start_iso: str, end_iso: str) -> List[CalendarEvent]:
        """Return events between start and end."""

    def get_event(self, event_id: str) -> Optional[CalendarEvent]:
        """Return a single event."""


class GoogleCalendarClient(CalendarClient):
    def __init__(
        self,
        credentials_path: str,
        token_path: str,
        calendar_id: str = "primary",
    ) -> None:
        self._credentials_path = credentials_path
        self._token_path = token_path
        self._calendar_id = calendar_id

    def _get_credentials(self) -> Credentials:
        creds: Optional[Credentials] = None
        if os.path.exists(self._token_path):
            creds = Credentials.from_authorized_user_file(self._token_path, _scopes())
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        if not creds or not creds.valid:
            if not os.path.exists(self._credentials_path):
                raise RuntimeError(
                    "Missing Google OAuth credentials file. "
                    "Set CALENDAR_CREDENTIALS_PATH to your credentials.json."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                self._credentials_path, _scopes()
            )
            creds = flow.run_local_server(port=0)
        if creds:
            os.makedirs(os.path.dirname(self._token_path), exist_ok=True)
            with open(self._token_path, "w", encoding="utf-8") as token_file:
                token_file.write(creds.to_json())
        return creds

    def _service(self):
        creds = self._get_credentials()
        return build("calendar", "v3", credentials=creds)

    def list_events(self, start_iso: str, end_iso: str) -> List[CalendarEvent]:
        service = self._service()
        response = (
            service.events()
            .list(
                calendarId=self._calendar_id,
                timeMin=start_iso,
                timeMax=end_iso,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )
        events = []
        for item in response.get("items", []):
            start = item.get("start", {}).get("dateTime") or item.get("start", {}).get("date")
            end = item.get("end", {}).get("dateTime") or item.get("end", {}).get("date")
            events.append(
                CalendarEvent(
                    id=item.get("id", ""),
                    title=item.get("summary", ""),
                    start=start,
                    end=end,
                    location=item.get("location"),
                    description=item.get("description"),
                )
            )
        return events

    def get_event(self, event_id: str) -> Optional[CalendarEvent]:
        service = self._service()
        try:
            item = service.events().get(calendarId=self._calendar_id, eventId=event_id).execute()
        except Exception:
            return None
        start = item.get("start", {}).get("dateTime") or item.get("start", {}).get("date")
        end = item.get("end", {}).get("dateTime") or item.get("end", {}).get("date")
        return CalendarEvent(
            id=item.get("id", ""),
            title=item.get("summary", ""),
            start=start,
            end=end,
            location=item.get("location"),
            description=item.get("description"),
        )

    def create_event(
        self,
        summary: str,
        start_iso: str,
        end_iso: str,
        description: Optional[str] = None,
    ) -> CalendarEvent:
        service = self._service()
        payload = {
            "summary": summary,
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
        }
        if description:
            payload["description"] = description
        item = (
            service.events()
            .insert(calendarId=self._calendar_id, body=payload)
            .execute()
        )
        start = item.get("start", {}).get("dateTime") or item.get("start", {}).get("date")
        end = item.get("end", {}).get("dateTime") or item.get("end", {}).get("date")
        return CalendarEvent(
            id=item.get("id", ""),
            title=item.get("summary", ""),
            start=start,
            end=end,
            location=item.get("location"),
            description=item.get("description"),
        )

def _scopes() -> List[str]:
    raw = os.getenv("CALENDAR_SCOPES", "")
    if raw:
        return raw.split()
    return SCOPES_DEFAULT


def default_google_client() -> GoogleCalendarClient:
    credentials_path = os.getenv("CALENDAR_CREDENTIALS_PATH", "credentials.json")
    token_path = os.getenv("CALENDAR_TOKEN_PATH", "apps/api/data/token.json")
    calendar_id = os.getenv("CALENDAR_ID", "primary")
    return GoogleCalendarClient(
        credentials_path=credentials_path,
        token_path=token_path,
        calendar_id=calendar_id,
    )
