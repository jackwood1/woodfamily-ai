from __future__ import annotations

import io
import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from packages.core.bowling.fetcher import fetch_pdf
from packages.core.llm.openai_client import OpenAIClient
from packages.core.storage.base import BowlingFetchState, BowlingMatchState, BowlingStatState
from packages.core.storage.sqlite import SQLiteListStore

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

DEFAULT_CASCO_MONDAY_URL = (
    "https://www.cascobaysports.com/images/schedules-standings/bowling/"
    "Bowling_Schedules_and_Standings_-_Monday_Jan_26_3.pdf"
)
DEFAULT_CACHE_KEY = "casco_monday_bayside"


def get_casco_monday(
    team_name: Optional[str] = None, llm: Optional[OpenAIClient] = None
) -> Dict[str, Any]:
    store = _store()
    pdf_url = os.getenv("CASCO_MONDAY_URL", DEFAULT_CASCO_MONDAY_URL)
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not _should_refresh(fetch_state, pdf_url):
        standings = _load_cached_standings(store, DEFAULT_CACHE_KEY, team_name)
        schedule = _load_cached_schedule(store, DEFAULT_CACHE_KEY, team_name)
        return {
            "status": "ok",
            "source_url": pdf_url,
            "source_path": cache_path,
            "cached": True,
            "standings": standings,
            "schedule": schedule,
        }

    pdf_bytes = fetch_pdf(pdf_url)
    _write_cache_pdf(cache_path, pdf_bytes)
    _log_file_fetched(pdf_url, cache_path)
    text = _extract_pdf_text(pdf_bytes)
    client = llm or OpenAIClient()
    parsed = _extract_tables_with_llm(text, client)
    standings = parsed.get("standings", [])
    schedule = parsed.get("schedule", [])
    _save_standings(store, DEFAULT_CACHE_KEY, standings)
    _save_schedule(store, DEFAULT_CACHE_KEY, schedule)
    _upsert_fetch_state(store, DEFAULT_CACHE_KEY, pdf_url, cache_path)
    return {
        "status": "ok",
        "source_url": pdf_url,
        "source_path": cache_path,
        "cached": False,
        "standings": _filter_team(standings, team_name),
        "schedule": _filter_schedule(schedule, team_name),
    }


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_db)
    return SQLiteListStore(db_path=db_path)


def _cache_path() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cache_dir = os.path.join(base_dir, "apps", "api", "data", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "casco_monday_schedule_standings.pdf")


def _should_refresh(fetch_state: Optional[BowlingFetchState], pdf_url: str) -> bool:
    if not fetch_state or not fetch_state.last_fetch_at:
        return True
    try:
        last_fetch = datetime.fromisoformat(fetch_state.last_fetch_at)
    except ValueError:
        return True
    refresh_days = int(os.getenv("BOWLING_REFRESH_DAYS", "7"))
    if datetime.now() - last_fetch > timedelta(days=refresh_days):
        return True
    if _basename(fetch_state.standings_url) != _basename(pdf_url):
        return True
    return False


def _basename(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return os.path.basename(value)


def _write_cache_pdf(cache_path: str, pdf_bytes: bytes) -> None:
    with open(cache_path, "wb") as handle:
        handle.write(pdf_bytes)


def _upsert_fetch_state(
    store: SQLiteListStore, cache_key: str, pdf_url: str, cache_path: str
) -> None:
    timestamp = datetime.now().isoformat()
    store.upsert_bowling_fetch(
        BowlingFetchState(
            league_key=cache_key,
            last_fetch_at=timestamp,
            stats_url=None,
            schedule_url=None,
            standings_url=pdf_url,
            file_path=cache_path,
        )
    )


def _extract_pdf_text(data: bytes) -> str:
    _ensure_pdf_available()
    chunks: List[str] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)
    return "\n".join(chunks)


def _extract_tables_with_llm(text: str, llm: OpenAIClient) -> Dict[str, Any]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured bowling standings and schedule from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "From the PDF text below, extract two arrays: standings and schedule. "
                "Standings items must include: team, captain (optional), points (number). "
                "Schedule items must include: date, time, lane, team_a, team_b. "
                "Return JSON object with keys standings and schedule.\n\n"
                f"Text:\n{trimmed}"
            ),
        },
    ]
    response = llm.chat(messages=messages, tools=[])
    content = (
        response.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        extracted = _extract_json_object(content)
        if extracted is not None:
            return extracted
    logger.warning("Casco Monday LLM response not JSON")
    return {"standings": [], "schedule": []}


def _extract_json_object(content: str) -> Optional[Dict[str, Any]]:
    match = re.search(r"\{[\s\S]*\}", content)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _truncate_text(text: str, limit: int = 20000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit]


def _save_standings(
    store: SQLiteListStore, cache_key: str, standings: List[Dict[str, Any]]
) -> None:
    timestamp = datetime.now().isoformat()
    stats: List[BowlingStatState] = []
    for row in standings:
        stats.append(
            BowlingStatState(
                league_key=cache_key,
                team_name=_to_str(row.get("team")),
                player_name=None,
                average=None,
                handicap=None,
                wins=None,
                losses=None,
                high_game=None,
                high_series=None,
                points=_to_float(row.get("points")),
                raw=row,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    store.save_bowling_stats(cache_key, stats)


def _save_schedule(
    store: SQLiteListStore, cache_key: str, schedule: List[Dict[str, Any]]
) -> None:
    timestamp = datetime.now().isoformat()
    matches: List[BowlingMatchState] = []
    for row in schedule:
        matches.append(
            BowlingMatchState(
                league_key=cache_key,
                match_date=_to_str(row.get("date")),
                match_time=_to_str(row.get("time")),
                lane=_to_str(row.get("lane")),
                team_a=_to_str(row.get("team_a")),
                team_b=_to_str(row.get("team_b")),
                raw=row,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    store.save_bowling_matches(cache_key, matches)


def _load_cached_standings(
    store: SQLiteListStore, cache_key: str, team_name: Optional[str]
) -> List[Dict[str, Any]]:
    stats = store.list_bowling_stats(cache_key, team_name=team_name, player_name=None)
    return [
        {
            "team": stat.team_name,
            "points": stat.points,
            "raw": stat.raw,
        }
        for stat in stats
    ]


def _load_cached_schedule(
    store: SQLiteListStore, cache_key: str, team_name: Optional[str]
) -> List[Dict[str, Any]]:
    matches = store.list_bowling_matches(cache_key, team_name=team_name)
    return [
        {
            "date": match.match_date,
            "time": match.match_time,
            "lane": match.lane,
            "team_a": match.team_a,
            "team_b": match.team_b,
            **(match.raw or {}),
        }
        for match in matches
    ]


def _filter_team(rows: List[Dict[str, Any]], team_name: Optional[str]) -> List[Dict[str, Any]]:
    if not team_name:
        return rows
    lowered = team_name.strip().lower()
    return [row for row in rows if lowered in str(row.get("team") or "").lower()]


def _filter_schedule(
    rows: List[Dict[str, Any]], team_name: Optional[str]
) -> List[Dict[str, Any]]:
    if not team_name:
        return rows
    lowered = team_name.strip().lower()
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        team_a = str(row.get("team_a") or "").lower()
        team_b = str(row.get("team_b") or "").lower()
        if lowered in team_a or lowered in team_b:
            filtered.append(row)
    return filtered


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).replace("\u00a0", " ").strip()
    return cleaned or None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        cleaned = cleaned.replace("(", "-").replace(")", "")
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _log_file_fetched(pdf_url: str, cache_path: str) -> None:
    logger.info("Casco Monday fetched url=%s path=%s", pdf_url, cache_path)


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
