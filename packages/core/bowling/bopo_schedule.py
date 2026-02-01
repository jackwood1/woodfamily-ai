from __future__ import annotations

import io
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
from datetime import datetime, timedelta

from packages.core.bowling.fetcher import fetch_html, fetch_pdf
from packages.core.llm.openai_client import OpenAIClient
from packages.core.storage.base import BowlingFetchState, BowlingMatchState
from packages.core.storage.sqlite import SQLiteListStore

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

DEFAULT_BOPO_URL = "https://www.bopostats.com/"
DEFAULT_CACHE_KEY = "bopo_schedule"


def get_bopo_schedule(team_name: str, llm: Optional[OpenAIClient] = None) -> Dict[str, Any]:
    if not team_name:
        return {"status": "error", "error": "team_name_required"}
    store = _store()
    schedule_url = _resolve_schedule_url()
    if not schedule_url:
        return {"status": "error", "error": "schedule_url_not_found"}
    cache_key = DEFAULT_CACHE_KEY
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(cache_key)
    if not _should_refresh(fetch_state, schedule_url):
        cached = _load_cached_matches(store, cache_key)
        if cached is not None:
            return {
                "status": "ok",
                "team_name": team_name,
                "schedule_url": schedule_url,
                "schedule_path": cache_path,
                "matches": _filter_matches(cached, team_name),
                "cached": True,
            }

    pdf_bytes = fetch_pdf(schedule_url)
    _write_cache_pdf(cache_path, pdf_bytes)
    _log_file_fetched(schedule_url, cache_path)
    text = _extract_pdf_text(pdf_bytes)
    client = llm or OpenAIClient()
    matches = _extract_team_schedule_with_llm(text, team_name, client)
    _save_matches(store, cache_key, matches)
    _upsert_fetch_state(store, cache_key, schedule_url, cache_path)
    return {
        "status": "ok",
        "team_name": team_name,
        "schedule_url": schedule_url,
        "schedule_path": cache_path,
        "matches": matches,
        "cached": False,
    }


def _resolve_schedule_url() -> Optional[str]:
    override = os.getenv("BOPO_SCHEDULE_URL")
    if override:
        return override
    html = fetch_html(DEFAULT_BOPO_URL)
    if not html:
        return None
    schedule_url = _find_schedule_link(html, DEFAULT_BOPO_URL)
    if schedule_url:
        return schedule_url
    pdf_links = _extract_pdf_links(html, DEFAULT_BOPO_URL)
    return pdf_links[0] if pdf_links else None


def _find_schedule_link(html: str, base_url: str) -> Optional[str]:
    anchor_pattern = re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+\.pdf)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html):
        label = match.group("label")
        if "schedule" not in label.lower():
            continue
        href = match.group("href")
        return urljoin(base_url, href)
    return None


def _extract_pdf_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    anchor_pattern = re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+\.pdf)[\"'][^>]*>",
        re.IGNORECASE,
    )
    for match in anchor_pattern.finditer(html):
        href = match.group("href")
        links.append(urljoin(base_url, href))
    return links


def _extract_pdf_text(data: bytes) -> str:
    _ensure_pdf_available()
    chunks: List[str] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)
    return "\n".join(chunks)


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_db)
    return SQLiteListStore(db_path=db_path)


def _cache_path() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cache_dir = os.path.join(base_dir, "apps", "api", "data", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "bopo_schedule.pdf")


def _should_refresh(fetch_state: Optional[BowlingFetchState], schedule_url: str) -> bool:
    if not fetch_state:
        return True
    if not fetch_state.last_fetch_at:
        return True
    try:
        last_fetch = datetime.fromisoformat(fetch_state.last_fetch_at)
    except ValueError:
        return True
    refresh_days = int(os.getenv("BOWLING_REFRESH_DAYS", "7"))
    if datetime.now() - last_fetch > timedelta(days=refresh_days):
        return True
    if _basename(fetch_state.schedule_url) != _basename(schedule_url):
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
    store: SQLiteListStore, cache_key: str, schedule_url: str, cache_path: str
) -> None:
    timestamp = datetime.now().isoformat()
    store.upsert_bowling_fetch(
        BowlingFetchState(
            league_key=cache_key,
            last_fetch_at=timestamp,
            stats_url=None,
            schedule_url=schedule_url,
            standings_url=None,
            file_path=cache_path,
        )
    )


def _log_file_fetched(schedule_url: str, cache_path: str) -> None:
    logger.info("BoPo schedule fetched url=%s path=%s", schedule_url, cache_path)


def _save_matches(store: SQLiteListStore, cache_key: str, matches: List[Dict[str, Any]]) -> None:
    timestamp = datetime.now().isoformat()
    states: List[BowlingMatchState] = []
    for match in matches:
        states.append(
            BowlingMatchState(
                league_key=cache_key,
                match_date=str(match.get("date") or ""),
                match_time=str(match.get("time") or ""),
                lane=str(match.get("lanes") or ""),
                team_a=str(match.get("team_a") or ""),
                team_b=str(match.get("team_b") or ""),
                raw=match,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    store.save_bowling_matches(cache_key, states)


def _load_cached_matches(
    store: SQLiteListStore, cache_key: str
) -> Optional[List[Dict[str, Any]]]:
    matches = store.list_bowling_matches(cache_key)
    if not matches:
        return None
    return [
        {
            "date": match.match_date,
            "time": match.match_time,
            "lanes": match.lane,
            "team_a": match.team_a,
            "team_b": match.team_b,
            **(match.raw or {}),
        }
        for match in matches
    ]


def _filter_matches(matches: List[Dict[str, Any]], team_name: str) -> List[Dict[str, Any]]:
    lowered = team_name.strip().lower()
    filtered: List[Dict[str, Any]] = []
    for match in matches:
        team_a = str(match.get("team_a") or "").lower()
        team_b = str(match.get("team_b") or "").lower()
        if lowered in team_a or lowered in team_b:
            filtered.append(match)
    return filtered


def _extract_team_schedule_with_llm(
    text: str, team_name: str, llm: OpenAIClient
) -> List[Dict[str, Any]]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured schedules from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "Given the schedule text below, return a JSON array of all matches "
                f"that include the team '{team_name}'. Each item must include: "
                "date, time, lanes, team_a, team_b, opponent. "
                "If none found, return an empty array.\n\n"
                f"Schedule text:\n{trimmed}"
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
        return json.loads(content)
    except json.JSONDecodeError:
        extracted = _extract_json_array(content)
        if extracted is not None:
            return extracted
    logger.warning("BoPo schedule LLM response not JSON")
    return []


def _extract_json_array(content: str) -> Optional[List[Dict[str, Any]]]:
    match = re.search(r"\[[\s\S]*\]", content)
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


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
