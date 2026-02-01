from __future__ import annotations

import io
import json
import logging
import os
import re
import socket
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from packages.core.bowling.fetcher import fetch_html, fetch_pdf
from packages.core.llm.openai_client import OpenAIClient
from packages.core.storage.base import BowlingFetchState, BowlingStatState
from packages.core.storage.sqlite import SQLiteListStore

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

DEFAULT_BOPO_URL = "https://www.bopostats.com/"
DEFAULT_CACHE_KEY = "bopo_standings"


def get_bopo_standings(
    day: Optional[str] = None,
    team_name: Optional[str] = None,
    llm: Optional[OpenAIClient] = None,
) -> Dict[str, Any]:
    store = _store()
    standings_url = _resolve_standings_url()
    if not standings_url:
        return {"status": "error", "error": "standings_url_not_found"}
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not _should_refresh(fetch_state, standings_url):
        cached = _load_cached_standings(store, DEFAULT_CACHE_KEY, day, team_name)
        if cached is not None:
            return {
                "status": "ok",
                "standings_url": standings_url,
                "standings_path": cache_path,
                "count": len(cached),
                "cached": True,
                "standings": cached,
            }

    pdf_bytes = fetch_pdf(standings_url)
    _write_cache_pdf(cache_path, pdf_bytes)
    _log_file_fetched(standings_url, cache_path)
    text = _extract_pdf_text(pdf_bytes)
    client = llm or OpenAIClient()
    try:
        standings = _extract_standings_with_llm_chunks(text, client)
    except (socket.timeout, TimeoutError) as exc:
        return {"status": "error", "error": "llm_timeout", "message": str(exc)}
    _save_standings(store, DEFAULT_CACHE_KEY, standings)
    _upsert_fetch_state(store, DEFAULT_CACHE_KEY, standings_url, cache_path)
    filtered = _load_cached_standings(store, DEFAULT_CACHE_KEY, day, team_name) or []
    return {
        "status": "ok",
        "standings_url": standings_url,
        "standings_path": cache_path,
        "count": len(filtered),
        "cached": False,
        "standings": filtered,
    }


def _resolve_standings_url() -> Optional[str]:
    override = os.getenv("BOPO_STANDINGS_URL")
    if override:
        return override
    html = fetch_html(DEFAULT_BOPO_URL)
    if not html:
        return None
    standings_url = _find_standings_link(html, DEFAULT_BOPO_URL)
    if standings_url:
        return standings_url
    section_url = _find_section_pdf_link(html, DEFAULT_BOPO_URL, "standings")
    if section_url:
        return section_url
    pdf_links = _extract_pdf_links(html, DEFAULT_BOPO_URL, keyword="standings")
    return pdf_links[0] if pdf_links else None


def _find_standings_link(html: str, base_url: str) -> Optional[str]:
    anchor_pattern = re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+\.pdf)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html):
        label = match.group("label")
        if "standings" not in label.lower():
            continue
        href = match.group("href")
        return urljoin(base_url, href)
    return None


def _find_section_pdf_link(html: str, base_url: str, section_label: str) -> Optional[str]:
    lowered = html.lower()
    index = lowered.find(section_label.lower())
    if index == -1:
        return None
    window = html[index : index + 20000]
    pdf_candidates = _extract_pdf_urls(window)
    if not pdf_candidates:
        return None
    preferred = _pick_preferred_pdf(pdf_candidates, keyword=section_label)
    return urljoin(base_url, preferred)


def _extract_pdf_urls(html: str) -> List[str]:
    pattern = re.compile(r"(https?://[^\s\"'>]+\.pdf|/[^\s\"'>]+\.pdf)", re.IGNORECASE)
    return pattern.findall(html)


def _pick_preferred_pdf(urls: List[str], keyword: str) -> str:
    keyword_lower = keyword.lower()
    for url in urls:
        if keyword_lower in url.lower():
            return url
    return urls[0]


def _extract_pdf_links(html: str, base_url: str, keyword: Optional[str] = None) -> List[str]:
    links: List[str] = []
    anchor_pattern = re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+\.pdf)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html):
        label = match.group("label") or ""
        if keyword and keyword not in label.lower():
            continue
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


def _extract_standings_with_llm_chunks(
    text: str, llm: OpenAIClient
) -> List[Dict[str, Any]]:
    combined: List[Dict[str, Any]] = []
    for chunk in _chunk_text(text, limit=7000):
        try:
            chunk_rows = _extract_standings_with_llm(chunk, llm)
        except (socket.timeout, TimeoutError) as exc:
            logger.warning("BoPo standings LLM chunk timeout: %s", exc)
            continue
        combined.extend(chunk_rows)
    return _dedupe_standings(combined)


def _extract_standings_with_llm(text: str, llm: OpenAIClient) -> List[Dict[str, Any]]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured bowling standings from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "From the standings text below, return a JSON array of rows. "
                "Each item must include: day, team, wins, losses, points, "
                "hi_series, team_avg, opp_avg, team_diff. "
                "If a field is missing, set it to null. "
                "Return only the JSON array.\n\n"
                f"Standings text:\n{trimmed}"
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
    logger.warning("BoPo standings LLM response not JSON")
    return []


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_db)
    return SQLiteListStore(db_path=db_path)


def _cache_path() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cache_dir = os.path.join(base_dir, "apps", "api", "data", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "bopo_standings.pdf")


def _should_refresh(fetch_state: Optional[BowlingFetchState], standings_url: str) -> bool:
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
    if _basename(fetch_state.standings_url) != _basename(standings_url):
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
    store: SQLiteListStore, cache_key: str, standings_url: str, cache_path: str
) -> None:
    timestamp = datetime.now().isoformat()
    store.upsert_bowling_fetch(
        BowlingFetchState(
            league_key=cache_key,
            last_fetch_at=timestamp,
            stats_url=None,
            schedule_url=None,
            standings_url=standings_url,
            file_path=cache_path,
        )
    )


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
                average=_to_int(row.get("team_avg")),
                handicap=None,
                wins=_to_int(row.get("wins")),
                losses=_to_int(row.get("losses")),
                high_game=None,
                high_series=_to_int(row.get("hi_series")),
                points=_to_float(row.get("points")),
                raw=row,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    store.save_bowling_stats(cache_key, stats)


def _load_cached_standings(
    store: SQLiteListStore,
    cache_key: str,
    day: Optional[str],
    team_name: Optional[str],
) -> Optional[List[Dict[str, Any]]]:
    stats = store.list_bowling_stats(cache_key, team_name=team_name, player_name=None)
    if not stats:
        return None
    rows = [_stat_to_dict(stat) for stat in stats]
    if day:
        lowered = day.strip().lower()
        rows = [row for row in rows if str(row.get("day") or "").lower() == lowered]
    return rows


def _stat_to_dict(stat: BowlingStatState) -> Dict[str, Any]:
    raw = stat.raw or {}
    return {
        "day": raw.get("day"),
        "team": stat.team_name,
        "wins": stat.wins,
        "losses": stat.losses,
        "points": stat.points,
        "hi_series": stat.high_series,
        "team_avg": stat.average,
        "opp_avg": raw.get("opp_avg"),
        "team_diff": raw.get("team_diff"),
        "raw": raw,
    }


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        cleaned = cleaned.replace("(", "-").replace(")", "")
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        cleaned = cleaned.replace("(", "-").replace(")", "")
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).replace("\u00a0", " ").strip()
    return cleaned or None


def _log_file_fetched(standings_url: str, cache_path: str) -> None:
    logger.info("BoPo standings fetched url=%s path=%s", standings_url, cache_path)


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


def _chunk_text(text: str, limit: int = 7000) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    lines = text.splitlines()
    current: List[str] = []
    current_len = 0
    for line in lines:
        if current_len + len(line) + 1 > limit and current:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks


def _dedupe_standings(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        day = str(row.get("day") or "").strip().lower()
        team = str(row.get("team") or "").strip().lower()
        key = (day, team)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
