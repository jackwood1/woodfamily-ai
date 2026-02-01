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
from packages.core.storage.base import BowlingFetchState, BowlingStatState
from packages.core.storage.sqlite import SQLiteListStore

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

DEFAULT_CASCO_STATS_URL = (
    "https://www.cascobaysports.com/images/schedules-standings/bowling/"
    "Bowling_Statistics_-_Jan_Mon_26_2.pdf"
)
DEFAULT_CACHE_KEY = "casco_monday_stats"


def get_casco_monday_bowlers(
    team_name: Optional[str] = None,
    player_name: Optional[str] = None,
    force_refresh: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    store = _store()
    stats_url = os.getenv("CASCO_MONDAY_STATS_URL", DEFAULT_CASCO_STATS_URL)
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not force_refresh and not _should_refresh(fetch_state, stats_url):
        cached = _load_cached_bowlers(store, DEFAULT_CACHE_KEY, team_name, player_name)
        if cached is not None:
            return {
                "status": "ok",
                "stats_url": stats_url,
                "stats_path": cache_path,
                "count": len(cached),
                "cached": True,
                "bowlers": cached,
            }

    pdf_bytes = fetch_pdf(stats_url)
    _write_cache_pdf(cache_path, pdf_bytes)
    _log_file_fetched(stats_url, cache_path)
    client = OpenAIClient()
    bowlers, debug_info = _parse_stats_pdf(pdf_bytes, client, debug=debug)
    _save_bowler_stats(store, DEFAULT_CACHE_KEY, bowlers)
    _upsert_fetch_state(store, DEFAULT_CACHE_KEY, stats_url, cache_path)
    filtered = _load_cached_bowlers(store, DEFAULT_CACHE_KEY, team_name, player_name) or []
    response = {
        "status": "ok",
        "stats_url": stats_url,
        "stats_path": cache_path,
        "count": len(filtered),
        "cached": False,
        "bowlers": filtered,
    }
    if debug and debug_info:
        response["debug"] = debug_info
    return response


def _parse_stats_pdf(
    data: bytes, llm: OpenAIClient, debug: bool = False
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    if not rows:
        text = _extract_pdf_text(data)
        local = _parse_stats_text(text)
        parsed = _merge_with_local_stats(local, _extract_stats_with_llm(text, llm))
        return (
            parsed,
            _build_debug(parsed, rows, text, local_rows=local) if debug else (parsed, None),
        )
    parsed: List[Dict[str, Any]] = []
    current_team: Optional[str] = None
    for row in rows:
        cells = [cell for cell in row if cell]
        if not cells:
            continue
        team_name, team_avg = _split_name_avg(cells[0])
        if _looks_like_team_header(cells):
            current_team = _normalize_team(team_name or cells[0])
            continue
        if _looks_like_bowler_header(cells):
            continue
        if current_team and _looks_like_bowler_row(cells):
            if len(cells) == 1:
                bowler = _normalize_name(team_name)
                average = _to_int(team_avg)
            else:
                bowler = _normalize_name(cells[0])
                average = _to_int(cells[1])
            if bowler and average is not None:
                parsed.append(
                    {
                        "bowler": bowler,
                        "team": current_team,
                        "average": average,
                        "raw_row": row,
                    }
                )
    if parsed:
        return parsed, _build_debug(parsed, rows, None) if debug else (parsed, None)
    text = _extract_pdf_text(data)
    local = _parse_stats_text(text)
    parsed = _merge_with_local_stats(local, _extract_stats_with_llm(text, llm))
    return (
        parsed,
        _build_debug(parsed, rows, text, local_rows=local) if debug else (parsed, None),
    )


def _extract_rows(data: bytes) -> List[List[str]]:
    _ensure_pdf_available()
    rows: List[List[str]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            tables = _extract_tables_from_page(page)
            for table in tables:
                rows.extend(_table_to_rows(table))
    return rows


def _extract_tables_from_page(page: Any) -> List[List[List[Optional[str]]]]:
    settings_list = [
        None,
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {"vertical_strategy": "lines", "horizontal_strategy": "text"},
        {"vertical_strategy": "text", "horizontal_strategy": "lines"},
    ]
    for settings in settings_list:
        try:
            tables = (
                page.extract_tables()
                if settings is None
                else page.extract_tables(table_settings=settings)
            )
        except TypeError:
            tables = page.extract_tables()
        if tables:
            return tables
    return []


def _table_to_rows(table: List[List[Optional[str]]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for raw_row in table:
        cells = [_clean_cell(cell) for cell in raw_row]
        if any(cells):
            rows.append(cells)
    return rows


def _looks_like_team_header(cells: List[str]) -> bool:
    if len(cells) == 1:
        name, avg = _split_name_avg(cells[0])
        return bool(name) and avg is not None and avg >= 300
    if len(cells) < 2:
        return False
    if not cells[0]:
        return False
    if _is_numeric(cells[0]):
        return False
    if "name" in cells[0].lower():
        return False
    if not _is_numeric(cells[1]):
        return False
    remaining = [cell for cell in cells[2:] if cell]
    return len(remaining) == 0


def _looks_like_bowler_header(cells: List[str]) -> bool:
    joined = " ".join(cell.lower() for cell in cells if cell)
    return "name" in joined and ("avg" in joined or "average" in joined)


def _looks_like_bowler_row(cells: List[str]) -> bool:
    if len(cells) == 1:
        name, avg = _split_name_avg(cells[0])
        return bool(name) and avg is not None and avg < 300
    if len(cells) < 2:
        return False
    if not cells[0] or _is_numeric(cells[0]):
        return False
    return _to_int(cells[1]) is not None


def _is_numeric(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", value.strip().replace(",", "")))


def _normalize_name(value: str) -> Optional[str]:
    cleaned = value.replace("\u00a0", " ").strip()
    return cleaned or None


def _normalize_team(value: str) -> Optional[str]:
    return _normalize_name(value)


def _split_name_avg(value: str) -> tuple[Optional[str], Optional[int]]:
    cleaned = value.replace("|", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    match = re.match(r"^(?P<name>.+?)\s+(?P<num>\d{2,4})$", cleaned)
    if not match:
        return None, None
    name = match.group("name").strip()
    avg = _to_int(match.group("num"))
    return (name or None), avg


def _clean_cell(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.replace("\n", " ").strip()


def _extract_pdf_text(data: bytes) -> str:
    _ensure_pdf_available()
    chunks: List[str] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text:
                chunks.append(text)
    return "\n".join(chunks)


def _parse_stats_text(text: str) -> List[Dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    parsed: List[Dict[str, Any]] = []
    current_team: Optional[str] = None
    for line in lines:
        cleaned = re.sub(r"\s+", " ", line.replace("|", " "))
        if _looks_like_header_text(cleaned):
            continue
        name, avg = _extract_name_and_value(cleaned)
        if not name or avg is None:
            continue
        if avg >= 300:
            current_team = _normalize_team(name)
            continue
        if current_team:
            parsed.append(
                {
                    "bowler": name,
                    "team": current_team,
                    "average": avg,
                    "raw_line": line,
                }
            )
    return parsed


def _extract_stats_with_llm(text: str, llm: OpenAIClient) -> List[Dict[str, Any]]:
    trimmed = _truncate_text(text)
    parsed = _extract_stats_with_llm_once(trimmed, llm)
    parsed = _filter_bowler_rows(parsed)
    if parsed:
        return parsed
    combined: List[Dict[str, Any]] = []
    for chunk in _chunk_text(text, limit=8000):
        chunk_rows = _filter_bowler_rows(_extract_stats_with_llm_once(chunk, llm))
        combined.extend(chunk_rows)
    return _filter_bowler_rows(combined)


def _extract_stats_with_llm_once(text: str, llm: OpenAIClient) -> List[Dict[str, Any]]:
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured bowling statistics from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "From the stats text below, return a JSON array of bowler rows. "
                "Each row must include: bowler, team, average. "
                "Do NOT return team-only rows (team averages are typically 300+). "
                "If the PDF groups bowlers under a team header, assign that team "
                "to each bowler row. "
                "Include any of these if present: handicap, wins, losses, high_game, "
                "high_series, points. Return only the JSON array.\n\n"
                f"Text:\n{text}"
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
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        extracted = _extract_json_array(content)
        if extracted is not None:
            return extracted
    logger.warning("Casco Monday stats LLM response not JSON")
    return []


def _filter_bowler_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        bowler = _to_str(row.get("bowler"))
        team = _to_str(row.get("team"))
        average = _to_int(row.get("average"))
        if not bowler or not team or average is None:
            continue
        if bowler.lower() == team.lower():
            continue
        if average >= 300:
            continue
        filtered.append(row)
    return filtered


def _merge_with_local_stats(
    local_rows: List[Dict[str, Any]], llm_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not local_rows:
        return _filter_bowler_rows(llm_rows)
    local_map: Dict[str, Dict[str, Any]] = {}
    for row in local_rows:
        bowler = _to_str(row.get("bowler"))
        if not bowler:
            continue
        local_map[bowler.lower()] = row
    merged: List[Dict[str, Any]] = [row.copy() for row in local_rows]
    for row in llm_rows:
        bowler = _to_str(row.get("bowler"))
        if not bowler:
            continue
        local = local_map.get(bowler.lower())
        if local:
            merged_row = local.copy()
            for key, value in row.items():
                if value is None:
                    continue
                if key == "team" and (value == "Unknown" or not _to_str(value)):
                    continue
                merged_row[key] = value
            local_map[bowler.lower()] = merged_row
    merged = list(local_map.values())
    return _filter_bowler_rows(merged)


def _extract_name_and_value(line: str) -> tuple[Optional[str], Optional[int]]:
    cleaned = line.replace("|", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    match = re.search(r"\b\d{2,4}\b", cleaned)
    if not match:
        return None, None
    name = cleaned[: match.start()].strip()
    if not name or not re.search(r"[A-Za-z]", name):
        return None, None
    return name, _to_int(match.group(0))


def _looks_like_header_text(line: str) -> bool:
    lowered = line.lower()
    header_keywords = ["statistics", "standings", "captain", "points", "tm #", "name", "avg"]
    return any(keyword in lowered for keyword in header_keywords)


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


def _chunk_text(text: str, limit: int = 8000) -> List[str]:
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


def _build_debug(
    parsed: List[Dict[str, Any]],
    rows: List[List[str]],
    text: Optional[str],
    local_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    sample_rows: List[List[str]] = []
    for row in rows[:6]:
        sample_rows.append(row[:6])
    sample_lines: List[str] = []
    if text:
        for line in [line.strip() for line in text.splitlines() if line.strip()][:10]:
            sample_lines.append(line)
    debug_payload = {
        "parsed_count": len(parsed),
        "row_count": len(rows),
        "row_sample": sample_rows,
        "text_sample": sample_lines,
    }
    if local_rows is not None:
        debug_payload["local_count"] = len(local_rows)
        debug_payload["local_sample_bowlers"] = [
            row.get("bowler") for row in local_rows[:30] if row.get("bowler")
        ]
    if text:
        debug_payload["gino_lines"] = [
            line
            for line in text.splitlines()
            if "gino" in line.lower()
        ][:10]
    return debug_payload


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_db)
    return SQLiteListStore(db_path=db_path)


def _cache_path() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cache_dir = os.path.join(base_dir, "apps", "api", "data", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "casco_monday_stats.pdf")


def _should_refresh(fetch_state: Optional[BowlingFetchState], stats_url: str) -> bool:
    if not fetch_state or not fetch_state.last_fetch_at:
        return True
    try:
        last_fetch = datetime.fromisoformat(fetch_state.last_fetch_at)
    except ValueError:
        return True
    refresh_days = int(os.getenv("BOWLING_REFRESH_DAYS", "7"))
    if datetime.now() - last_fetch > timedelta(days=refresh_days):
        return True
    if _basename(fetch_state.stats_url) != _basename(stats_url):
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
    store: SQLiteListStore, cache_key: str, stats_url: str, cache_path: str
) -> None:
    timestamp = datetime.now().isoformat()
    store.upsert_bowling_fetch(
        BowlingFetchState(
            league_key=cache_key,
            last_fetch_at=timestamp,
            stats_url=stats_url,
            schedule_url=None,
            standings_url=None,
            file_path=cache_path,
        )
    )


def _save_bowler_stats(
    store: SQLiteListStore, cache_key: str, bowlers: List[Dict[str, Any]]
) -> None:
    timestamp = datetime.now().isoformat()
    stats: List[BowlingStatState] = []
    for bowler in bowlers:
        stats.append(
            BowlingStatState(
                league_key=cache_key,
                team_name=_to_str(bowler.get("team")),
                player_name=_to_str(bowler.get("bowler")),
                average=_to_int(bowler.get("average")),
                handicap=None,
                wins=None,
                losses=None,
                high_game=None,
                high_series=None,
                points=None,
                raw=bowler,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    store.save_bowling_stats(cache_key, stats)


def _load_cached_bowlers(
    store: SQLiteListStore,
    cache_key: str,
    team_name: Optional[str],
    player_name: Optional[str],
) -> Optional[List[Dict[str, Any]]]:
    stats = store.list_bowling_stats(cache_key, team_name=team_name, player_name=player_name)
    if not stats:
        return None
    return [_stat_to_dict(stat) for stat in stats]


def _stat_to_dict(stat: BowlingStatState) -> Dict[str, Any]:
    return {
        "bowler": stat.player_name,
        "team": stat.team_name,
        "average": stat.average,
        "raw": stat.raw,
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


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).replace("\u00a0", " ").strip()
    return cleaned or None


def _log_file_fetched(stats_url: str, cache_path: str) -> None:
    logger.info("Casco Monday stats fetched url=%s path=%s", stats_url, cache_path)


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
