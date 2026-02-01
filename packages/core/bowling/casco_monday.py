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
    team_name: Optional[str] = None,
    llm: Optional[OpenAIClient] = None,
    force_refresh: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    store = _store()
    pdf_url = os.getenv("CASCO_MONDAY_URL", DEFAULT_CASCO_MONDAY_URL)
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not force_refresh and not _should_refresh(fetch_state, pdf_url):
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
    standings_text, schedule_text = _split_sections(text)
    standings = _extract_standings_with_llm(standings_text, client)
    schedule, debug_info = _extract_schedule_from_pdf(pdf_bytes, debug=debug)
    if not schedule:
        schedule = _extract_schedule_from_text(schedule_text)
    _save_standings(store, DEFAULT_CACHE_KEY, standings)
    _save_schedule(store, DEFAULT_CACHE_KEY, schedule)
    _upsert_fetch_state(store, DEFAULT_CACHE_KEY, pdf_url, cache_path)
    response = {
        "status": "ok",
        "source_url": pdf_url,
        "source_path": cache_path,
        "cached": False,
        "standings": _filter_team(standings, team_name),
        "schedule": _filter_schedule(schedule, team_name),
    }
    if debug and debug_info:
        response["debug"] = debug_info
    return response


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
                "Each row must include: team, captain (optional), points. "
                "Return only the JSON array.\n\n"
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
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        extracted = _extract_json_array(content)
        if extracted is not None:
            return extracted
    logger.warning("Casco Monday standings LLM response not JSON")
    return []


def _extract_schedule_with_llm(text: str, llm: OpenAIClient) -> List[Dict[str, Any]]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured bowling schedules from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "From the schedule text below, return a JSON array of games. "
                "Each item must include: date, time, lane, team_a, team_b. "
                "Return only the JSON array.\n\n"
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
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        extracted = _extract_json_array(content)
        if extracted is not None:
            return extracted
    logger.warning("Casco Monday schedule LLM response not JSON")
    return []


def _extract_schedule_with_llm_chunks(
    text: str, llm: OpenAIClient
) -> List[Dict[str, Any]]:
    combined: List[Dict[str, Any]] = []
    for chunk in _chunk_text(text, limit=7000):
        chunk_rows = _extract_schedule_with_llm(chunk, llm)
        combined.extend(chunk_rows)
    return _dedupe_schedule(combined)


def _extract_json_object(content: str) -> Optional[Dict[str, Any]]:
    match = re.search(r"\{[\s\S]*\}", content)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _extract_json_array(content: str) -> Optional[List[Dict[str, Any]]]:
    match = re.search(r"\[[\s\S]*\]", content)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _split_sections(text: str) -> tuple[str, str]:
    lowered = text.lower()
    schedule_idx = lowered.find("schedule")
    standings_idx = lowered.find("standings")
    if schedule_idx == -1 and standings_idx == -1:
        return text, text
    if schedule_idx == -1:
        return text, text
    if standings_idx == -1:
        return text, text
    if standings_idx < schedule_idx:
        standings_text = text[standings_idx:schedule_idx]
        schedule_text = text[schedule_idx:]
    else:
        standings_text = text[standings_idx:]
        schedule_text = text[schedule_idx:standings_idx]
    return standings_text, schedule_text


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


def _extract_schedule_from_pdf(
    data: bytes, debug: bool = False
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    _ensure_pdf_available()
    schedule: List[Dict[str, Any]] = []
    debug_info: Optional[Dict[str, Any]] = None
    table_debugs: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page_idx, page in enumerate(pdf.pages):
            tables = _extract_tables_from_page(page)
            for table_idx, table in enumerate(tables):
                parsed, table_debug = _parse_schedule_table(table, debug=debug)
                if debug and table_debug:
                    table_debug["page_idx"] = page_idx
                    table_debug["table_idx"] = table_idx
                    table_debugs.append(table_debug)
                if parsed:
                    schedule.extend(parsed)
    if debug and table_debugs:
        debug_info = {"tables": table_debugs}
    return _dedupe_schedule(schedule), debug_info


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


def _parse_schedule_table(
    table: List[List[Optional[str]]], debug: bool = False
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    rows = [[_clean_cell(cell) for cell in row] for row in table]
    if not rows:
        return [], None
    week_idx = _find_row_index(rows, "week")
    date_idx = _find_row_index(rows, "date")
    date_row = rows[date_idx] if date_idx is not None else []
    date_columns = _extract_date_columns(date_row)
    if not date_columns:
        return [], _build_schedule_debug(rows, week_idx, date_idx, date_columns) if debug else ([], None)
    start_idx = (date_idx + 1) if date_idx is not None else 0
    team_map = _build_team_map(rows, start_idx)
    schedule: List[Dict[str, Any]] = []
    for row in rows[start_idx:]:
        team_number, team_name = _parse_team_row_header(row)
        if not team_name:
            continue
        for col_idx, date_value in date_columns.items():
            cell = row[col_idx] if col_idx < len(row) else ""
            if not cell or "postponed" in cell.lower():
                continue
            time_value, lane_value, opponent_number = _parse_schedule_cell(cell)
            if not time_value:
                continue
            opponent_name = team_map.get(opponent_number) if opponent_number else None
            schedule.append(
                {
                    "date": date_value,
                    "time": time_value,
                    "lane": lane_value,
                    "team_a": team_name,
                    "team_b": opponent_name or opponent_number or "",
                    "team_number": team_number,
                    "opponent_number": opponent_number,
                }
            )
    if debug:
        return schedule, _build_schedule_debug(rows, week_idx, date_idx, date_columns)
    return schedule, None


def _build_schedule_debug(
    rows: List[List[str]],
    week_idx: Optional[int],
    date_idx: Optional[int],
    date_columns: Optional[Dict[int, str]] = None,
    limit_rows: int = 6,
    limit_cols: int = 10,
) -> Dict[str, Any]:
    preview = []
    for row in rows[:limit_rows]:
        preview.append(row[:limit_cols])
    return {
        "row_count": len(rows),
        "week_idx": week_idx,
        "date_idx": date_idx,
        "date_columns": date_columns or {},
        "preview": preview,
    }


def _find_row_index(rows: List[List[str]], label: str) -> Optional[int]:
    label_lower = label.lower()
    for idx, row in enumerate(rows):
        joined = " ".join(cell.lower() for cell in row if cell)
        if label_lower in joined:
            return idx
    return None


def _extract_date_columns(row: List[str]) -> Dict[int, str]:
    date_columns: Dict[int, str] = {}
    for idx, cell in enumerate(row):
        match = re.search(r"\b\d{1,2}/\d{1,2}\b", cell)
        if match:
            date_columns[idx] = match.group(0)
    return date_columns


def _build_team_map(rows: List[List[str]], start_idx: int) -> Dict[str, str]:
    team_map: Dict[str, str] = {}
    for row in rows[start_idx:]:
        team_number, team_name = _parse_team_row_header(row)
        if team_number and team_name:
            team_map[team_number] = team_name
    return team_map


def _parse_team_row_header(row: List[str]) -> tuple[Optional[str], Optional[str]]:
    if not row:
        return None, None
    first = row[0]
    second = row[1] if len(row) > 1 else ""
    if first.isdigit():
        team_number = first
        team_name = second or None
        return team_number, team_name
    if second.isdigit():
        team_number = second
        team_name = first or None
        return team_number, team_name
    return None, first or None


def _parse_schedule_cell(cell: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    lines = [line.strip() for line in cell.splitlines() if line.strip()]
    text = " ".join(lines)
    time_match = re.search(r"\b\d{1,2}:\d{2}\b", text)
    lane_match = re.search(r"\b\d{1,2}\b", text)
    time_value = time_match.group(0) if time_match else None
    lane_value = lane_match.group(0) if lane_match else None
    opponent_number = None
    for line in lines[1:]:
        if line.isdigit():
            opponent_number = line
            break
    if opponent_number is None:
        digit_candidates = [token for token in lines if token.isdigit()]
        if digit_candidates:
            opponent_number = digit_candidates[-1]
    return time_value, lane_value, opponent_number


def _extract_schedule_from_text(text: str) -> List[Dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    date_idx = _find_line_index(lines, "date:")
    if date_idx is None:
        return []
    dates = _extract_dates_from_line(lines[date_idx])
    if not dates:
        return []
    team_rows = _parse_team_rows(lines[date_idx + 1 :])
    team_map = {row["team_number"]: row["team_name"] for row in team_rows}
    schedule: List[Dict[str, Any]] = []
    for row in team_rows:
        time_lane_pairs = row["time_lane_pairs"]
        opponent_numbers = row["opponent_numbers"]
        for idx, date_value in enumerate(dates):
            if idx >= len(time_lane_pairs):
                continue
            time_value, lane_value = time_lane_pairs[idx]
            if not time_value:
                continue
            opponent_number = opponent_numbers[idx] if idx < len(opponent_numbers) else None
            opponent_name = team_map.get(opponent_number) if opponent_number else None
            schedule.append(
                {
                    "date": date_value,
                    "time": time_value,
                    "lane": lane_value,
                    "team_a": row["team_name"],
                    "team_b": opponent_name or opponent_number or "",
                    "team_number": row["team_number"],
                    "opponent_number": opponent_number,
                }
            )
    return _dedupe_schedule(schedule)


def _find_line_index(lines: List[str], needle: str) -> Optional[int]:
    needle_lower = needle.lower()
    for idx, line in enumerate(lines):
        if needle_lower in line.lower():
            return idx
    return None


def _extract_dates_from_line(line: str) -> List[str]:
    return re.findall(r"\b\d{1,2}/\d{1,2}\b", line)


def _parse_team_rows(lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        match = re.match(r"^(\d+)\s+(.+?)\s+(\d{1,2}:\d{2})", line)
        if not match:
            idx += 1
            continue
        team_number = match.group(1)
        team_name = match.group(2).strip()
        time_lane_pairs = _extract_time_lane_pairs(line)
        opponent_numbers: List[str] = []
        if idx + 1 < len(lines) and _looks_like_opponent_line(lines[idx + 1]):
            opponent_numbers = re.findall(r"\b\d+\b", lines[idx + 1])
            idx += 1
        rows.append(
            {
                "team_number": team_number,
                "team_name": team_name,
                "time_lane_pairs": time_lane_pairs,
                "opponent_numbers": opponent_numbers,
            }
        )
        idx += 1
    return rows


def _extract_time_lane_pairs(line: str) -> List[tuple[Optional[str], Optional[str]]]:
    pairs: List[tuple[Optional[str], Optional[str]]] = []
    tokens = re.findall(r"\d{1,2}:\d{2}|\b\d+\b", line)
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        if ":" in token and idx + 1 < len(tokens):
            time_value = token
            lane_value = tokens[idx + 1] if tokens[idx + 1].isdigit() else None
            pairs.append((time_value, lane_value))
            idx += 2
            continue
        idx += 1
    return pairs


def _looks_like_opponent_line(line: str) -> bool:
    return bool(re.fullmatch(r"[\d\s]+", line))


def _debug_log_schedule_lines(lines: List[str], limit: int = 40) -> None:
    logger.info("Casco schedule debug lines=%d", len(lines))
    for idx, line in enumerate(lines[:limit]):
        logger.info("Casco schedule line %d: %s", idx + 1, line)


def _looks_like_date(value: str) -> bool:
    return bool(re.search(r"\b\d{1,2}/\d{1,2}\b", value))


def _extract_date(value: str) -> Optional[str]:
    match = re.search(r"\b\d{1,2}/\d{1,2}\b", value)
    return match.group(0) if match else None


def _extract_lanes(value: str) -> Optional[str]:
    match = re.search(r"\b\d{1,2}\b", value)
    if not match:
        return None
    return match.group(0)


def _extract_teams(value: str, time_value: str, lanes: Optional[str]) -> List[str]:
    cleaned = value
    cleaned = cleaned.replace(time_value, "")
    if lanes:
        cleaned = cleaned.replace(lanes, "")
    cleaned = re.sub(r"\b\d{1,2}\b", "", cleaned)
    parts = [part.strip() for part in cleaned.split("  ") if part.strip()]
    if len(parts) >= 2:
        return [parts[0], parts[1]]
    tokens = [token.strip() for token in cleaned.split() if token.strip()]
    if len(tokens) < 2:
        return []
    mid = len(tokens) // 2
    return [" ".join(tokens[:mid]), " ".join(tokens[mid:])]


def _dedupe_schedule(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("date") or "").strip().lower(),
            str(row.get("time") or "").strip().lower(),
            str(row.get("team_a") or "").strip().lower(),
            str(row.get("team_b") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


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


def _clean_cell(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.replace("\n", " ").strip()


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
