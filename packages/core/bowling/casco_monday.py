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
    standings = _extract_standings_from_pdf(pdf_bytes)
    if not standings:
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


def get_casco_monday_team_summary(
    team_name: str,
    llm: Optional[OpenAIClient] = None,
    force_refresh: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    store = _store()
    pdf_url = os.getenv("CASCO_MONDAY_URL", DEFAULT_CASCO_MONDAY_URL)
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not force_refresh and os.path.exists(cache_path):
        with open(cache_path, "rb") as handle:
            pdf_bytes = handle.read()
    else:
        pdf_bytes = fetch_pdf(pdf_url)
        _write_cache_pdf(cache_path, pdf_bytes)
        _log_file_fetched(pdf_url, cache_path)
        _upsert_fetch_state(store, DEFAULT_CACHE_KEY, pdf_url, cache_path)
    text = _extract_pdf_text(pdf_bytes)
    standings = _extract_standings_from_pdf(pdf_bytes)
    if not standings:
        client = llm or OpenAIClient()
        standings = _extract_standings_with_llm(_split_sections(text)[0], client)
    schedule_text = _split_sections(text)[1]
    schedule_table_text = _extract_schedule_table_text(pdf_bytes)
    table_schedule, table_debug = _extract_team_schedule_from_table(
        pdf_bytes, team_name, debug=debug
    )
    schedule, _ = _extract_schedule_from_pdf(pdf_bytes, debug=False)
    if not schedule:
        schedule = _extract_schedule_from_text(schedule_text)
    client = llm or OpenAIClient()
    llm_input = schedule_text
    if schedule_table_text:
        llm_input = f"{schedule_text}\n\nSchedule table:\n{schedule_table_text}"
    llm_schedule = _extract_team_schedule_with_llm(llm_input, team_name, client)
    if llm_schedule:
        schedule = [
            {"date": row.get("date"), "time": row.get("time"), "lane": row.get("lane"), "team_a": team_name, "team_b": ""}
            for row in llm_schedule
            if row.get("date") and row.get("time")
        ]
    if table_schedule:
        schedule = [
            {
                "date": row.get("date"),
                "time": row.get("time"),
                "lane": row.get("lane"),
                "team_a": team_name,
                "team_b": "",
            }
            for row in table_schedule
        ]
    if not _extract_team_schedule_from_parsed(schedule, team_name):
        text_schedule = _extract_team_schedule_from_text(schedule_text, team_name)
        if text_schedule:
            schedule = [
                {"date": row.get("date"), "time": row.get("time"), "lane": row.get("lane"), "team_a": team_name, "team_b": ""}
                for row in text_schedule
            ]
    local_summary = _build_team_summary_from_parsed(team_name, standings, schedule)
    if local_summary is None:
        client = llm or OpenAIClient()
        summary = _extract_team_summary_with_llm(text, team_name, client)
        if summary.get("status") == "ok":
            summary["source_url"] = pdf_url
            summary["source_path"] = cache_path
            summary["cached"] = not force_refresh and not _should_refresh(fetch_state, pdf_url)
            if debug:
                summary["debug"] = _build_team_summary_debug(
                    team_name, standings, schedule, schedule_text
                )
        return summary
    response = {
        "status": "ok",
        "team_summary": local_summary,
        "source_url": pdf_url,
        "source_path": cache_path,
        "cached": not force_refresh and not _should_refresh(fetch_state, pdf_url),
    }
    if debug:
        response["debug"] = _build_team_summary_debug(
            team_name, standings, schedule, schedule_text, table_debug
        )
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


def _extract_team_summary_with_llm(
    text: str, team_name: str, llm: OpenAIClient
) -> Dict[str, Any]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract a single team's bowling standings and schedule from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                "From the PDF text below, return a JSON object for the team "
                f"\"{team_name}\" with:\n"
                "- team\n"
                "- points (number)\n"
                "- points_from_first (number)\n"
                "- schedule: array of items with date, time, lane\n"
                "The schedule dates come from the columns in the schedule table, "
                "and the time/lane come from the cell for the team's row. "
                "Return only the JSON object.\n\n"
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
            return {"status": "ok", "team_summary": parsed}
    except json.JSONDecodeError:
        extracted = _extract_json_object(content)
        if extracted is not None:
            return {"status": "ok", "team_summary": extracted}
    logger.warning("Casco Monday team summary LLM response not JSON")
    return {"status": "error", "error": "team_summary_parse_failed"}


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


def _extract_team_schedule_with_llm(
    text: str, team_name: str, llm: OpenAIClient
) -> List[Dict[str, Any]]:
    trimmed = _truncate_text(text)
    messages = [
        {
            "role": "system",
            "content": (
                "You extract a single team's bowling schedule from text. "
                "Return JSON only, with no extra commentary."
            ),
        },
        {
            "role": "user",
            "content": (
                f"From the schedule text below, return a JSON array for team "
                f"\"{team_name}\". Each item must include: date, time, lane. "
                "If a lane is missing, return null for lane. "
                "Use the schedule table if present for column alignment. "
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
    logger.warning("Casco Monday team schedule LLM response not JSON")
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
    if not schedule:
        schedule = _extract_schedule_from_text(_extract_pdf_text(data))
    if debug and table_debugs:
        debug_info = {"tables": table_debugs}
    return _dedupe_schedule(schedule), debug_info


def _extract_schedule_table_text(data: bytes) -> str:
    _ensure_pdf_available()
    rows: List[List[str]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            tables = _extract_tables_from_page(page)
            for table in tables:
                cleaned = [[_clean_cell(cell) for cell in row] for row in table]
                if _looks_like_schedule_table(cleaned):
                    rows = cleaned
                    break
            if rows:
                break
    if not rows:
        return ""
    lines: List[str] = []
    for row in rows:
        if not any(row):
            continue
        lines.append(" | ".join(cell for cell in row if cell))
    return "\n".join(lines)


def _looks_like_schedule_table(rows: List[List[str]]) -> bool:
    has_week = False
    has_date = False
    for row in rows:
        joined = " ".join(cell.lower() for cell in row if cell)
        if "week number" in joined:
            has_week = True
        if "date" in joined:
            has_date = True
    return has_week and has_date


def _extract_team_schedule_from_table(
    data: bytes, team_name: str, debug: bool = False
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    _ensure_pdf_available()
    team_key = team_name.strip().lower()
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            tables = _extract_tables_from_page(page)
            for table in tables:
                cleaned = [[_clean_cell(cell) for cell in row] for row in table]
                if not _looks_like_schedule_table(cleaned):
                    continue
                date_row = _find_best_date_row(cleaned)
                if date_row is None:
                    continue
                expanded_date_columns = _expand_date_columns(date_row)
                if not expanded_date_columns:
                    continue
                start_idx = cleaned.index(date_row) + 1
                for idx, row in enumerate(cleaned[start_idx:], start=start_idx):
                    if not any(row):
                        continue
                    if any(team_key == cell.strip().lower() for cell in row if cell):
                        time_row = row
                        if _count_time_tokens(time_row) == 0 and idx > 0:
                            prev_row = cleaned[idx - 1]
                            if _count_time_tokens(prev_row) > 0:
                                time_row = prev_row
                        schedule = _build_team_schedule_from_row_with_columns(
                            time_row, expanded_date_columns
                        )
                        if debug:
                            return schedule, {
                                "date_row": date_row,
                                "team_row": row,
                                "time_row": time_row,
                                "date_columns": expanded_date_columns,
                            }
                        return schedule, None
    return [], None


def _build_team_schedule_from_row_with_columns(
    row: List[str], date_columns: List[tuple[int, str]]
) -> List[Dict[str, Any]]:
    schedule: List[Dict[str, Any]] = []
    for col_idx, date_value in date_columns:
        cell = row[col_idx] if col_idx < len(row) else ""
        time_value, lane_value, _ = _parse_schedule_cell(cell)
        if time_value is None and lane_value is None:
            numeric = _extract_cell_numeric(cell)
            lane_value = numeric
        schedule.append(
            {
                "date": date_value,
                "time": time_value,
                "lane": lane_value,
            }
        )
    return schedule


def _find_best_date_row(rows: List[List[str]]) -> Optional[List[str]]:
    best_row = None
    best_count = 0
    for row in rows:
        count = len(_extract_date_columns(row))
        if count > best_count:
            best_count = count
            best_row = row
    return best_row


def _expand_date_columns(row: List[str]) -> List[tuple[int, str]]:
    expanded: List[tuple[int, str]] = []
    for idx, cell in enumerate(row):
        if not cell:
            continue
        dates = re.findall(r"\b\d{1,2}/\d{1,2}\b", cell)
        if not dates:
            continue
        for offset, date_value in enumerate(dates):
            expanded.append((idx + offset, date_value))
    return expanded


def _extract_cell_numeric(cell: str) -> Optional[str]:
    if not cell:
        return None
    match = re.search(r"\b\d{1,2}\b", cell)
    return match.group(0) if match else None


def _count_time_tokens(row: List[str]) -> int:
    count = 0
    for cell in row:
        if cell and re.search(r"\b\d{1,2}:\d{2}\b", cell):
            count += 1
    return count


def _extract_standings_from_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    standings: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:  # type: ignore[union-attr]
        for page in pdf.pages:
            tables = _extract_tables_from_page(page)
            for table in tables:
                parsed = _parse_standings_table(table)
                if parsed:
                    standings.extend(parsed)
    return standings


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
    idx = start_idx
    while idx < len(rows):
        row = rows[idx]
        team_number, team_name = _parse_team_row_header(row)
        if not team_name:
            idx += 1
            continue
        opponent_row = (
            rows[idx + 1] if idx + 1 < len(rows) and _looks_like_opponent_row(rows[idx + 1]) else None
        )
        for col_idx, date_value in date_columns.items():
            cell = row[col_idx] if col_idx < len(row) else ""
            if not cell or "postponed" in cell.lower():
                continue
            time_value, lane_value, opponent_number = _parse_schedule_cell(cell)
            if opponent_row and col_idx < len(opponent_row):
                opponent_cell = opponent_row[col_idx]
                if opponent_cell and opponent_cell.strip().isdigit():
                    opponent_number = opponent_cell.strip()
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
        idx += 2 if opponent_row else 1
    if debug:
        return schedule, _build_schedule_debug(rows, week_idx, date_idx, date_columns)
    return schedule, None


def _parse_standings_table(table: List[List[Optional[str]]]) -> List[Dict[str, Any]]:
    rows = [[_clean_cell(cell) for cell in row] for row in table]
    if not rows:
        return []
    header_idx = _find_standings_header_index(rows)
    if header_idx is None:
        return []
    header = rows[header_idx]
    tm_idx = _find_header_col(header, ["tm", "tm #", "team #"])
    name_idx = _find_header_col(header, ["name", "team"])
    captain_idx = _find_header_col(header, ["captain"])
    points_idx = _find_header_col(header, ["points", "pts"])
    parsed: List[Dict[str, Any]] = []
    for row in rows[header_idx + 1 :]:
        if not any(row):
            continue
        if tm_idx is None or name_idx is None or points_idx is None:
            continue
        tm_value = row[tm_idx] if tm_idx < len(row) else ""
        team = row[name_idx] if name_idx < len(row) else ""
        points_value = row[points_idx] if points_idx < len(row) else ""
        if not tm_value or not tm_value.strip().isdigit():
            continue
        if not team or not points_value:
            continue
        points = _to_points(points_value)
        if points is None:
            continue
        parsed.append(
            {
                "team_number": tm_value,
                "team": team,
                "captain": row[captain_idx] if captain_idx is not None and captain_idx < len(row) else None,
                "points": points,
            }
        )
    return parsed


def _find_standings_header_index(rows: List[List[str]]) -> Optional[int]:
    for idx, row in enumerate(rows):
        lowered_cells = [cell.lower() for cell in row if cell]
        if (
            any("tm" in cell for cell in lowered_cells)
            and any("name" in cell for cell in lowered_cells)
            and any("captain" in cell for cell in lowered_cells)
            and any("points" in cell for cell in lowered_cells)
        ):
            return idx
    return None


def _find_header_col(header: List[str], keys: List[str]) -> Optional[int]:
    for idx, cell in enumerate(header):
        normalized = cell.lower().strip()
        for key in keys:
            if key in normalized:
                return idx
    return None


def _to_points(value: str) -> Optional[float]:
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _build_team_summary_from_parsed(
    team_name: str,
    standings: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    team_key = team_name.strip().lower()
    normalized: List[Dict[str, Any]] = []
    for row in standings:
        points = row.get("points")
        points_value = _to_points(str(points)) if points is not None else None
        normalized.append(
            {
                "team": row.get("team"),
                "points": points_value,
            }
        )
    normalized = [row for row in normalized if row.get("team") and row.get("points") is not None]
    if not normalized:
        return None
    sorted_rows = sorted(
        normalized,
        key=lambda row: float(row.get("points") or 0),
        reverse=True,
    )
    top_points = float(sorted_rows[0]["points"])
    team_row = None
    for idx, row in enumerate(sorted_rows, start=1):
        if str(row["team"]).strip().lower() == team_key:
            team_row = {"position": idx, "team": row["team"], "points": row["points"]}
            break
    if team_row is None:
        return None
    team_row["points_from_first"] = round(top_points - float(team_row["points"]), 2)
    team_schedule = _extract_team_schedule_from_parsed(schedule, team_name)
    return {
        "team": team_row["team"],
        "position": team_row["position"],
        "points": team_row["points"],
        "points_from_first": team_row["points_from_first"],
        "schedule": team_schedule,
    }


def _build_team_summary_debug(
    team_name: str,
    standings: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
    schedule_text: str,
    table_debug: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    team_key = team_name.strip().lower()
    standings_sample = [row.get("team") for row in standings[:15] if row.get("team")]
    team_schedule = _extract_team_schedule_from_parsed(schedule, team_name)
    schedule_sample = [
        {
            "date": row.get("date"),
            "time": row.get("time"),
            "lane": row.get("lane"),
            "team_a": row.get("team_a"),
            "team_b": row.get("team_b"),
        }
        for row in team_schedule[:10]
    ]
    debug_payload = {
        "standings_count": len(standings),
        "standings_sample": standings_sample,
        "schedule_count": len(schedule),
        "team_schedule_count": len(team_schedule),
        "team_schedule_sample": schedule_sample,
    }
    debug_payload.update(_debug_schedule_text(schedule_text))
    if table_debug:
        debug_payload["table_debug"] = table_debug
    return debug_payload


def _debug_schedule_text(text: str) -> Dict[str, Any]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    date_idx = _find_line_index(lines, "date:")
    date_line = lines[date_idx] if date_idx is not None else None
    dates = _extract_dates_from_line(date_line or "")
    team_rows = _parse_team_rows_with_dates(
        lines[(date_idx + 1) :] if date_idx is not None else [], dates
    )
    team_row_samples = []
    for row in team_rows[:5]:
        team_row_samples.append(
            {
                "team_number": row.get("team_number"),
                "team_name": row.get("team_name"),
                "time_lane_pairs": row.get("time_lane_pairs"),
                "opponent_numbers": row.get("opponent_numbers"),
            }
        )
    return {
        "schedule_text_sample": lines[:10],
        "date_line": date_line,
        "date_columns": dates,
        "team_rows_count": len(team_rows),
        "team_rows_sample": team_row_samples,
    }


def _extract_team_schedule_from_parsed(
    schedule: List[Dict[str, Any]], team_name: str
) -> List[Dict[str, Any]]:
    team_key = team_name.strip().lower()
    return [
        {
            "date": row.get("date"),
            "time": row.get("time"),
            "lane": row.get("lane"),
            "team_a": row.get("team_a"),
            "team_b": row.get("team_b"),
        }
        for row in schedule
        if str(row.get("team_a") or "").strip().lower() == team_key
        or str(row.get("team_b") or "").strip().lower() == team_key
    ]


def _extract_team_schedule_from_text(
    text: str, team_name: str
) -> List[Dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    date_idx = _find_line_index(lines, "date:")
    if date_idx is None:
        return []
    dates = _extract_dates_from_line(lines[date_idx])
    if not dates:
        return []
    team_key = team_name.strip().lower()
    last_time_pairs: List[tuple[Optional[str], Optional[str]]] = []
    for line in lines[date_idx + 1 :]:
        if _line_has_time_pairs(line):
            last_time_pairs = _parse_time_lane_line(line, len(dates))
            continue
        if team_key not in line.lower():
            continue
        if not last_time_pairs:
            continue
        schedule: List[Dict[str, Any]] = []
        for idx, date_value in enumerate(dates):
            if idx >= len(last_time_pairs):
                continue
            time_value, lane_value = last_time_pairs[idx]
            if not time_value:
                continue
            schedule.append(
                {"date": date_value, "time": time_value, "lane": lane_value}
            )
        return schedule
    return []


def _looks_like_opponent_row(row: List[str]) -> bool:
    if not row:
        return False
    if any(re.search(r"[A-Za-z]", cell) for cell in row if cell):
        return False
    digit_count = sum(1 for cell in row if cell and cell.strip().isdigit())
    return digit_count >= 2


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
    time_value = time_match.group(0) if time_match else None
    lane_value = None
    if time_match:
        remainder = text[time_match.end() :]
        lane_match = re.search(r"\b\d{1,2}\b", remainder)
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
    team_rows = _parse_team_rows_with_dates(lines[date_idx + 1 :], dates)
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
    return _parse_team_rows_with_dates(lines, [])


def _parse_team_rows_with_dates(lines: List[str], dates: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    idx = 0
    date_len = len(dates)
    last_time_pairs: List[tuple[Optional[str], Optional[str]]] = []
    while idx < len(lines):
        line = lines[idx]
        if _line_has_time_pairs(line):
            last_time_pairs = _parse_time_lane_line(line, date_len)
            idx += 1
            continue
        if not line[0].isdigit():
            idx += 1
            continue
        if idx + 1 >= len(lines):
            idx += 1
            continue
        if not _looks_like_opponent_line(lines[idx + 1]):
            idx += 1
            continue
        team_number, team_name = _parse_team_header(line)
        time_lane_pairs = last_time_pairs
        if not team_name:
            idx += 1
            continue
        if not time_lane_pairs:
            idx += 1
            continue
        opponent_numbers = re.findall(r"\b\d+\b", lines[idx + 1])
        rows.append(
            {
                "team_number": team_number,
                "team_name": team_name,
                "time_lane_pairs": time_lane_pairs,
                "opponent_numbers": opponent_numbers,
            }
        )
        last_time_pairs = []
        idx += 2
    return rows


def _parse_team_header(line: str) -> tuple[Optional[str], Optional[str]]:
    tokens = line.split()
    if not tokens or not tokens[0].isdigit():
        return None, None
    team_number = tokens[0]
    team_name = " ".join(tokens[1:]).strip()
    return team_number, team_name


def _is_time_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}:\d{2}", token))


def _line_has_time_pairs(line: str) -> bool:
    return any(_is_time_token(token) for token in line.split())


def _parse_time_lane_line(
    line: str, date_len: int
) -> List[tuple[Optional[str], Optional[str]]]:
    tokens = line.split()
    pairs: List[tuple[Optional[str], Optional[str]]] = []
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        if not _is_time_token(token):
            idx += 1
            continue
        time_value = token
        lane_value = tokens[idx + 1] if idx + 1 < len(tokens) and tokens[idx + 1].isdigit() else None
        pairs.append((time_value, lane_value))
        idx += 2
    if date_len and len(pairs) > date_len:
        pairs = pairs[:date_len]
    return pairs


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
