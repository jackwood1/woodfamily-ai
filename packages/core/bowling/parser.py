from __future__ import annotations

import io
import logging
import re
from typing import Any, Dict, List, Optional

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


STAT_HEADERS = {
    "team": ["team", "team name"],
    "player": ["name", "player", "player name", "bowler", "bowler name"],
    "average": ["avg", "average", "avg."],
    "handicap": ["hdcp", "hcp", "handicap", "hdcp."],
    "wins": ["wins", "win"],
    "losses": ["losses", "loss"],
    "high_game": ["high game", "highgame", "hg", "high g"],
    "high_series": ["high series", "highseries", "hs", "high s"],
    "points": ["points", "pts", "tot pts", "total points"],
}

SCHEDULE_HEADERS = {
    "date": ["date"],
    "time": ["time", "start"],
    "lane": ["lane", "lanes", "lane #", "lanes #"],
    "team_a": ["team 1", "team a", "home", "team"],
    "team_b": ["team 2", "team b", "away", "opponent"],
}


logger = logging.getLogger(__name__)


def parse_stats_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    parsed = _parse_stats_rows(rows)
    if parsed:
        _log_parse_summary("stats", rows, parsed, used_text=False)
        return parsed
    text_rows = _extract_text_rows(data)
    parsed = _parse_stats_rows(text_rows)
    _log_parse_summary("stats", text_rows, parsed, used_text=True)
    return parsed


def parse_schedule_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    parsed = _parse_schedule_rows(rows)
    if parsed:
        _log_parse_summary("schedule", rows, parsed, used_text=False)
        return parsed
    text_rows = _extract_text_rows(data)
    parsed = _parse_schedule_rows(text_rows)
    _log_parse_summary("schedule", text_rows, parsed, used_text=True)
    return parsed


def _parse_stats_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        team_name = _normalize_name(_find_value(row, STAT_HEADERS["team"]))
        player_name = _normalize_name(_find_value(row, STAT_HEADERS["player"]))
        record = {
            "team_name": team_name,
            "player_name": player_name,
            "average": _parse_int(_find_value(row, STAT_HEADERS["average"])),
            "handicap": _parse_int(_find_value(row, STAT_HEADERS["handicap"])),
            "wins": _parse_int(_find_value(row, STAT_HEADERS["wins"])),
            "losses": _parse_int(_find_value(row, STAT_HEADERS["losses"])),
            "high_game": _parse_int(_find_value(row, STAT_HEADERS["high_game"])),
            "high_series": _parse_int(_find_value(row, STAT_HEADERS["high_series"])),
            "points": _parse_float(_find_value(row, STAT_HEADERS["points"])),
            "raw": row,
        }
        if _has_any_value(record):
            parsed.append(record)
    return parsed


def _parse_schedule_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        record = _parse_schedule_row(row)
        if record:
            parsed.append(record)
    return parsed


def _normalize_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("\u00a0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _extract_rows(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            tables = _extract_tables_from_page(page)
            for table in tables:
                rows.extend(_table_to_rows(table))
    return rows


def _extract_text_rows(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            tables = _extract_text_tables_from_page(page)
            for table in tables:
                rows.extend(_table_to_rows(table))
    return rows


def _table_to_rows(table: List[List[Optional[str]]]) -> List[Dict[str, Any]]:
    header: Optional[List[str]] = None
    header_index: Optional[int] = None
    cleaned_rows: List[List[str]] = []
    rows: List[Dict[str, Any]] = []
    for raw_row in table:
        cells = [(_clean_cell(cell) or "") for cell in raw_row]
        if not any(cells):
            continue
        cleaned_rows.append(cells)
        if header is None and _looks_like_header(cells):
            header = [_normalize_header(cell) for cell in cells]
            header_index = len(cleaned_rows) - 1
            continue
    if header is None:
        max_cols = max((len(row) for row in cleaned_rows), default=0)
        header = [f"col_{idx}" for idx in range(max_cols)]
        start_idx = 0
    else:
        start_idx = (header_index or 0) + 1
    for cells in cleaned_rows[start_idx:]:
        row_dict = {
            header[idx]: (cells[idx] if idx < len(cells) else "")
            for idx in range(len(header))
        }
        rows.append(row_dict)
    return rows


def _looks_like_header(cells: List[str]) -> bool:
    joined = " ".join(cell.lower() for cell in cells if cell)
    keywords = [
        "team",
        "name",
        "avg",
        "average",
        "lane",
        "date",
        "time",
        "hdcp",
        "handicap",
        "points",
        "home",
        "away",
        "opponent",
    ]
    return any(keyword in joined for keyword in keywords)


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


def _extract_text_tables_from_page(page: Any) -> List[List[List[str]]]:
    try:
        text = page.extract_text() or ""
    except Exception:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    table: List[List[str]] = []
    for line in lines:
        cells = [part.strip() for part in re.split(r"\s{2,}", line) if part.strip()]
        if cells:
            table.append(cells)
    return [table] if table else []


def _normalize_header(value: str) -> str:
    value = _clean_cell(value).lower()
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _clean_cell(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.replace("\n", " ").strip()


def _find_value(row: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        for header, value in row.items():
            if key in header:
                return value
    return None


def _parse_schedule_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    date_value = _find_value(row, SCHEDULE_HEADERS["date"])
    time_value = _find_value(row, SCHEDULE_HEADERS["time"])
    lane_value = _find_value(row, SCHEDULE_HEADERS["lane"])
    team_a = _find_value(row, SCHEDULE_HEADERS["team_a"])
    team_b = _find_value(row, SCHEDULE_HEADERS["team_b"])

    if not team_a or ("vs" in team_a.lower() and not team_b):
        parts = [part.strip() for part in team_a.split("vs")] if team_a else []
        if len(parts) == 2:
            team_a, team_b = parts

    if not any([date_value, time_value, lane_value, team_a, team_b]):
        return None

    return {
        "match_date": date_value,
        "match_time": time_value,
        "lane": lane_value,
        "team_a": team_a,
        "team_b": team_b,
        "raw": row,
    }


def _parse_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    value = re.sub(r"[^\d-]", "", value)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_float(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.replace(",", "")
    try:
        return float(value)
    except ValueError:
        return None


def _has_any_value(record: Dict[str, Any]) -> bool:
    for key in [
        "team_name",
        "player_name",
        "average",
        "handicap",
        "wins",
        "losses",
        "high_game",
        "high_series",
        "points",
    ]:
        if record.get(key):
            return True
    return False


def _log_parse_summary(
    kind: str, rows: List[Dict[str, Any]], parsed: List[Dict[str, Any]], used_text: bool
) -> None:
    sample_keys = list(parsed[0].keys()) if parsed else []
    sample_rows = _summarize_rows(rows, limit=3)
    logger.info(
        "Bowling %s parsed rows=%d parsed=%d used_text=%s sample_keys=%s sample_rows=%s",
        kind,
        len(rows),
        len(parsed),
        used_text,
        sample_keys,
        sample_rows,
    )


def _summarize_rows(rows: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, str]]:
    summarized: List[Dict[str, str]] = []
    for row in rows[:limit]:
        summarized.append(
            {
                str(key): _truncate_value(str(value))
                for key, value in row.items()
                if value is not None
            }
        )
    return summarized


def _truncate_value(value: str, limit: int = 80) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}…"


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
