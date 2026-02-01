from __future__ import annotations

import io
import re
from typing import Any, Dict, List, Optional

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


STAT_HEADERS = {
    "team": ["team", "team name"],
    "player": ["name", "bowler", "player"],
    "average": ["avg", "average"],
    "handicap": ["hdcp", "hcp", "handicap"],
    "wins": ["wins", "win"],
    "losses": ["losses", "loss"],
    "high_game": ["high game", "highgame", "hg"],
    "high_series": ["high series", "highseries", "hs"],
    "points": ["points", "pts"],
}

SCHEDULE_HEADERS = {
    "date": ["date"],
    "time": ["time"],
    "lane": ["lane", "lanes"],
    "team_a": ["team 1", "team a", "home", "team"],
    "team_b": ["team 2", "team b", "away", "opponent"],
}


def parse_stats_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        record = {
            "team_name": _find_value(row, STAT_HEADERS["team"]),
            "player_name": _find_value(row, STAT_HEADERS["player"]),
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


def parse_schedule_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        record = _parse_schedule_row(row)
        if record:
            parsed.append(record)
    return parsed


def _extract_rows(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows: List[Dict[str, Any]] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables() or []
            for table in tables:
                rows.extend(_table_to_rows(table))
    return rows


def _table_to_rows(table: List[List[Optional[str]]]) -> List[Dict[str, Any]]:
    header: Optional[List[str]] = None
    rows: List[Dict[str, Any]] = []
    for raw_row in table:
        cells = [(_clean_cell(cell) or "") for cell in raw_row]
        if not any(cells):
            continue
        if header is None and _looks_like_header(cells):
            header = [_normalize_header(cell) for cell in cells]
            continue
        if header is None:
            header = [f"col_{idx}" for idx in range(len(cells))]
        row_dict = {header[idx]: cells[idx] for idx in range(len(cells))}
        rows.append(row_dict)
    return rows


def _looks_like_header(cells: List[str]) -> bool:
    joined = " ".join(cell.lower() for cell in cells if cell)
    keywords = ["team", "name", "avg", "average", "lane", "date", "time"]
    return any(keyword in joined for keyword in keywords)


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


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
