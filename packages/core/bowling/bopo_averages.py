from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from packages.core.bowling.fetcher import fetch_html, fetch_pdf
from packages.core.storage.base import BowlingFetchState, BowlingStatState
from packages.core.storage.sqlite import SQLiteListStore

try:
    import pdfplumber
except Exception as exc:  # pragma: no cover - optional dependency resolution
    pdfplumber = None
    _PDF_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

DEFAULT_BOPO_URL = "https://www.bopostats.com/"
DEFAULT_CACHE_KEY = "bopo_averages"


def get_bopo_averages(
    team_name: Optional[str] = None,
    player_name: Optional[str] = None,
) -> Dict[str, Any]:
    store = _store()
    averages_url = _resolve_averages_url()
    if not averages_url:
        return {"status": "error", "error": "averages_url_not_found"}
    cache_path = _cache_path()
    fetch_state = store.get_bowling_fetch(DEFAULT_CACHE_KEY)
    if not _should_refresh(fetch_state, averages_url):
        cached = _load_cached_bowlers(store, DEFAULT_CACHE_KEY, team_name, player_name)
        if cached is not None:
            return {
                "status": "ok",
                "averages_url": averages_url,
                "averages_path": cache_path,
                "count": len(cached),
                "cached": True,
                "bowlers": cached,
            }

    pdf_bytes = fetch_pdf(averages_url)
    _write_cache_pdf(cache_path, pdf_bytes)
    _log_file_fetched(averages_url, cache_path)
    bowlers = _parse_averages_pdf(pdf_bytes)
    _save_bowler_stats(store, DEFAULT_CACHE_KEY, bowlers)
    _upsert_fetch_state(store, DEFAULT_CACHE_KEY, averages_url, cache_path)
    filtered = _load_cached_bowlers(store, DEFAULT_CACHE_KEY, team_name, player_name) or []
    return {
        "status": "ok",
        "averages_url": averages_url,
        "averages_path": cache_path,
        "count": len(filtered),
        "cached": False,
        "bowlers": filtered,
    }


def _resolve_averages_url() -> Optional[str]:
    override = os.getenv("BOPO_AVERAGES_URL")
    if override:
        return override
    html = fetch_html(DEFAULT_BOPO_URL)
    if not html:
        return None
    averages_url = _find_averages_link(html, DEFAULT_BOPO_URL)
    if averages_url:
        return averages_url
    section_url = _find_section_pdf_link(html, DEFAULT_BOPO_URL, "averages")
    if section_url:
        return section_url
    pdf_links = _extract_pdf_links(html, DEFAULT_BOPO_URL, keyword="averages")
    return pdf_links[0] if pdf_links else None


def _find_averages_link(html: str, base_url: str) -> Optional[str]:
    anchor_pattern = re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+\.pdf)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in anchor_pattern.finditer(html):
        label = match.group("label")
        if "averages" not in label.lower():
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


def _parse_averages_pdf(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows = _extract_rows(data)
    if not rows:
        return []
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        parsed.append(
            {
                "bowler": _to_str(_find_value(row, ["bowler", "name"])),
                "team": _to_str(_find_value(row, ["team"])),
                "night": _to_str(_find_value(row, ["night"])),
                "sex": _to_str(_find_value(row, ["sex"])),
                "average": _to_float(_find_value(row, ["average", "avg"])),
                "games": _to_int(_find_value(row, ["games"])),
                "high_game": _to_int(_find_value(row, ["hi game", "high game", "hg"])),
                "low_game": _to_int(_find_value(row, ["low game", "lg"])),
                "pin_diff": _to_int(_find_value(row, ["pin diff", "diff"])),
            }
        )
    return [row for row in parsed if row.get("bowler") and row.get("team")]


def _extract_rows(data: bytes) -> List[Dict[str, Any]]:
    _ensure_pdf_available()
    rows: List[Dict[str, Any]] = []
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
    keywords = ["bowler", "team", "night", "sex", "average", "games", "pin diff"]
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


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
    db_path = os.getenv("HOME_OPS_DB_PATH", default_db)
    return SQLiteListStore(db_path=db_path)


def _cache_path() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cache_dir = os.path.join(base_dir, "apps", "api", "data", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "bopo_averages.pdf")


def _should_refresh(fetch_state: Optional[BowlingFetchState], averages_url: str) -> bool:
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
    if _basename(fetch_state.stats_url) != _basename(averages_url):
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
    store: SQLiteListStore, cache_key: str, averages_url: str, cache_path: str
) -> None:
    timestamp = datetime.now().isoformat()
    store.upsert_bowling_fetch(
        BowlingFetchState(
            league_key=cache_key,
            last_fetch_at=timestamp,
            stats_url=averages_url,
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
                high_game=_to_int(bowler.get("high_game")),
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
        "high_game": stat.high_game,
        "raw": stat.raw,
    }


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        cleaned = cleaned.replace("(", "-").replace(")", "")
        return int(cleaned)
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).replace("\u00a0", " ").strip()
    return cleaned or None


def _log_file_fetched(averages_url: str, cache_path: str) -> None:
    logger.info("BoPo averages fetched url=%s path=%s", averages_url, cache_path)


def _ensure_pdf_available() -> None:
    if pdfplumber is None:
        raise RuntimeError(
            "pdfplumber is required for PDF parsing. Install it via requirements.txt."
        ) from _PDF_IMPORT_ERROR
