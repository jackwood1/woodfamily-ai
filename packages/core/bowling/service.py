from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from packages.core.storage.base import (
    BowlingFetchState,
    BowlingMatchState,
    BowlingStatState,
)
from packages.core.storage.sqlite import SQLiteListStore

from .config import get_league, load_bowling_config
from .fetcher import fetch_html, safe_fetch_pdf
from .parser import parse_schedule_pdf, parse_stats_pdf


class BowlingService:
    def __init__(self, config_path: Optional[str] = None, db_path: Optional[str] = None) -> None:
        self._config = load_bowling_config(config_path)
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        default_db = os.path.join(base_dir, "apps", "api", "data", "lists.db")
        self._store = SQLiteListStore(db_path=db_path or os.getenv("HOME_OPS_DB_PATH", default_db))
        self._refresh_days = int(os.getenv("BOWLING_REFRESH_DAYS", "7"))

    def list_leagues(self) -> List[Dict[str, Any]]:
        return self._config.get("leagues", [])

    def sync_league(self, league_key: str) -> Dict[str, Any]:
        league = get_league(self._config, league_key)
        if not league:
            return {"status": "error", "error": "league_not_found", "league_key": league_key}

        resolved = _resolve_league_urls(league)
        listing_url = resolved["listing_url"]
        stats_url = resolved["stats_url"]
        schedule_url = resolved["schedule_url"]
        standings_url = resolved["standings_url"]
        stats_pdf = safe_fetch_pdf(stats_url)
        schedule_pdf = safe_fetch_pdf(schedule_url)
        stats_rows = parse_stats_pdf(stats_pdf) if stats_pdf else []
        schedule_rows = parse_schedule_pdf(schedule_pdf) if schedule_pdf else []

        timestamp = datetime.now().isoformat()
        stats = [
            BowlingStatState(
                league_key=league_key,
                team_name=row.get("team_name"),
                player_name=row.get("player_name"),
                average=row.get("average"),
                handicap=row.get("handicap"),
                wins=row.get("wins"),
                losses=row.get("losses"),
                high_game=row.get("high_game"),
                high_series=row.get("high_series"),
                points=row.get("points"),
                raw=row.get("raw", {}),
                created_at=timestamp,
                updated_at=timestamp,
            )
            for row in stats_rows
        ]
        matches = [
            BowlingMatchState(
                league_key=league_key,
                match_date=row.get("match_date"),
                match_time=row.get("match_time"),
                lane=row.get("lane"),
                team_a=row.get("team_a"),
                team_b=row.get("team_b"),
                raw=row.get("raw", {}),
                created_at=timestamp,
                updated_at=timestamp,
            )
            for row in schedule_rows
        ]
        self._store.save_bowling_stats(league_key, stats)
        self._store.save_bowling_matches(league_key, matches)
        self._store.upsert_bowling_fetch(
            BowlingFetchState(
                league_key=league_key,
                last_fetch_at=timestamp,
                stats_url=stats_url,
                schedule_url=schedule_url,
                standings_url=standings_url,
            )
        )
        return {
            "status": "ok",
            "league_key": league_key,
            "stats_rows": len(stats),
            "matches": len(matches),
            "stats_url": stats_url,
            "schedule_url": schedule_url,
            "standings_url": standings_url,
            "last_fetch_at": timestamp,
        }

    def list_teams(self, league_key: str) -> List[Dict[str, Any]]:
        self._ensure_league_data(league_key)
        league = get_league(self._config, league_key)
        if league and league.get("teams"):
            return league.get("teams", [])
        stats = self._store.list_bowling_stats(league_key)
        teams = sorted({stat.team_name for stat in stats if stat.team_name})
        return [{"name": team} for team in teams]

    def team_stats(self, league_key: str, team_name: str) -> List[Dict[str, Any]]:
        self._ensure_league_data(league_key)
        stats = self._store.list_bowling_stats(league_key, team_name=team_name)
        return [self._stat_to_dict(stat) for stat in stats]

    def player_stats(self, league_key: str, player_name: str) -> List[Dict[str, Any]]:
        self._ensure_league_data(league_key)
        stats = self._store.list_bowling_stats(league_key, player_name=player_name)
        return [self._stat_to_dict(stat) for stat in stats]

    def list_matches(
        self,
        league_key: str,
        team_name: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_league_data(league_key)
        matches = self._store.list_bowling_matches(
            league_key, team_name=team_name, date_from=date_from, date_to=date_to
        )
        return [
            {
                "match_date": match.match_date,
                "match_time": match.match_time,
                "lane": match.lane,
                "team_a": match.team_a,
                "team_b": match.team_b,
            }
            for match in matches
        ]

    @staticmethod
    def _stat_to_dict(stat: BowlingStatState) -> Dict[str, Any]:
        return {
            "team_name": stat.team_name,
            "player_name": stat.player_name,
            "average": stat.average,
            "handicap": stat.handicap,
            "wins": stat.wins,
            "losses": stat.losses,
            "high_game": stat.high_game,
            "high_series": stat.high_series,
            "points": stat.points,
        }

    def _ensure_league_data(self, league_key: str) -> None:
        if not get_league(self._config, league_key):
            return
        if not self._should_refresh(league_key):
            return
        self.sync_league(league_key)

    def _should_refresh(self, league_key: str) -> bool:
        fetch_state = self._store.get_bowling_fetch(league_key)
        if fetch_state is None:
            return True
        try:
            last_fetch = datetime.fromisoformat(fetch_state.last_fetch_at)
        except ValueError:
            return True
        age = datetime.now() - last_fetch
        if age > timedelta(days=self._refresh_days):
            return True
        return False


def _resolve_league_urls(league: Dict[str, Any]) -> Dict[str, Optional[str]]:
    listing_url = league.get("listing_url")
    listing_html = fetch_html(listing_url) if listing_url else None
    stats_url = _resolve_pdf_url(
        listing_html, listing_url, league.get("stats_match"), league.get("stats_url")
    )
    schedule_url = _resolve_pdf_url(
        listing_html, listing_url, league.get("schedule_match"), league.get("schedule_url")
    )
    standings_url = _resolve_pdf_url(
        listing_html, listing_url, league.get("standings_match"), league.get("standings_url")
    )
    return {
        "listing_url": listing_url,
        "stats_url": stats_url,
        "schedule_url": schedule_url,
        "standings_url": standings_url,
    }


def _resolve_pdf_url(
    html: Optional[str],
    base_url: Optional[str],
    match_text: Optional[str],
    fallback_url: Optional[str],
) -> Optional[str]:
    if match_text is None:
        return fallback_url
    candidates = _extract_pdf_links(html or "")
    if not candidates:
        return fallback_url
    lowered_match = match_text.lower()
    matching = [link for link in candidates if lowered_match in link[0].lower()]
    if not matching:
        return fallback_url
    href = matching[-1][1]
    if base_url:
        return urljoin(base_url, href)
    return href


def _extract_pdf_links(html: str) -> List[tuple[str, str]]:
    links: List[tuple[str, str]] = []
    for line in html.splitlines():
        if ".pdf" not in line.lower():
            continue
        parts = line.split("href=")
        for part in parts[1:]:
            quote = '"' if '"' in part else "'"
            if quote not in part:
                continue
            href = part.split(quote, 2)[1]
            if ".pdf" not in href.lower():
                continue
            text = line
            links.append((text, href))
    return links
