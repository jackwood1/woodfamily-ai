from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional

from packages.core.bowling import bopo_averages
from packages.core.storage.base import BowlingFetchState, BowlingStatState


class _FakePage:
    def __init__(self, tables: List[List[List[str]]]) -> None:
        self._tables = tables

    def extract_tables(self, *args, **kwargs):
        return self._tables


class _FakePDF:
    def __init__(self, tables: List[List[List[str]]]) -> None:
        self.pages = [_FakePage(tables)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_bopo_averages_fetches_and_parses(monkeypatch):
    monkeypatch.setenv("BOPO_AVERAGES_URL", "https://example.com/averages.pdf")
    monkeypatch.setattr(bopo_averages, "fetch_pdf", lambda _: b"%PDF-1.4")
    tables = [
        [
            [
                "Bowler",
                "Team",
                "Night",
                "Sex",
                "Average",
                "Games",
                "Hi Game",
                "Low Game",
                "Pin Diff",
            ],
            ["Gino", "Beer Frame", "Thurs", "M", "188.3", "6", "245", "139", "72"],
        ]
    ]
    monkeypatch.setattr(
        bopo_averages,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF(tables)),
        raising=False,
    )
    monkeypatch.setattr(bopo_averages, "_store", lambda: _FakeStore())

    result = bopo_averages.get_bopo_averages(player_name="Gino")
    assert result["status"] == "ok"
    assert result["bowlers"][0]["bowler"] == "Gino"


def test_bopo_averages_uses_cache(monkeypatch):
    now_iso = datetime.now().isoformat()
    fetch_state = BowlingFetchState(
        league_key="bopo_averages",
        last_fetch_at=now_iso,
        stats_url="https://example.com/averages.pdf",
        schedule_url=None,
        standings_url=None,
        file_path="cached.pdf",
    )
    store = _FakeStore(fetch_state=fetch_state)
    store.save_bowling_stats(
        "bopo_averages",
        [
            BowlingStatState(
                league_key="bopo_averages",
                team_name="Beer Frame",
                player_name="Gino",
                average=188,
                handicap=None,
                wins=None,
                losses=None,
                high_game=245,
                high_series=None,
                points=None,
                raw={},
                created_at=now_iso,
                updated_at=now_iso,
            )
        ],
    )
    monkeypatch.setattr(bopo_averages, "_store", lambda: store)
    monkeypatch.setattr(
        bopo_averages, "_resolve_averages_url", lambda: "https://example.com/averages.pdf"
    )
    result = bopo_averages.get_bopo_averages(player_name="Gino")
    assert result["cached"] is True
    assert result["bowlers"][0]["bowler"] == "Gino"


def test_resolve_averages_url_from_section(monkeypatch):
    html = """
    <h3>AVERAGES</h3>
    <div data-file="https://example.com/averages.pdf">Download</div>
    """
    monkeypatch.setattr(bopo_averages, "fetch_html", lambda _: html)
    url = bopo_averages._resolve_averages_url()
    assert url == "https://example.com/averages.pdf"


class _FakeStore:
    def __init__(self, fetch_state: Optional[BowlingFetchState] = None) -> None:
        self._fetch_state = fetch_state
        self._stats: List[BowlingStatState] = []

    def get_bowling_fetch(self, league_key: str):
        return self._fetch_state

    def upsert_bowling_fetch(self, fetch):
        self._fetch_state = fetch

    def save_bowling_stats(self, league_key: str, stats: List[BowlingStatState]) -> None:
        self._stats = stats

    def list_bowling_stats(self, league_key: str, team_name=None, player_name=None):
        return self._stats
