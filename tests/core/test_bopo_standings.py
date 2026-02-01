from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional

from packages.core.bowling import bopo_standings
from packages.core.storage.base import BowlingFetchState, BowlingStatState


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def extract_text(self):
        return self._text


class _FakePDF:
    def __init__(self, text: str) -> None:
        self.pages = [_FakePage(text)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeLLM:
    def chat(self, messages, tools):
        return {
            "choices": [
                {
                    "message": {
                        "content": '[{"day":"Tuesday A","team":"Beer Frame","wins":2,"losses":2,"points":25,"hi_series":681,"team_avg":582.3,"opp_avg":599.9,"team_diff":-17.7}]'
                    }
                }
            ]
        }


def test_get_bopo_standings_fetches_and_parses(monkeypatch):
    monkeypatch.setenv("BOPO_STANDINGS_URL", "https://example.com/standings.pdf")
    monkeypatch.setattr(bopo_standings, "fetch_pdf", lambda _: b"%PDF-1.4")
    monkeypatch.setattr(
        bopo_standings,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF("Team Won Loss Points")),
        raising=False,
    )
    monkeypatch.setattr(bopo_standings, "_store", lambda: _FakeStore())

    result = bopo_standings.get_bopo_standings(day="Tuesday A", llm=_FakeLLM())
    assert result["status"] == "ok"
    assert result["standings"][0]["team"] == "Beer Frame"


def test_bopo_standings_uses_cache(monkeypatch):
    now_iso = datetime.now().isoformat()
    fetch_state = BowlingFetchState(
        league_key="bopo_standings",
        last_fetch_at=now_iso,
        stats_url=None,
        schedule_url=None,
        standings_url="https://example.com/standings.pdf",
        file_path="cached.pdf",
    )
    store = _FakeStore(fetch_state=fetch_state)
    store.save_bowling_stats(
        "bopo_standings",
        [
            BowlingStatState(
                league_key="bopo_standings",
                team_name="Beer Frame",
                player_name=None,
                average=582,
                handicap=None,
                wins=2,
                losses=2,
                high_game=None,
                high_series=681,
                points=25,
                raw={"day": "Tuesday A"},
                created_at=now_iso,
                updated_at=now_iso,
            )
        ],
    )
    monkeypatch.setattr(bopo_standings, "_store", lambda: store)
    monkeypatch.setattr(
        bopo_standings, "_resolve_standings_url", lambda: "https://example.com/standings.pdf"
    )
    result = bopo_standings.get_bopo_standings(day="Tuesday A", llm=_FakeLLM())
    assert result["cached"] is True
    assert result["standings"][0]["team"] == "Beer Frame"


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
