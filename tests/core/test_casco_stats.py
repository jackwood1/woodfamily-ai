from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional

from packages.core.bowling import casco_stats
from packages.core.storage.base import BowlingFetchState, BowlingStatState


class _FakePage:
    def __init__(self, text: str, tables: List[List[List[str]]]) -> None:
        self._text = text
        self._tables = tables

    def extract_text(self):
        return self._text

    def extract_tables(self, *args, **kwargs):
        return self._tables


class _FakePDF:
    def __init__(self, text: str, tables: List[List[List[str]]]) -> None:
        self.pages = [_FakePage(text, tables)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeLLM:
    def chat(self, messages, tools):
        return {"choices": [{"message": {"content": "[]"}}]}


def test_casco_stats_parses_bowler_from_text(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "CASCO_MONDAY_STATS_URL", "https://example.com/casco_stats.pdf"
    )
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(tmp_path / "lists.db"))
    monkeypatch.setattr(casco_stats, "fetch_pdf", lambda _: b"%PDF-1.4")
    monkeypatch.setattr(casco_stats, "OpenAIClient", _FakeLLM)
    text = "\n".join(
        [
            "Monday Bowling Statistics",
            "Beer Frame 600",
            "Gino 156 149 176 143",
            "Murph 143 150 120",
        ]
    )
    monkeypatch.setattr(
        casco_stats,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF(text, [])),
        raising=False,
    )
    result = casco_stats.get_casco_monday_bowlers(player_name="Gino", force_refresh=True)
    assert result["status"] == "ok"
    assert result["count"] == 1
    assert result["bowlers"][0]["bowler"] == "Gino"
    assert result["bowlers"][0]["team"] == "Beer Frame"
    assert result["bowlers"][0]["average"] == 156


def test_casco_stats_uses_cache(monkeypatch):
    now_iso = datetime.now().isoformat()
    fetch_state = BowlingFetchState(
        league_key="casco_monday_stats",
        last_fetch_at=now_iso,
        stats_url="https://example.com/casco_stats.pdf",
        schedule_url=None,
        standings_url=None,
        file_path="cached.pdf",
    )
    store = _FakeStore(fetch_state=fetch_state)
    store.save_bowling_stats(
        "casco_monday_stats",
        [
            BowlingStatState(
                league_key="casco_monday_stats",
                team_name="Beer Frame",
                player_name="Gino",
                average=156,
                handicap=None,
                wins=None,
                losses=None,
                high_game=None,
                high_series=None,
                points=None,
                raw={},
                created_at=now_iso,
                updated_at=now_iso,
            )
        ],
    )
    monkeypatch.setattr(casco_stats, "_store", lambda: store)
    monkeypatch.setenv(
        "CASCO_MONDAY_STATS_URL", "https://example.com/casco_stats.pdf"
    )
    result = casco_stats.get_casco_monday_bowlers(player_name="Gino")
    assert result["cached"] is True
    assert result["count"] == 1
    assert result["bowlers"][0]["bowler"] == "Gino"


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
