from __future__ import annotations

from datetime import datetime
import json
from types import SimpleNamespace
from typing import List, Optional

from packages.core.bowling import casco_monday
from packages.core.storage.base import BowlingFetchState, BowlingMatchState, BowlingStatState


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
                        "content": json.dumps(
                            {
                                "standings": [
                                    {"team": "Beer Frame", "points": 25, "captain": "Rob"}
                                ],
                                "schedule": [
                                    {
                                        "date": "1/12",
                                        "time": "5:30",
                                        "lane": "11",
                                        "team_a": "Beer Frame",
                                        "team_b": "Bowl Cuts",
                                    }
                                ],
                            }
                        )
                    }
                }
            ]
        }


def test_get_casco_monday_fetches_and_parses(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCO_MONDAY_URL", "https://example.com/casco.pdf")
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(tmp_path / "lists.db"))
    monkeypatch.setattr(casco_monday, "fetch_pdf", lambda _: b"%PDF-1.4")
    monkeypatch.setattr(
        casco_monday,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF("Standings Schedule")),
        raising=False,
    )
    result = casco_monday.get_casco_monday(team_name="Beer Frame", llm=_FakeLLM())
    assert result["status"] == "ok"
    assert result["standings"][0]["team"] == "Beer Frame"
    assert result["schedule"][0]["team_a"] == "Beer Frame"


def test_casco_monday_uses_cache(monkeypatch, tmp_path):
    now_iso = datetime.now().isoformat()
    fetch_state = BowlingFetchState(
        league_key="casco_monday_bayside",
        last_fetch_at=now_iso,
        stats_url=None,
        schedule_url=None,
        standings_url="https://example.com/casco.pdf",
        file_path="cached.pdf",
    )
    store = _FakeStore(fetch_state=fetch_state)
    store.save_bowling_stats(
        "casco_monday_bayside",
        [
            BowlingStatState(
                league_key="casco_monday_bayside",
                team_name="Beer Frame",
                player_name=None,
                average=None,
                handicap=None,
                wins=None,
                losses=None,
                high_game=None,
                high_series=None,
                points=25,
                raw={},
                created_at=now_iso,
                updated_at=now_iso,
            )
        ],
    )
    store.save_bowling_matches(
        "casco_monday_bayside",
        [
            BowlingMatchState(
                league_key="casco_monday_bayside",
                match_date="1/12",
                match_time="5:30",
                lane="11",
                team_a="Beer Frame",
                team_b="Bowl Cuts",
                raw={},
                created_at=now_iso,
                updated_at=now_iso,
            )
        ],
    )
    monkeypatch.setenv("CASCO_MONDAY_URL", "https://example.com/casco.pdf")
    monkeypatch.setattr(casco_monday, "_store", lambda: store)
    result = casco_monday.get_casco_monday(team_name="Beer Frame")
    assert result["cached"] is True
    assert result["standings"][0]["team"] == "Beer Frame"


class _FakeStore:
    def __init__(self, fetch_state: Optional[BowlingFetchState] = None) -> None:
        self._fetch_state = fetch_state
        self._stats: List[BowlingStatState] = []
        self._matches: List[BowlingMatchState] = []

    def get_bowling_fetch(self, league_key: str):
        return self._fetch_state

    def upsert_bowling_fetch(self, fetch):
        self._fetch_state = fetch

    def save_bowling_stats(self, league_key: str, stats: List[BowlingStatState]) -> None:
        self._stats = stats

    def save_bowling_matches(self, league_key: str, matches: List[BowlingMatchState]) -> None:
        self._matches = matches

    def list_bowling_stats(self, league_key: str, team_name=None, player_name=None):
        return self._stats

    def list_bowling_matches(self, league_key: str, team_name=None, date_from=None, date_to=None):
        return self._matches
