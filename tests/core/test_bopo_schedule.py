from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import List

from packages.core.bowling import bopo_schedule
from packages.core.storage.base import BowlingFetchState


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
                        "content": '[{"date":"1/2/2026","time":"6:00","lanes":"1&2","team_a":"Beer Frame","team_b":"Oddballs","opponent":"Oddballs"}]'
                    }
                }
            ]
        }


def test_get_bopo_schedule_uses_llm(monkeypatch, tmp_path):
    monkeypatch.setenv("BOPO_SCHEDULE_URL", "https://example.com/schedule.pdf")
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(tmp_path / "lists.db"))
    monkeypatch.setattr(bopo_schedule, "fetch_pdf", lambda _: b"%PDF-1.4")
    monkeypatch.setattr(
        bopo_schedule,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF("Date Team 1 Team 2 Lanes")),
        raising=False,
    )

    result = bopo_schedule.get_bopo_schedule("Beer Frame", llm=_FakeLLM())
    assert result["status"] == "ok"
    assert result["matches"][0]["team_a"] == "Beer Frame"
    assert result["schedule_url"] == "https://example.com/schedule.pdf"


def test_bopo_schedule_uses_cache(monkeypatch, tmp_path):
    cache_path = tmp_path / "bopo_schedule.pdf"
    cache_path.write_bytes(b"%PDF-1.4 cached")
    monkeypatch.setenv("BOPO_SCHEDULE_URL", "https://example.com/schedule.pdf")
    monkeypatch.setenv("HOME_OPS_DB_PATH", str(tmp_path / "lists.db"))
    monkeypatch.setattr(bopo_schedule, "_cache_path", lambda: str(cache_path))

    now_iso = datetime.now().isoformat()
    fetch_state = BowlingFetchState(
        league_key="bopo_schedule",
        last_fetch_at=now_iso,
        stats_url=None,
        schedule_url="https://example.com/schedule.pdf",
        standings_url=None,
        file_path=str(cache_path),
    )
    store = _FakeStore(fetch_state)
    store.save_bowling_matches(
        "bopo_schedule",
        [
            _match_state(
                league_key="bopo_schedule",
                match_date="1/2/2026",
                match_time="6:00",
                lane="1&2",
                team_a="Beer Frame",
                team_b="Oddballs",
                raw={"opponent": "Oddballs"},
            )
        ],
    )
    monkeypatch.setattr(bopo_schedule, "_store", lambda: store)
    monkeypatch.setattr(bopo_schedule, "fetch_pdf", lambda _: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(
        bopo_schedule,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF("Date Team 1 Team 2 Lanes")),
        raising=False,
    )

    result = bopo_schedule.get_bopo_schedule("Beer Frame", llm=_FakeLLM())
    assert result["status"] == "ok"
    assert result["matches"][0]["team_a"] == "Beer Frame"


class _FakeStore:
    def __init__(self, fetch_state: BowlingFetchState) -> None:
        self._fetch_state = fetch_state
        self._matches: List[object] = []

    def get_bowling_fetch(self, league_key: str):
        return self._fetch_state

    def upsert_bowling_fetch(self, fetch):
        self._fetch_state = fetch

    def save_bowling_matches(self, league_key: str, matches):
        self._matches = matches

    def list_bowling_matches(self, league_key: str, team_name=None, date_from=None, date_to=None):
        return self._matches


def _match_state(
    league_key: str,
    match_date: str,
    match_time: str,
    lane: str,
    team_a: str,
    team_b: str,
    raw: dict,
):
    return bopo_schedule.BowlingMatchState(
        league_key=league_key,
        match_date=match_date,
        match_time=match_time,
        lane=lane,
        team_a=team_a,
        team_b=team_b,
        raw=raw,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat(),
    )
