from __future__ import annotations

from typing import List

import pytest

from types import SimpleNamespace

from packages.core.bowling import parser as bowling_parser


class _FakePage:
    def __init__(self, tables: List[List[List[str]]]) -> None:
        self._tables = tables

    def extract_tables(self):
        return self._tables


class _FakePDF:
    def __init__(self, tables: List[List[List[str]]]) -> None:
        self.pages = [_FakePage(tables)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_parse_stats_pdf(monkeypatch):
    tables = [
        [
            ["Team", "Name", "Avg", "Hdcp", "Wins", "Losses", "High Game", "High Series", "Points"],
            ["Strikers", "Alex", "180", "15", "5", "2", "210", "590", "12"],
        ]
    ]

    monkeypatch.setattr(
        bowling_parser,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF(tables)),
        raising=False,
    )

    results = bowling_parser.parse_stats_pdf(b"%PDF-1.4")
    assert results[0]["team_name"] == "Strikers"
    assert results[0]["player_name"] == "Alex"
    assert results[0]["average"] == 180
    assert results[0]["handicap"] == 15
    assert results[0]["high_game"] == 210


def test_parse_schedule_pdf(monkeypatch):
    tables = [
        [
            ["Date", "Time", "Lane", "Team 1", "Team 2"],
            ["2026-02-01", "18:30", "12", "Strikers", "Spare Us"],
        ]
    ]

    monkeypatch.setattr(
        bowling_parser,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF(tables)),
        raising=False,
    )

    results = bowling_parser.parse_schedule_pdf(b"%PDF-1.4")
    assert results[0]["match_date"] == "2026-02-01"
    assert results[0]["lane"] == "12"
    assert results[0]["team_a"] == "Strikers"
