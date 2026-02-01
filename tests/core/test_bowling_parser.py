from __future__ import annotations

from typing import List

import pytest

from types import SimpleNamespace

from packages.core.bowling import parser as bowling_parser


class _FakePage:
    def __init__(self, tables: List[List[List[str]]], text: str = "") -> None:
        self._tables = tables
        self._text = text

    def extract_tables(self, *args, **kwargs):
        return self._tables

    def extract_text(self):
        return self._text


class _FakePDF:
    def __init__(self, tables: List[List[List[str]]], text: str = "") -> None:
        self.pages = [_FakePage(tables, text=text)]

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


def test_parse_stats_header_not_first_row(monkeypatch):
    tables = [
        [
            ["League Stats", "", ""],
            ["Team", "Name", "Avg"],
            ["Strikers", "Alex", "180"],
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


def test_parse_stats_uses_text_fallback(monkeypatch):
    text = "\n".join(
        [
            "Monday Bowling Statistics",
            "Team  Name  Avg  Hdcp  Wins  Losses  High Game  High Series  Points",
            "Beer Frame  Gino  180  12  5  2  210  590  12",
        ]
    )

    monkeypatch.setattr(
        bowling_parser,
        "pdfplumber",
        SimpleNamespace(open=lambda _: _FakePDF([], text=text)),
        raising=False,
    )

    results = bowling_parser.parse_stats_pdf(b"%PDF-1.4")
    assert results[0]["team_name"] == "Beer Frame"
    assert results[0]["player_name"] == "Gino"
