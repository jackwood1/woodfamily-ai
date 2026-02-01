from __future__ import annotations

from packages.core.bowling.service import BowlingService


def test_sync_league_saves_stats_and_matches(tmp_path, monkeypatch):
    config_path = tmp_path / "bowling.json"
    config_path.write_text(
        """
        {
          "leagues": [
            {
              "key": "monday_bayside",
              "name": "Monday at Bayside",
              "listing_url": "https://example.com/listing",
              "stats_match": "Bowling Statistics",
              "schedule_match": "Schedule"
            }
          ]
        }
        """.strip()
    )
    db_path = tmp_path / "lists.db"

    monkeypatch.setattr(
        "packages.core.bowling.service.fetch_html",
        lambda _: '<a href="https://example.com/stats.pdf">Bowling Statistics</a>'
        '<a href="https://example.com/schedule.pdf">Schedule</a>',
    )
    monkeypatch.setattr(
        "packages.core.bowling.service.safe_fetch_pdf", lambda _: b"%PDF-1.4"
    )
    monkeypatch.setattr(
        "packages.core.bowling.service.parse_stats_pdf",
        lambda _: [
            {
                "team_name": "Strikers",
                "player_name": "Alex",
                "average": 180,
                "handicap": 10,
                "wins": 5,
                "losses": 2,
                "high_game": 220,
                "high_series": 600,
                "points": 12,
                "raw": {},
            }
        ],
    )
    monkeypatch.setattr(
        "packages.core.bowling.service.parse_schedule_pdf",
        lambda _: [
            {
                "match_date": "2026-02-01",
                "match_time": "18:30",
                "lane": "12",
                "team_a": "Strikers",
                "team_b": "Spare Us",
                "raw": {},
            }
        ],
    )

    service = BowlingService(config_path=str(config_path), db_path=str(db_path))
    response = service.sync_league("monday_bayside")
    assert response["status"] == "ok"
    assert response["stats_rows"] == 1
    assert response["matches"] == 1

    stats = service.team_stats("monday_bayside", "Strikers")
    assert stats[0]["player_name"] == "Alex"

    matches = service.list_matches("monday_bayside", team_name="Strikers")
    assert matches[0]["lane"] == "12"
