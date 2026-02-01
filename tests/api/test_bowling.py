from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import bowling as bowling_module


class DummyBowlingService:
    def list_leagues(self):
        return [{"key": "monday_bayside", "name": "Monday at Bayside"}]

    def sync_league(self, league_key: str):
        return {"status": "ok", "league_key": league_key, "stats_rows": 1, "matches": 2}

    def list_teams(self, league_key: str, force_refresh: bool = False):
        return [{"name": "Beer Frame"}]

    def team_stats(self, league_key: str, team_name: str, force_refresh: bool = False):
        return [{"player_name": "Gino", "average": 180}]

    def player_stats(self, league_key: str, player_name: str, force_refresh: bool = False):
        return [{"player_name": "Gino", "average": 180}]

    def list_matches(
        self,
        league_key: str,
        team_name=None,
        date_from=None,
        date_to=None,
        force_refresh: bool = False,
    ):
        return [{"lane": "12", "team_a": "Beer Frame"}]


def test_bowling_leagues(monkeypatch):
    monkeypatch.setattr(bowling_module, "_service", lambda: DummyBowlingService())
    client = TestClient(app)
    response = client.get("/api/bowling/leagues")
    assert response.status_code == 200
    assert response.json()[0]["key"] == "monday_bayside"


def test_bowling_sync(monkeypatch):
    monkeypatch.setattr(bowling_module, "_service", lambda: DummyBowlingService())
    client = TestClient(app)
    response = client.post("/api/bowling/monday_bayside/sync")
    assert response.status_code == 200
    assert response.json()["stats_rows"] == 1


def test_bowling_team_stats(monkeypatch):
    monkeypatch.setattr(bowling_module, "_service", lambda: DummyBowlingService())
    client = TestClient(app)
    response = client.get("/api/bowling/monday_bayside/team-stats?team_name=Beer%20Frame")
    assert response.status_code == 200
    assert response.json()[0]["player_name"] == "Gino"


def test_bowling_player_stats(monkeypatch):
    monkeypatch.setattr(bowling_module, "_service", lambda: DummyBowlingService())
    client = TestClient(app)
    response = client.get("/api/bowling/monday_bayside/player-stats?player_name=Gino")
    assert response.status_code == 200
    assert response.json()[0]["average"] == 180


def test_bowling_matches(monkeypatch):
    monkeypatch.setattr(bowling_module, "_service", lambda: DummyBowlingService())
    client = TestClient(app)
    response = client.get("/api/bowling/monday_bayside/matches?team_name=Beer%20Frame")
    assert response.status_code == 200
    assert response.json()[0]["lane"] == "12"


def test_bopo_schedule(monkeypatch):
    monkeypatch.setattr(
        bowling_module,
        "get_bopo_schedule",
        lambda team_name: {
            "status": "ok",
            "team_name": team_name,
            "schedule_url": "https://example.com/schedule.pdf",
            "matches": [{"date": "1/2/2026", "team_a": "Beer Frame"}],
        },
    )
    client = TestClient(app)
    response = client.get("/api/bowling/bopo/schedule?team_name=Beer%20Frame")
    assert response.status_code == 200
    assert response.json()["matches"][0]["team_a"] == "Beer Frame"


def test_bopo_averages(monkeypatch):
    monkeypatch.setattr(
        bowling_module,
        "get_bopo_averages",
        lambda team_name=None, player_name=None: {
            "status": "ok",
            "averages_url": "https://example.com/averages.pdf",
            "count": 1,
            "bowlers": [{"bowler": "Gino", "team": "Beer Frame"}],
        },
    )
    client = TestClient(app)
    response = client.get("/api/bowling/bopo/averages?player_name=Gino")
    assert response.status_code == 200
    assert response.json()["bowlers"][0]["bowler"] == "Gino"


def test_bopo_standings(monkeypatch):
    monkeypatch.setattr(
        bowling_module,
        "get_bopo_standings",
        lambda day=None, team_name=None: {
            "status": "ok",
            "standings_url": "https://example.com/standings.pdf",
            "count": 1,
            "standings": [{"day": "Tuesday A", "team": "Beer Frame"}],
        },
    )
    client = TestClient(app)
    response = client.get("/api/bowling/bopo/standings?day=Tuesday%20A")
    assert response.status_code == 200
    assert response.json()["standings"][0]["team"] == "Beer Frame"


def test_casco_monday(monkeypatch):
    monkeypatch.setattr(
        bowling_module,
        "get_casco_monday",
        lambda team_name=None: {
            "status": "ok",
            "source_url": "https://example.com/casco.pdf",
            "standings": [{"team": "Beer Frame", "points": 25}],
            "schedule": [{"team_a": "Beer Frame", "team_b": "Bowl Cuts"}],
        },
    )
    client = TestClient(app)
    response = client.get("/api/bowling/casco/monday?team_name=Beer%20Frame")
    assert response.status_code == 200
    assert response.json()["standings"][0]["team"] == "Beer Frame"
