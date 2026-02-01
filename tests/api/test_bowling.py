from fastapi.testclient import TestClient

from apps.api.main import app
from apps.api.routes import bowling as bowling_module


class DummyBowlingService:
    def list_leagues(self):
        return [{"key": "monday_bayside", "name": "Monday at Bayside"}]

    def sync_league(self, league_key: str):
        return {"status": "ok", "league_key": league_key, "stats_rows": 1, "matches": 2}

    def list_teams(self, league_key: str):
        return [{"name": "Beer Frame"}]

    def team_stats(self, league_key: str, team_name: str):
        return [{"player_name": "Gino", "average": 180}]

    def player_stats(self, league_key: str, player_name: str):
        return [{"player_name": "Gino", "average": 180}]

    def list_matches(self, league_key: str, team_name=None, date_from=None, date_to=None):
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
