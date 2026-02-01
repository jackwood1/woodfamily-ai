from packages.core.bowling.config import get_league, load_bowling_config


def test_load_bowling_config_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("BOWLING_CONFIG_PATH", str(tmp_path / "missing.json"))
    config = load_bowling_config()
    assert config["leagues"] == []


def test_get_league(tmp_path):
    config_path = tmp_path / "bowling.json"
    config_path.write_text('{"leagues":[{"key":"monday_bayside","name":"Monday"}]}')
    config = load_bowling_config(str(config_path))
    league = get_league(config, "monday_bayside")
    assert league["name"] == "Monday"
