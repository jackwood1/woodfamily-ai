import os

from packages.core.tools.google_tools import gmail_list_unread, calendar_list_upcoming


def test_google_tools_not_connected(tmp_path, monkeypatch):
    token_path = tmp_path / "google_tokens.json"
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", str(token_path))

    gmail = gmail_list_unread()
    assert gmail["error"] == "google_not_connected"

    calendar = calendar_list_upcoming()
    assert calendar["error"] == "google_not_connected"
