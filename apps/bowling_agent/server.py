from __future__ import annotations

import os
from typing import Dict, List, Optional

try:
    from mcp.server.fastmcp import FastMCP
except Exception as exc:  # pragma: no cover - optional dependency resolution
    FastMCP = None
    _MCP_IMPORT_ERROR = exc

from packages.core.bowling.service import BowlingService


def _build_mcp() -> "FastMCP":
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    return FastMCP("HomeOpsBowlingAgent", json_response=True)


mcp = _build_mcp()


def _service() -> BowlingService:
    config_path = os.getenv("BOWLING_CONFIG_PATH")
    db_path = os.getenv("HOME_OPS_DB_PATH")
    return BowlingService(config_path=config_path, db_path=db_path)


@mcp.tool()
def list_leagues() -> List[Dict[str, object]]:
    """List configured bowling leagues."""
    return _service().list_leagues()


@mcp.tool()
def sync_league(league_key: str) -> Dict[str, object]:
    """Sync stats and schedules for a league."""
    return _service().sync_league(league_key)


@mcp.tool()
def list_teams(league_key: str) -> List[Dict[str, object]]:
    """List teams for a league."""
    return _service().list_teams(league_key)


@mcp.tool()
def team_stats(league_key: str, team_name: str) -> List[Dict[str, object]]:
    """Get stats for a team."""
    return _service().team_stats(league_key, team_name)


@mcp.tool()
def player_stats(league_key: str, player_name: str) -> List[Dict[str, object]]:
    """Get stats for a player."""
    return _service().player_stats(league_key, player_name)


@mcp.tool()
def list_matches(
    league_key: str,
    team_name: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List matches for a league, optionally filtered."""
    return _service().list_matches(
        league_key, team_name=team_name, date_from=date_from, date_to=date_to
    )


def main() -> None:
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

try:
    from mcp.server.fastmcp import FastMCP
except Exception as exc:  # pragma: no cover - optional dependency resolution
    FastMCP = None
    _MCP_IMPORT_ERROR = exc

from packages.core.storage.base import BowlingSessionState
from packages.core.storage.sqlite import SQLiteListStore


def _build_mcp() -> "FastMCP":
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    return FastMCP("HomeOpsBowlingAgent", json_response=True)


mcp = _build_mcp()


def _store() -> SQLiteListStore:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(base_dir, "api", "data")
    db_path = os.getenv("HOME_OPS_DB_PATH", os.path.join(data_dir, "lists.db"))
    return SQLiteListStore(db_path=db_path)


@mcp.tool()
def log_session(
    date_iso: str, location: str, scores: List[int], notes: Optional[str] = None
) -> Dict[str, str]:
    """Log a bowling session with scores."""
    session_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    session = BowlingSessionState(
        session_id=session_id,
        date_iso=date_iso,
        location=location,
        scores=scores,
        notes=notes,
        created_at=timestamp,
        updated_at=timestamp,
    )
    _store().create_bowling_session(session)
    return {"status": "ok", "session_id": session_id}


@mcp.tool()
def list_sessions(limit: int = 20) -> List[Dict[str, object]]:
    """List recent bowling sessions."""
    sessions = _store().list_bowling_sessions(limit=limit)
    return [
        {
            "session_id": session.session_id,
            "date_iso": session.date_iso,
            "location": session.location,
            "scores": session.scores,
            "notes": session.notes,
        }
        for session in sessions
    ]


@mcp.tool()
def get_session(session_id: str) -> Optional[Dict[str, object]]:
    """Get a bowling session by id."""
    session = _store().get_bowling_session(session_id)
    if session is None:
        return None
    return {
        "session_id": session.session_id,
        "date_iso": session.date_iso,
        "location": session.location,
        "scores": session.scores,
        "notes": session.notes,
    }


@mcp.tool()
def delete_session(session_id: str) -> Dict[str, str]:
    """Delete a bowling session by id."""
    _store().delete_bowling_session(session_id)
    return {"status": "ok", "session_id": session_id}


@mcp.tool()
def stats() -> Dict[str, object]:
    """Compute simple bowling stats."""
    sessions = _store().list_bowling_sessions(limit=200)
    games = [score for session in sessions for score in session.scores]
    if not games:
        return {
            "games": 0,
            "average": 0,
            "best": None,
            "recent_session": None,
        }
    recent = sessions[0] if sessions else None
    return {
        "games": len(games),
        "average": sum(games) / len(games),
        "best": max(games),
        "recent_session": recent.session_id if recent else None,
    }


def main() -> None:
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
