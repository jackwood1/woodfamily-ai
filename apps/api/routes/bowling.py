from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from packages.core.bowling.service import BowlingService


router = APIRouter(prefix="/api/bowling", tags=["bowling"])


def _service() -> BowlingService:
    return BowlingService()


@router.get("/leagues")
def list_leagues() -> List[Dict[str, Any]]:
    return _service().list_leagues()


@router.post("/{league_key}/sync")
def sync_league(league_key: str) -> Dict[str, Any]:
    result = _service().sync_league(league_key)
    if result.get("status") != "ok":
        raise HTTPException(status_code=400, detail=result.get("error", "sync_failed"))
    return result


@router.get("/{league_key}/teams")
def list_teams(league_key: str) -> List[Dict[str, Any]]:
    return _service().list_teams(league_key)


@router.get("/{league_key}/team-stats")
def team_stats(league_key: str, team_name: str) -> List[Dict[str, Any]]:
    return _service().team_stats(league_key, team_name)


@router.get("/{league_key}/player-stats")
def player_stats(league_key: str, player_name: str) -> List[Dict[str, Any]]:
    return _service().player_stats(league_key, player_name)


@router.get("/{league_key}/matches")
def list_matches(
    league_key: str,
    team_name: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return _service().list_matches(
        league_key, team_name=team_name, date_from=date_from, date_to=date_to
    )
