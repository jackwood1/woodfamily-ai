from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from packages.core.bowling.service import BowlingService
from packages.core.bowling.bopo_schedule import get_bopo_schedule
from packages.core.bowling.bopo_averages import get_bopo_averages
from packages.core.bowling.bopo_standings import get_bopo_standings
from packages.core.bowling.casco_monday import get_casco_monday


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
def list_teams(league_key: str, force_refresh: bool = False) -> List[Dict[str, Any]]:
    return _service().list_teams(league_key, force_refresh=force_refresh)


@router.get("/{league_key}/team-stats")
def team_stats(league_key: str, team_name: str, force_refresh: bool = False) -> List[Dict[str, Any]]:
    return _service().team_stats(league_key, team_name, force_refresh=force_refresh)


@router.get("/{league_key}/player-stats")
def player_stats(
    league_key: str, player_name: str, force_refresh: bool = False
) -> List[Dict[str, Any]]:
    return _service().player_stats(league_key, player_name, force_refresh=force_refresh)


@router.get("/{league_key}/matches")
def list_matches(
    league_key: str,
    team_name: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:
    return _service().list_matches(
        league_key,
        team_name=team_name,
        date_from=date_from,
        date_to=date_to,
        force_refresh=force_refresh,
    )


@router.get("/bopo/schedule")
def bopo_schedule(team_name: str) -> Dict[str, Any]:
    result = get_bopo_schedule(team_name=team_name)
    if result.get("status") != "ok":
        raise HTTPException(status_code=400, detail=result.get("error", "bopo_failed"))
    return result


@router.get("/bopo/averages")
def bopo_averages(
    team_name: Optional[str] = None, player_name: Optional[str] = None
) -> Dict[str, Any]:
    result = get_bopo_averages(team_name=team_name, player_name=player_name)
    if result.get("status") != "ok":
        raise HTTPException(status_code=400, detail=result.get("error", "bopo_failed"))
    return result


@router.get("/bopo/standings")
def bopo_standings(
    day: Optional[str] = None, team_name: Optional[str] = None
) -> Dict[str, Any]:
    result = get_bopo_standings(day=day, team_name=team_name)
    if result.get("status") != "ok":
        raise HTTPException(status_code=400, detail=result.get("error", "bopo_failed"))
    return result


@router.get("/casco/monday")
def casco_monday(team_name: Optional[str] = None) -> Dict[str, Any]:
    result = get_casco_monday(team_name=team_name)
    if result.get("status") != "ok":
        raise HTTPException(status_code=400, detail=result.get("error", "casco_failed"))
    return result
