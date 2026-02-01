from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional


DEFAULT_CONFIG_PATH = os.path.join("apps", "api", "data", "bowling.json")


def load_bowling_config(path: Optional[str] = None) -> Dict[str, Any]:
    config_path = path or os.getenv("BOWLING_CONFIG_PATH", DEFAULT_CONFIG_PATH)
    if not os.path.exists(config_path):
        return {"leagues": []}
    with open(config_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def get_league(config: Dict[str, Any], league_key: str) -> Optional[Dict[str, Any]]:
    for league in config.get("leagues", []):
        if league.get("key") == league_key:
            return league
    return None
