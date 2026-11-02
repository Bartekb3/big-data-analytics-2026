"""Endpoints, pacing, job cadences and settings of the collector.

The values are documented (with the measurements behind them) in architecture/collector/README.md.
"""

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# ------------------------------------------------------------------ endpoints

ROBLOX_SORTS_URL = "https://apis.roblox.com/explore-api/v1/get-sorts"
ROBLOX_SEARCH_URL = "https://apis.roblox.com/search-api/omni-search"
ROBLOX_GAMES_URL = "https://games.roblox.com/v1/games"
ROBLOX_VOTES_URL = "https://games.roblox.com/v1/games/votes"
STEAM_CHARTS_URL = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
STEAM_PLAYERS_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
STEAM_STORE_SEARCH_URL = "https://store.steampowered.com/search/results/"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"

USER_AGENT = "GameTrendRadar-student-project/0.1 (raw data collector)"

# ------------------------------------------------------------------ feeds and pacing

# feed name -> pacer key; feeds sharing a key share one minimum interval between requests
FEED_PACER = {
    "roblox_sorts": "roblox_explore",
    "roblox_search": "roblox_search",
    "roblox_games": "roblox_games",
    "roblox_votes": "roblox_votes",
    "steam_charts": "steam_api",
    "steam_players": "steam_api",
    "steam_store_new": "steam_store",
    "steam_appdetails": "steam_store",
}

# pacer key -> minimum seconds between request starts
PACER_INTERVAL = {
    "roblox_explore": 1.0,
    "roblox_search": 1.0,
    "roblox_games": 3.0,  # hidden limit: 429 after ~550 IDs in a burst
    "roblox_votes": 1.0,
    "steam_api": 1.0,
    "steam_store": 2.0,
}

FEEDS = tuple(FEED_PACER)

# ------------------------------------------------------------------ jobs


@dataclass(frozen=True)
class Job:
    name: str
    interval_s: float  # minimum time between cycle starts
    feeds: tuple[str, ...]
    retry_empty_s: float = 120  # sooner retry when a cycle had nothing to do (e.g. empty ID list)


JOBS = {
    j.name: j
    for j in (
        Job("roblox-sorts", 15 * 60, ("roblox_sorts",)),
        Job("steam-lists", 6 * 3600, ("steam_charts", "steam_store_new", "steam_appdetails")),
        Job("roblox-search", 24 * 3600, ("roblox_search",)),
        Job("roblox-games", 60, ("roblox_games",)),
        Job("roblox-votes", 5 * 60, ("roblox_votes",)),
        Job("steam-players", 5 * 60, ("steam_players",)),
    )
}

JOB_GROUPS = {
    "discovery": ("roblox-sorts", "steam-lists", "roblox-search"),
    "snapshots": ("roblox-games", "roblox-votes", "steam-players"),
}
JOB_GROUPS["all"] = JOB_GROUPS["discovery"] + JOB_GROUPS["snapshots"]


# ------------------------------------------------------------------ settings


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    roblox_countries: tuple[str, ...] = ("us", "br", "pl")
    max_sort_pages: int = 10
    max_store_pages: int = 5
    roblox_batch: int = 50  # max universeIds per games / votes call
    id_max_age_days: int = 7  # IDs not seen in discovery for this long are dropped
    max_roblox_ids: int = 3000
    max_steam_ids: int = 500
    candidate_max_age_days: int = 90  # Steam release age for clone candidates
    max_retries: int = 6  # sporadic 429 streaks from Roblox cleared in 30-60 s; 6 back-offs wait ~2.5-4.5 min
    timeout_s: float = 30.0
    pacer_interval: dict[str, float] = field(default_factory=lambda: dict(PACER_INTERVAL))
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))  # Roblox website endpoints accept any UUID

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def ids_path(self) -> Path:
        return self.data_dir / "state" / "ids.json"


def settings_from_env(data_dir: str | None = None) -> Settings:
    """Settings from CLI arguments and environment variables (see .env.example)."""
    raw = data_dir or os.environ.get("COLLECTOR_DATA_DIR") or "~/gametrend-data"
    countries = os.environ.get("COLLECTOR_ROBLOX_COUNTRIES")
    kwargs = {}
    if countries:
        kwargs["roblox_countries"] = tuple(c.strip() for c in countries.split(",") if c.strip())
    return Settings(data_dir=Path(raw).expanduser(), **kwargs)
