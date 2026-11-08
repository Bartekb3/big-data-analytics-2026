"""Routes, freshness limits and settings of the mock API (documented in architecture/mockapi/README.md)."""

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

from ..collector import config as collector_config

# feed -> URL path of the real endpoint (one port serves all of them; the paths do not overlap)
FEED_PATH = {
    "roblox_games": urlsplit(collector_config.ROBLOX_GAMES_URL).path,
    "roblox_votes": urlsplit(collector_config.ROBLOX_VOTES_URL).path,
    "roblox_sorts": urlsplit(collector_config.ROBLOX_SORTS_URL).path,
    "roblox_search": urlsplit(collector_config.ROBLOX_SEARCH_URL).path,
    "steam_charts": urlsplit(collector_config.STEAM_CHARTS_URL).path,
    "steam_players": urlsplit(collector_config.STEAM_PLAYERS_URL).path,
    "steam_store_new": urlsplit(collector_config.STEAM_STORE_SEARCH_URL).path,
    "steam_appdetails": urlsplit(collector_config.STEAM_APPDETAILS_URL).path,
}
PATH_FEED = {path.rstrip("/"): feed for feed, path in FEED_PATH.items()}

FEEDS = tuple(FEED_PATH)

# feed -> how old the latest observation may be and still be served; None = no limit
FRESHNESS = {
    "roblox_games": timedelta(minutes=10),
    "roblox_votes": timedelta(minutes=15),
    "steam_players": timedelta(minutes=15),
    "roblox_sorts": timedelta(minutes=45),
    "steam_charts": timedelta(days=2),
    "steam_store_new": timedelta(days=2),
    "roblox_search": timedelta(days=2),
    "steam_appdetails": None,
}

# feeds whose IDs /_mock/ids lists
ID_FEEDS = {"roblox": ("roblox_games", "roblox_votes"), "steam": ("steam_players",)}

MAX_SORT_PAGES = collector_config.Settings.max_sort_pages
RESCAN_EVERY = timedelta(seconds=60)  # follow mode: look for new archive files at most this often (replay time)
# an error window lasts until the next usable attempt of the feed, but at most this long after its last
# failure (the collector's back-off cap): a failure with no later attempt must not block the feed for hours
ERROR_WINDOW_TAIL = timedelta(seconds=120)
MAX_GAMES_IDS = 50  # the real /v1/games answers 400 above this (the real votes endpoint accepted 51)
SORT_CYCLES_KEPT = 2  # complete cycles kept per country, so tokens of the previous cycle still work


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    replay_errors: bool = True  # serve archived error windows
    inject_p: float = 0.0  # probability of an injected error per request
    inject_delay_s: float = 35.0  # injected "timeout": answer after this delay (beyond a 30 s client timeout)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"


def data_dir_from_env(data_dir: str | None = None) -> Path:
    raw = data_dir or os.environ.get("MOCKAPI_DATA_DIR") or os.environ.get("COLLECTOR_DATA_DIR") or "~/gametrend-data"
    return Path(raw).expanduser()
