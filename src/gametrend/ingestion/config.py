"""Endpoints, pacing, polling plan and settings of the producers.

Values and their reasons: architecture/ingestion/README.md (mock vs real APIs) and the polling plan in
architecture/mockapi/README.md. Settings come from the environment (see .env.example).
"""

import os
from dataclasses import dataclass, field

# ------------------------------------------------------------------ endpoints

# API group -> real base URL. In mock mode every group uses the mock's base URL (paths stay the same).
REAL_BASE_URL = {
    "roblox_games_api": "https://games.roblox.com",
    "roblox_apis": "https://apis.roblox.com",
    "steam_api": "https://api.steampowered.com",
    "steam_store": "https://store.steampowered.com",
}

# feed -> (API group, path)
ENDPOINT = {
    "roblox_games": ("roblox_games_api", "/v1/games"),
    "roblox_votes": ("roblox_games_api", "/v1/games/votes"),
    "roblox_sorts": ("roblox_apis", "/explore-api/v1/get-sorts"),
    "steam_players": ("steam_api", "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"),
    "steam_appdetails": ("steam_store", "/api/appdetails"),
}

MOCK_IDS_PATH = "/_mock/ids"

USER_AGENT = "GameTrendRadar-student-project/0.1 (ingestion)"

# ------------------------------------------------------------------ pacing

# feed -> rate limiter key; feeds sharing a key share one minimum interval between request starts
FEED_LIMITER = {
    "roblox_games": "roblox_games",
    "roblox_votes": "roblox_votes",
    "roblox_sorts": "roblox_explore",
    "steam_players": "steam_api",
    "steam_appdetails": "steam_store",
}

# limiter key -> minimum seconds between request starts (measured by the collector, see collector § Pacing)
LIMITER_INTERVAL = {
    "roblox_games": 3.0,
    "roblox_votes": 1.0,
    "roblox_explore": 1.0,
    "steam_api": 1.0,
    "steam_store": 2.0,
}

# stream source -> (pace, cycle) of the polling plan; spacing = max(pace, min(cycle / N, MAX_SPACING_S))
STREAM_PLAN = {
    "roblox_games": (3.0, 60.0),
    "roblox_votes": (1.0, 300.0),
    "steam_players": (1.0, 300.0),
}
MAX_SPACING_S = 14.0  # course rule: a request to every stream source at least every 15 s

BATCH_SIZE = {"roblox_games": 50, "roblox_votes": 50, "steam_players": 1}

SORTS_COUNTRIES = ("us", "br", "pl")
SORTS_INTERVAL_S = 15 * 60
SORTS_MAX_PAGES = 10
IDS_REFRESH_S = 60.0
HTTP_TIMEOUT_S = 10.0
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = {"mock": 10.0, "real": 120.0}
STATS_EVERY_S = 60.0


@dataclass(frozen=True)
class Settings:
    mode: str = "mock"  # "mock" | "real"
    mock_url: str = "http://127.0.0.1:8080"
    kafka_bootstrap: str = "127.0.0.1:9094"
    countries: tuple[str, ...] = SORTS_COUNTRIES
    # divides every interval (pace, cycle, sorts, refresh) for an accelerated mock (REPLAY_SPEED); the 14 s
    # upper limit of the spacing stays, so the 15 s rule holds at any speed
    speed: float = 1.0
    workers: int = 32
    base_url: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.mode not in ("mock", "real"):
            raise ValueError(f"mode must be 'mock' or 'real', not {self.mode!r}")
        if self.speed <= 0:
            raise ValueError("speed must be > 0")
        if not self.base_url:
            urls = REAL_BASE_URL if self.mode == "real" else {g: self.mock_url.rstrip("/") for g in REAL_BASE_URL}
            object.__setattr__(self, "base_url", dict(urls))

    def url(self, feed: str) -> str:
        group, path = ENDPOINT[feed]
        return self.base_url[group] + path

    @property
    def backoff_cap_s(self) -> float:
        return BACKOFF_CAP_S[self.mode] / self.speed

    def scaled(self, seconds: float) -> float:
        return seconds / self.speed


def settings_from_env(**overrides) -> Settings:
    env = {
        "mode": os.environ.get("INGEST_MODE"),
        "mock_url": os.environ.get("INGEST_MOCK_URL"),
        "kafka_bootstrap": os.environ.get("KAFKA_BOOTSTRAP"),
        "countries": tuple(c.strip() for c in os.environ["INGEST_ROBLOX_COUNTRIES"].split(",") if c.strip())
        if os.environ.get("INGEST_ROBLOX_COUNTRIES") else None,
        "speed": float(os.environ["INGEST_SPEED"]) if os.environ.get("INGEST_SPEED") else None,
    }
    kwargs = {k: v for k, v in env.items() if v is not None}
    kwargs.update({k: v for k, v in overrides.items() if v is not None})
    return Settings(**kwargs)
