"""Small archives written to tmp_path, and a settable replay clock (no network)."""

import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

from gametrend.collector import config as cc
from gametrend.collector.records import utc_iso

T0 = datetime(2026, 11, 2, 14, 0, 0, tzinfo=timezone.utc)

URL = {
    "roblox_games": cc.ROBLOX_GAMES_URL, "roblox_votes": cc.ROBLOX_VOTES_URL, "roblox_sorts": cc.ROBLOX_SORTS_URL,
    "roblox_search": cc.ROBLOX_SEARCH_URL, "steam_charts": cc.STEAM_CHARTS_URL, "steam_players": cc.STEAM_PLAYERS_URL,
    "steam_store_new": cc.STEAM_STORE_SEARCH_URL, "steam_appdetails": cc.STEAM_APPDETAILS_URL,
}


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def rec(feed: str, seconds: float, params: dict | None = None, status: int | None = 200, body=None, **extra) -> dict:
    """A raw record like the collector writes (requested_at = T0 + seconds)."""
    url = URL[feed] + ("?" + urlencode(params) if params else "")
    r = {"feed": feed, "requested_at": utc_iso(at(seconds)), "received_at": utc_iso(at(seconds + 0.2)),
         "url": url, "elapsed_ms": 200}
    if status is not None:
        r["status"] = status
    if body is not None:
        r["body"] = body
    r.update(extra)
    return r


def games(seconds: float, items: dict[int, int], feed: str = "roblox_games") -> dict:
    """One /v1/games (or votes) call: {id: playing}."""
    key = "playing" if feed == "roblox_games" else "upVotes"
    data = [{"id": i, key: v, "name": f"game {i}"} for i, v in items.items()]
    return rec(feed, seconds, {"universeIds": ",".join(map(str, items))}, body={"data": data})


def players(seconds: float, appid: int, count: int | None) -> dict:
    if count is None:
        return rec("steam_players", seconds, {"appid": appid}, status=404, body={"response": {"result": 42}})
    return rec("steam_players", seconds, {"appid": appid}, body={"response": {"player_count": count, "result": 1}})


def sorts_cycle(seconds: float, country: str = "us", pages: int = 3, tag: str = "") -> list[dict]:
    """One explore-sorts cycle: pages 1 s apart, the last one without a next token."""
    out = []
    for n in range(pages):
        params = {"country": country} if n == 0 else {"country": country, "sortsPageToken": f"real{n}"}
        body = {"sorts": [{"sortId": f"{tag}s{n}", "games": [{"universeId": 100 + n}]}],
                "nextSortsPageToken": f"real{n + 1}" if n + 1 < pages else ""}
        out.append(rec("roblox_sorts", seconds + n, params, body=body))
    return out


def write(raw: Path, records: list[dict], name: str | None = None) -> None:
    """Write records into hour files (raw/<feed>/date=.../HH.jsonl.gz, or `name` instead of HH.jsonl.gz)."""
    files: dict[Path, list[str]] = {}
    for r in records:
        t = r["requested_at"]
        day, hour = t[:10], t[11:13]
        path = raw / r["feed"] / f"date={day}" / (name or f"{hour}.jsonl.gz")
        files.setdefault(path, []).append(json.dumps(r) if isinstance(r, dict) else r)
    for path, lines in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "at", encoding="utf-8") as fh:
            fh.write("".join(line + "\n" for line in lines))


class SetClock:
    """Replay clock whose time the test sets directly."""

    speed = 1.0

    def __init__(self, t: datetime = T0):
        self.t = t

    def now(self) -> datetime:
        return self.t

    def set(self, seconds: float) -> None:
        self.t = at(seconds)
