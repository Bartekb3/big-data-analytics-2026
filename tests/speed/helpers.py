import json
from datetime import datetime, timezone

from gametrend.speed.parse import PlayerEvent

RT = "roblox.snapshots"
ST = "steam.players"


def t(hhmmss: str, day: int = 6) -> datetime:
    """2026-11-<day> hh:mm[:ss] UTC."""
    parts = [int(p) for p in hhmmss.split(":")] + [0]
    return datetime(2026, 11, day, parts[0], parts[1], parts[2], tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def game_json(gid: int, at: datetime, playing: int, visits: int, favorites: int = 1, updated: datetime | None = None):
    return json.dumps({"kind": "game", "ts": iso(at), "id": gid, "playing": playing, "visits": visits,
                       "favorites": favorites, "updated": iso(updated or t("00:00"))})


def votes_json(gid: int, at: datetime, up: int, down: int):
    return json.dumps({"kind": "votes", "ts": iso(at), "id": gid, "up": up, "down": down})


def steam_json(gid: int, at: datetime, players: int):
    return json.dumps({"kind": "players", "ts": iso(at), "id": gid, "players": players})


def pe(at: datetime, players: int | None = 10, visits: int | None = None, gid: int = 1, platform: str = "roblox",
       favorites: int | None = None):
    if platform == "roblox":
        return PlayerEvent(platform, gid, at, players=players, visits=visits,
                           favorites=1 if favorites is None else favorites, updated=t("00:00"))
    return PlayerEvent(platform, gid, at, players=players)


def doc(at: datetime, players=None, visits=None, gid: int = 1, platform: str = "roblox", ts: datetime | None = None):
    from gametrend.common.metrics import bucket_start

    d = {"platform": platform, "id": gid, "ts": ts or bucket_start(at), "observed_at": at}
    if players is not None:
        d["players"] = players
    if visits is not None:
        d["visits"] = visits
    return d


class FakeStore:
    """Records reads and writes; answers history reads with the given documents (filtered by id only)."""

    def __init__(self, docs=(), write_result=None):
        self.docs = list(docs)
        self.filters = []
        self.written = {}
        self.write_result = write_result or {"matched": 0, "modified": 0, "upserted": 0, "rejected": 0}

    def read_history(self, flt):
        self.filters.append(flt)
        ids = {int(i) for i in flt["id"]["$in"]}
        return [d for d in self.docs if d["platform"] == flt["platform"] and d["id"] in ids]

    def write(self, collection, ops):
        self.written[collection] = list(ops)
        return dict(self.write_result)
