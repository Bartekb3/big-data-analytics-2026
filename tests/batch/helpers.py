import json

RT = "roblox.snapshots"
ST = "steam.players"
TS = "2026-11-06T13:01:00.000Z"
UPDATED = "2026-11-01T08:00:00.000Z"


def game(gid=1, ts=TS, playing=10, visits=100, favorites=1, updated=UPDATED):
    return json.dumps({"kind": "game", "ts": ts, "id": gid, "playing": playing, "visits": visits,
                       "favorites": favorites, "updated": updated})


def votes(gid=1, ts=TS, up=5, down=1):
    return json.dumps({"kind": "votes", "ts": ts, "id": gid, "up": up, "down": down})


def sort(gid=1, ts=TS, country="us", sort_id="top-trending", rank=1, playing=10):
    return json.dumps({"kind": "sort", "ts": ts, "id": gid, "country": country, "sort": sort_id, "rank": rank,
                       "playing": playing})


def roblox_meta(gid=1, ts=TS, **fields):
    return json.dumps({"kind": "meta", "ts": ts, "id": gid, "name": "A", **fields})


def steam_players(gid=730, ts=TS, players=5):
    return json.dumps({"kind": "players", "ts": ts, "id": gid, "players": players})


def steam_meta(gid=730, ts=TS, **fields):
    return json.dumps({"kind": "meta", "ts": ts, "id": gid, "name": "B", **fields})
