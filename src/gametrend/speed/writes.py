"""MongoDB write operations of speed (architecture/speed/README.md, Conditional upsert pattern).

Each event kind owns a group of fields. One guarded `UpdateOne` per group, game and bucket: the fields the
observation has are `$set`, the group's other fields are `$unset`, so a newer observation never keeps
derived values of an older one. The guard (`observed_at`, `votes_observed_at`, `games.observed_at`) rejects
older observations; the rejected upsert then fails with E11000, which is counted, not retried.
"""

from bson.int64 import Int64
from pymongo import UpdateOne

from ..common.metrics import bucket_start
from .parse import MetaEvent, VotesEvent
from .scoring import PlayerUpdate

GAME_METRICS = "game_metrics"
GAMES = "games"

PLAYER_FIELDS = {
    "roblox": ("players", "visits", "favorites", "updated", "new_visits", "growth_1h", "growth_3h", "growth_24h"),
    "steam": ("players", "growth_1h", "growth_3h", "growth_24h"),
}
VOTES_FIELDS = ("up", "down")
META_FIELDS = {
    "roblox": ("name", "genre_l1", "genre_l2", "description", "created"),
    "steam": ("name", "genres", "description", "released"),
}
LONG_FIELDS = frozenset({"visits", "favorites", "up", "down"})  # BSON Long in the contract

DUPLICATE_KEY = 11000


class WriteFailed(RuntimeError):
    """A bulk write failed for another reason than a rejected (older) observation."""


def _bson(name: str, value):
    return Int64(value) if name in LONG_FIELDS else value


def guarded_upsert(key: dict, guard: str, observed_at, values: dict, group) -> UpdateOne:
    """`$set` the present fields of `group` and `guard`, `$unset` the absent ones, unless a newer guard exists."""
    unknown = set(values) - set(group)
    if unknown:
        raise ValueError(f"fields {sorted(unknown)} are not in the group {group}")
    present = {name: _bson(name, values[name]) for name in group if values.get(name) is not None}
    update = {"$set": {guard: observed_at, **present}}
    absent = {name: "" for name in group if name not in present}
    if absent:
        update["$unset"] = absent
    # $not $gte matches a missing, null or older guard
    return UpdateOne({**key, guard: {"$not": {"$gte": observed_at}}}, update, upsert=True)


def _metrics_key(platform: str, gid: int, bucket) -> dict:
    return {"platform": platform, "id": Int64(gid), "ts": bucket}


def player_op(u: PlayerUpdate) -> UpdateOne:
    e = u.event
    values = {"players": e.players, "visits": e.visits, "favorites": e.favorites, "updated": e.updated,
              "new_visits": u.new_visits, **u.growths}
    group = PLAYER_FIELDS[e.platform]
    values = {k: v for k, v in values.items() if k in group}
    return guarded_upsert(_metrics_key(e.platform, e.id, u.bucket), "observed_at", e.observed_at, values, group)


def votes_op(e: VotesEvent) -> UpdateOne:
    return guarded_upsert(_metrics_key(e.platform, e.id, bucket_start(e.observed_at)), "votes_observed_at",
                          e.observed_at, {"up": e.up, "down": e.down}, VOTES_FIELDS)


def meta_op(e: MetaEvent) -> UpdateOne:
    return guarded_upsert({"platform": e.platform, "id": Int64(e.id)}, "observed_at", e.observed_at,
                          e.fields, META_FIELDS[e.platform])


def summarize(details: dict) -> dict:
    """Counts of a bulk write result or of `BulkWriteError.details`.

    E11000 errors are rejected older (or already written) observations. Any other write error or a
    write concern error raises WriteFailed: the micro-batch fails and Spark repeats it after a restart.
    """
    errors = details.get("writeErrors") or []
    other = [e for e in errors if e.get("code") != DUPLICATE_KEY]
    if other or details.get("writeConcernErrors"):
        first = (other or details.get("writeConcernErrors"))[0]
        raise WriteFailed(f"bulk write failed: code {first.get('code')}: {first.get('errmsg')}")
    return {"matched": details.get("nMatched", 0), "modified": details.get("nModified", 0),
            "upserted": details.get("nUpserted", 0), "rejected": len(errors)}
