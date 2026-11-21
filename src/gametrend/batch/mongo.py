"""MongoDB I/O of batch: hourly writes (one transaction per hour) and the retention sweep (no transaction).

Thin layer; decisions are in hourly.py and retention.py. Design: architecture/batch/README.md.
"""

from dataclasses import dataclass
from datetime import datetime

from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING, ReplaceOne

from .hours import HOUR
from .retention import hours_to_sweep, split_ids

GAME_METRICS = "game_metrics"
GAME_METRICS_HOURLY = "game_metrics_hourly"
PLATFORMS = ("roblox", "steam")
LONG_FIELDS = ("id", "up", "down")  # BSON Long in the contract


def to_bson(doc: dict) -> dict:
    return {k: Int64(v) if k in LONG_FIELDS else v for k, v in doc.items()}


def replace_ops(docs: list[dict]) -> list[ReplaceOne]:
    """Whole-document upserts: batch owns every field, so a field absent after a recompute disappears."""
    ops = []
    for doc in docs:
        d = to_bson(doc)
        ops.append(ReplaceOne({"platform": d["platform"], "id": d["id"], "hour_ts": d["hour_ts"]}, d, upsert=True))
    return ops


@dataclass
class SweepReport:
    hours: int = 0                   # (platform, hour) pairs that had 5-minute documents
    deleted: int = 0
    kept_games: int = 0              # (game, hour) pairs kept because the hour has no hourly document
    oldest_kept: datetime | None = None


class BatchStore:
    def __init__(self, client, db):
        self.client = client
        self.db = db

    def last_hour(self) -> datetime | None:
        doc = self.db[GAME_METRICS_HOURLY].find_one({}, {"hour_ts": 1}, sort=[("hour_ts", DESCENDING)])
        return None if doc is None else doc["hour_ts"]

    def write_hour(self, docs: list[dict]) -> None:
        """All documents of one hour in one transaction: readers never see half an hour recomputed."""
        ops = replace_ops(docs)
        if not ops:
            return
        with self.client.start_session() as session:
            session.with_transaction(
                lambda s: self.db[GAME_METRICS_HOURLY].bulk_write(ops, ordered=False, session=s))

    def oldest_5min(self, platform: str, before: datetime) -> datetime | None:
        doc = self.db[GAME_METRICS].find_one({"platform": platform, "ts": {"$lt": before}}, {"ts": 1},
                                             sort=[("ts", ASCENDING)])
        return None if doc is None else doc["ts"]

    def sweep_hour(self, platform: str, hour: datetime) -> tuple[int, int]:
        """(deleted documents, kept games) for one (platform, hour)."""
        in_hour = {"platform": platform, "ts": {"$gte": hour, "$lt": hour + HOUR}}
        ids_5min = self.db[GAME_METRICS].distinct("id", in_hour)
        if not ids_5min:
            return 0, 0
        ids_hourly = self.db[GAME_METRICS_HOURLY].distinct("id", {"platform": platform, "hour_ts": hour})
        delete, keep = split_ids(ids_5min, ids_hourly)
        deleted = 0
        if delete:
            result = self.db[GAME_METRICS].delete_many({**in_hour, "id": {"$in": [Int64(i) for i in delete]}})
            deleted = result.deleted_count
        return deleted, len(keep)

    def sweep(self, cutoff: datetime) -> SweepReport:
        report = SweepReport()
        for platform in PLATFORMS:
            for hour in hours_to_sweep(self.oldest_5min(platform, cutoff), cutoff):
                deleted, kept = self.sweep_hour(platform, hour)
                if not deleted and not kept:
                    continue  # no 5-minute documents in this hour
                report.hours += 1
                report.deleted += deleted
                report.kept_games += kept
                if kept and (report.oldest_kept is None or hour < report.oldest_kept):
                    report.oldest_kept = hour
        return report
