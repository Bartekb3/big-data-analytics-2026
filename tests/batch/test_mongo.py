"""BatchStore against in-memory fakes of the pymongo calls it uses (no MongoDB)."""

from datetime import datetime, timezone

from bson.int64 import Int64

from gametrend.batch.mongo import GAME_METRICS, GAME_METRICS_HOURLY, BatchStore, replace_ops


def t(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, tzinfo=timezone.utc)


def _match(doc, flt):
    for key, cond in flt.items():
        value = doc.get(key)
        if isinstance(cond, dict):
            if "$in" in cond and value not in cond["$in"]:
                return False
            if "$gte" in cond and not value >= cond["$gte"]:
                return False
            if "$lt" in cond and not value < cond["$lt"]:
                return False
        elif value != cond:
            return False
    return True


class FakeCollection:
    def __init__(self, docs=()):
        self.docs = list(docs)
        self.bulk = []

    def find_one(self, flt, projection=None, sort=None):
        found = [d for d in self.docs if _match(d, flt)]
        if sort:
            key, direction = sort[0]
            found.sort(key=lambda d: d[key], reverse=direction < 0)
        return found[0] if found else None

    def distinct(self, key, flt):
        return sorted({d[key] for d in self.docs if _match(d, flt)})

    def delete_many(self, flt):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _match(d, flt)]

        class Result:
            deleted_count = before - len(self.docs)
        return Result()

    def bulk_write(self, ops, ordered, session):
        assert session is not None  # always inside the transaction
        self.bulk.append(list(ops))


class FakeSession:
    def __init__(self):
        self.transactions = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def with_transaction(self, callback):
        self.transactions += 1
        return callback(self)


class FakeClient:
    def __init__(self):
        self.session = FakeSession()

    def start_session(self):
        return self.session


def store(metrics=(), hourly=()):
    db = {GAME_METRICS: FakeCollection(metrics), GAME_METRICS_HOURLY: FakeCollection(hourly)}
    client = FakeClient()
    return BatchStore(client, db), db, client


def m(gid, ts, platform="roblox"):
    return {"platform": platform, "id": Int64(gid), "ts": ts}


def h(gid, hour, platform="roblox"):
    return {"platform": platform, "id": Int64(gid), "hour_ts": hour}


def test_replace_ops_use_the_key_and_longs():
    op = replace_ops([{"platform": "roblox", "id": 5, "hour_ts": t(6, 13), "players": 3, "up": 1, "down": 0}])[0]
    doc = op._doc
    assert op._filter == {"platform": "roblox", "id": 5, "hour_ts": t(6, 13)}
    assert isinstance(op._filter["id"], Int64)
    assert isinstance(doc["up"], Int64) and isinstance(doc["down"], Int64)
    assert type(doc["players"]) is int
    assert op._upsert is True


def test_write_hour_is_one_transaction_and_skips_empty_hours():
    s, db, client = store()
    s.write_hour([{"platform": "roblox", "id": 1, "hour_ts": t(6, 13)},
                  {"platform": "steam", "id": 2, "hour_ts": t(6, 13)}])
    s.write_hour([])
    assert client.session.transactions == 1
    assert len(db[GAME_METRICS_HOURLY].bulk) == 1 and len(db[GAME_METRICS_HOURLY].bulk[0]) == 2


def test_last_hour():
    s, _, _ = store(hourly=[h(1, t(6, 12)), h(2, t(6, 14)), h(1, t(6, 13))])
    assert s.last_hour() == t(6, 14)
    assert store()[0].last_hour() is None


def test_sweep_deletes_only_games_with_an_hourly_document():
    metrics = [m(1, t(6, 10, 0)), m(1, t(6, 10, 5)), m(2, t(6, 10, 0)),   # 2 has no hourly doc for 10:00
               m(1, t(6, 11, 0)),                                         # 11:00 has hourly doc
               m(1, t(6, 13, 0)),                                         # after the cutoff: untouched
               m(7, t(6, 10, 0), platform="steam")]
    hourly = [h(1, t(6, 10)), h(1, t(6, 11)), h(2, t(6, 9)), h(7, t(6, 10), platform="steam")]
    s, db, _ = store(metrics, hourly)
    report = s.sweep(t(6, 12))
    left = {(d["platform"], int(d["id"]), d["ts"]) for d in db[GAME_METRICS].docs}
    assert left == {("roblox", 2, t(6, 10, 0)), ("roblox", 1, t(6, 13, 0))}
    assert (report.deleted, report.kept_games, report.oldest_kept) == (4, 1, t(6, 10))
    assert report.hours == 3  # roblox 10:00, 11:00; steam 10:00


def test_sweep_with_nothing_old():
    s, db, _ = store([m(1, t(6, 13))], [h(1, t(6, 13))])
    report = s.sweep(t(6, 12))
    assert (report.hours, report.deleted) == (0, 0)
    assert len(db[GAME_METRICS].docs) == 1
