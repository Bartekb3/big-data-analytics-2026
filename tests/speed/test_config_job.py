import pytest

from gametrend.speed.config import DEFAULT_MAX_OFFSETS_PER_TRIGGER, settings_from_env
from gametrend.speed.job import MongoStore, make_handler
from gametrend.speed.writes import WriteFailed

ENV = {"KAFKA_BOOTSTRAP": "kafka:9092", "MONGO_URI": "mongodb://m", "SPEED_CHECKPOINT_DIR": "/cp"}


def test_settings_defaults_and_override():
    s = settings_from_env(ENV)
    assert s.max_offsets_per_trigger == DEFAULT_MAX_OFFSETS_PER_TRIGGER == 50_000
    assert settings_from_env({**ENV, "SPEED_MAX_OFFSETS_PER_TRIGGER": "1000"}).max_offsets_per_trigger == 1000


def test_settings_missing_or_bad():
    with pytest.raises(ValueError, match="SPEED_CHECKPOINT_DIR"):
        settings_from_env({k: v for k, v in ENV.items() if k != "SPEED_CHECKPOINT_DIR"})
    with pytest.raises(ValueError):
        settings_from_env({**ENV, "SPEED_MAX_OFFSETS_PER_TRIGGER": "0"})


class _Coll:
    def __init__(self, exc=None, result=None):
        self.exc, self.result = exc, result

    def bulk_write(self, ops, ordered):
        assert ordered is False
        if self.exc:
            raise self.exc
        return self.result


class _Result:
    bulk_api_result = {"nMatched": 1, "nModified": 1, "nUpserted": 0, "writeErrors": []}


def test_mongo_store_counts_duplicate_keys_and_raises_on_others():
    from pymongo.errors import BulkWriteError

    dup = BulkWriteError({"nMatched": 0, "nModified": 0, "nUpserted": 1, "writeErrors": [{"code": 11000}]})
    assert MongoStore({"c": _Coll(exc=dup)}).write("c", [object()])["rejected"] == 1
    other = BulkWriteError({"writeErrors": [{"code": 2, "errmsg": "bad"}]})
    with pytest.raises(WriteFailed):
        MongoStore({"c": _Coll(exc=other)}).write("c", [object()])
    assert MongoStore({"c": _Coll(result=_Result())}).write("c", [object()])["matched"] == 1
    assert MongoStore({}).write("c", [])["upserted"] == 0  # nothing to write: no call


class _Row(dict):
    pass


class _DF:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *cols):
        assert cols == ("topic", "value")
        return self

    def collect(self):
        return [_Row(r) for r in self.rows]


def test_handler_passes_records_and_logs():
    seen, lines = [], []

    class Log:
        def info(self, line):
            lines.append(line)

    def process(batch_id, records, store):
        seen.append((batch_id, records, store))
        from gametrend.speed.microbatch import BatchReport
        from collections import Counter
        return BatchReport(batch_id, Counter())

    make_handler("store", process=process, logger=Log())(_DF([{"topic": "t", "value": "v"}]), 3)
    assert seen == [(3, [("t", "v")], "store")]
    assert lines[0].startswith("batch 3:")
