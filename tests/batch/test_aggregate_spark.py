"""Sink -> master -> hourly documents on a real (local) Spark. Runs in the Spark image: pytest -m spark."""

from datetime import datetime, timezone

import pytest

from gametrend.batch.aggregate import SparkMaster
from gametrend.batch.config import Settings
from gametrend.batch.sink import run_sink

from .helpers import RT, ST, game, sort, steam_players, votes

pytestmark = pytest.mark.spark

SOURCE_SCHEMA = "topic string, value string, kafka_partition bigint, kafka_offset bigint"
H = datetime(2026, 11, 6, 13, tzinfo=timezone.utc)

RECORDS = [
    (RT, game(ts="2026-11-06T12:58:00.000Z", playing=5, visits=1000), 0, 1),
    (RT, game(ts="2026-11-06T13:01:00.000Z", playing=10, visits=1010), 0, 2),
    (RT, game(ts="2026-11-06T13:56:00.000Z", playing=15, visits=1100), 0, 3),
    (RT, votes(ts="2026-11-06T13:52:00.000Z", up=60, down=6), 0, 4),
    (RT, sort(ts="2026-11-06T13:10:00.000Z"), 0, 5),
    (ST, steam_players(ts="2026-11-06T13:15:00.000Z", players=0), 0, 1),
    (ST, steam_players(ts="2026-11-07T00:10:00.000Z", players=4), 0, 2),
]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("batch-aggregate-test")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


def _archive(spark, tmp_path, records):
    source_dir = tmp_path / "source"
    spark.createDataFrame(records, SOURCE_SCHEMA).write.mode("append").parquet(str(source_dir))
    source = spark.readStream.schema(SOURCE_SCHEMA).parquet(str(source_dir))
    settings = Settings("unused", "unused", str(tmp_path / "checkpoint"), str(tmp_path / "master"))
    run_sink(spark, settings, source=source)
    return SparkMaster(spark, settings.master_dir)


def _by_key(docs):
    return {(d["platform"], d["id"], d["hour_ts"]): d for d in docs}


def test_now_earliest_and_hourly_documents(spark, tmp_path):
    master = _archive(spark, tmp_path, RECORDS)
    assert master.latest_ts() == {"roblox": datetime(2026, 11, 6, 13, 56, tzinfo=timezone.utc),
                                  "steam": datetime(2026, 11, 7, 0, 10, tzinfo=timezone.utc)}
    assert master.earliest_ts() == datetime(2026, 11, 6, 12, 58, tzinfo=timezone.utc)

    docs = _by_key(master.compute([H]))
    assert set(docs) == {("roblox", 1, H), ("steam", 730, H)}
    assert docs[("roblox", 1, H)] == {
        "platform": "roblox", "id": 1, "hour_ts": H, "players": 15, "peak_players": 15, "new_visits": 100,
        "updated": datetime(2026, 11, 1, 8, tzinfo=timezone.utc), "up": 60, "down": 6}
    assert docs[("steam", 730, H)] == {"platform": "steam", "id": 730, "hour_ts": H, "players": 0,
                                       "peak_players": 0}


def test_replayed_copies_with_other_offsets_change_nothing(spark, tmp_path):
    replay = [(topic, value, partition, offset + 100) for topic, value, partition, offset in RECORDS]
    master = _archive(spark, tmp_path, RECORDS + replay)
    single = _archive(spark, tmp_path / "single", RECORDS)
    assert _by_key(master.compute([H])) == _by_key(single.compute([H]))


def test_empty_master(spark, tmp_path):
    master = SparkMaster(spark, str(tmp_path / "nothing"))
    assert master.latest_ts() == {}
    assert master.earliest_ts() is None
    assert master.compute([H]) == []
