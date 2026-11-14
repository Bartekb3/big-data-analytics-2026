"""The sink on a real (local) Spark stream; a file source stands in for Kafka. Runs in the Spark image: pytest -m spark."""

import pytest

from gametrend.batch.config import Settings
from gametrend.batch.sink import run_sink

from .helpers import RT, ST, game, steam_meta, steam_players, votes

pytestmark = pytest.mark.spark

SOURCE_SCHEMA = "topic string, value string, kafka_partition bigint, kafka_offset bigint"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("batch-sink-test")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


def _run(spark, tmp_path, records):
    source_dir = tmp_path / "source"
    spark.createDataFrame(records, SOURCE_SCHEMA).write.mode("append").parquet(str(source_dir))
    source = spark.readStream.schema(SOURCE_SCHEMA).parquet(str(source_dir))
    settings = Settings("unused", "unused", str(tmp_path / "checkpoint"), str(tmp_path / "master"))
    return run_sink(spark, settings, source=source)


def test_sink_writes_partitioned_typed_rows_and_counts_invalid(spark, tmp_path):
    report = _run(spark, tmp_path, [
        (RT, game(visits=2**40), 0, 1),
        (RT, votes(ts="2026-11-07T00:00:00.000Z"), 1, 2),
        (ST, steam_players(players=0), 0, 3),
        (ST, steam_meta(released="2025-02-26"), 0, 4),
        (RT, "{broken", 2, 5),
    ])
    assert (report.read, report.invalid) == (5, 1)

    master = tmp_path / "master"
    assert (master / "_spark_metadata").is_dir()
    assert {p.name for p in master.iterdir() if p.name.startswith("platform=")} == {"platform=roblox",
                                                                                 "platform=steam"}
    assert (master / "platform=roblox" / "date=2026-11-07").is_dir()

    df = spark.read.parquet(str(master))
    types = dict(df.dtypes)
    assert (types["visits"], types["ts"], types["released"], types["genres"]) == (
        "bigint", "timestamp", "date", "array<string>")
    rows = {r["kind"]: r for r in df.selectExpr("*", "CAST(ts AS STRING) AS ts_utc").collect()}
    assert rows["game"]["visits"] == 2**40
    assert rows["game"]["ts_utc"] == "2026-11-06 13:01:00"
    assert rows["game"]["players"] is None  # absent field: null, never 0
    assert rows["players"]["players"] == 0  # real zero stays
    assert rows["players"]["visits"] is None
    assert str(rows["meta"]["released"]) == "2025-02-26"
    assert rows["votes"]["kafka_offset"] == 2


def test_second_run_writes_only_new_records(spark, tmp_path):
    first = _run(spark, tmp_path, [(RT, game(), 0, 1)])
    second = _run(spark, tmp_path, [(RT, votes(), 0, 2)])
    assert (first.read, second.read) == (1, 1)  # the checkpoint skips what was archived
    assert spark.read.parquet(str(tmp_path / "master")).count() == 2
