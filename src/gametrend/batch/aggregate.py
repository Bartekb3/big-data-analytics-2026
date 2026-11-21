"""Spark side of the hourly aggregates: read the master dataset, find "now", compute hours per game.

Thin I/O; the per-game logic is hourly.py, the hour range is hours.py. Reads MASTER_DIR through the file
sink's `_spark_metadata` log (spark.read.parquet on the sink path), so only committed files are seen.
Timestamps cross the pandas boundary as epoch microseconds, so no session or local time zone is involved.
"""

import os
from datetime import datetime

from .archive import EVENT_COLUMNS
from .hourly import INPUT_COLUMNS, INPUT_KINDS, OUTPUT_SCHEMA, aggregate_frame, to_document
from .hours import HOUR, from_micros, partition_dates, to_micros


def has_data(master_dir: str) -> bool:
    return os.path.isdir(os.path.join(master_dir, "_spark_metadata"))


def list_partitions(master_dir: str) -> dict[str, list]:
    tree = {}
    for name in os.listdir(master_dir):
        path = os.path.join(master_dir, name)
        if os.path.isdir(path):
            tree[name] = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    return partition_dates(tree)


def read_master(spark, master_dir: str):
    return spark.read.parquet(master_dir)


def _extreme_ts(master, platform: str, dates: list, latest: bool) -> datetime | None:
    """max (latest=True) or min `ts` of a platform, reading one date partition at a time from that end;
    a date directory holding only uncommitted files gives nothing and the next date is tried."""
    from pyspark.sql import functions as F

    agg = F.max if latest else F.min
    for d in sorted(dates, reverse=latest):
        value = (master.where((F.col("platform") == platform) & (F.col("date") == F.lit(d)))
                 .agg(agg(F.expr("unix_micros(ts)"))).first()[0])
        if value is not None:
            return from_micros(value)
    return None


def latest_ts(master, partitions: dict) -> dict[str, datetime | None]:
    return {p: _extreme_ts(master, p, dates, latest=True) for p, dates in partitions.items()}


def earliest_ts(master, partitions: dict) -> datetime | None:
    values = [v for p, dates in partitions.items() if (v := _extreme_ts(master, p, dates, latest=False))]
    return min(values) if values else None


class SparkMaster:
    """The master dataset as the hourly step sees it (loop.py); re-read on every call so new files count."""

    def __init__(self, spark, master_dir: str):
        self.spark = spark
        self.master_dir = master_dir

    def _read(self):
        if not has_data(self.master_dir):
            return None, {}
        return read_master(self.spark, self.master_dir), list_partitions(self.master_dir)

    def latest_ts(self) -> dict[str, datetime | None]:
        master, partitions = self._read()
        return {} if master is None else latest_ts(master, partitions)

    def earliest_ts(self) -> datetime | None:
        master, partitions = self._read()
        return None if master is None else earliest_ts(master, partitions)

    def compute(self, hours: list[datetime]) -> list[dict]:
        master, _ = self._read()
        return [] if master is None else compute_hours(master, hours)


def compute_hours(master, hours: list[datetime]) -> list[dict]:
    """game_metrics_hourly documents of `hours` (consecutive), from the events of `hours` plus the hour before.

    Exact copies (same values in every event column, Kafka metadata left out) are dropped first; the
    same key with different values is left to the deterministic rules in hourly.py.
    """
    from pyspark.sql import functions as F

    start, end = hours[0] - HOUR, hours[-1] + HOUR
    us = F.expr("unix_micros(ts)")
    window = (
        master.where(F.col("date").between(F.lit(start.date()), F.lit(end.date())))
        .where((us >= F.lit(to_micros(start))) & (us < F.lit(to_micros(end))))
        .where(F.col("kind").isin(*INPUT_KINDS))
        .select(*EVENT_COLUMNS)
        .dropDuplicates()
        .withColumn("ts_us", us)
        .withColumn("updated_us", F.expr("unix_micros(updated)"))
        .select(*INPUT_COLUMNS)
    )
    rows = (window.groupBy("platform", "id")
            .applyInPandas(lambda frame: aggregate_frame(frame, hours), schema=OUTPUT_SCHEMA)
            .collect())
    return [to_document(r.asDict()) for r in rows]
