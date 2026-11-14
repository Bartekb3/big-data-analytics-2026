"""Kafka -> Parquet master dataset: Structured Streaming with the availableNow trigger.

Thin I/O layer; the record -> row logic is in archive.py. Reads both topics from its own checkpoint (never
shared with speed), drops records that fail the contract (counted), writes one file per (platform, date) per
micro-batch (coalesce(1)) through the Parquet file sink, whose `_spark_metadata` log lets readers of
MASTER_DIR see only committed files. Design: architecture/batch/README.md.
"""

from dataclasses import dataclass

from ..common.kafka_contract import TOPIC
from .archive import PARTITION_COLUMNS, SPARK_SCHEMA, map_frames
from .config import Settings


@dataclass(frozen=True)
class SinkReport:
    batches: int
    read: int     # Kafka records read (from the query progress)
    invalid: int  # records dropped by the contract check

    def format(self) -> str:
        return f"sink: batches={self.batches} read={self.read} invalid={self.invalid} written={self.read - self.invalid}"


def read_kafka(spark, settings: Settings):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", ",".join(sorted(TOPIC.values())))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
        .selectExpr("topic", "CAST(value AS STRING) AS value",
                    "CAST(partition AS BIGINT) AS kafka_partition", "offset AS kafka_offset")
    )


def to_master_rows(records, count_invalid):
    """Kafka records (`topic`, `value`, `kafka_partition`, `kafka_offset`) -> master rows (SPARK_SCHEMA)."""
    return records.mapInPandas(lambda frames: map_frames(frames, count_invalid), schema=SPARK_SCHEMA)


def start_sink(rows, settings: Settings):
    return (
        rows.coalesce(1)
        .writeStream.format("parquet")
        .option("path", settings.master_dir)
        .option("checkpointLocation", settings.checkpoint_dir)
        .option("compression", "snappy")
        .partitionBy(*PARTITION_COLUMNS)
        .trigger(availableNow=True)
        .queryName("archive")
        .start()
    )


def run_sink(spark, settings: Settings, source=None) -> SinkReport:
    """Archive everything available in Kafka now, then return. `source` replaces Kafka in tests."""
    invalid = spark.sparkContext.accumulator(0)
    records = read_kafka(spark, settings) if source is None else source
    query = start_sink(to_master_rows(records, invalid.add), settings)
    query.awaitTermination()  # availableNow: returns when everything available at start is written
    progress = query.recentProgress
    return SinkReport(batches=len(progress), read=sum(p["numInputRows"] for p in progress), invalid=invalid.value)
