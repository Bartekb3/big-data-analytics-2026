"""The speed streaming job: Kafka (both topics, one query, one checkpoint) -> foreachBatch -> MongoDB.

Thin I/O layer; the logic is in parse, windows, scoring, writes and microbatch.
Design: architecture/speed/README.md. Failure policy: any error other than a rejected (older) observation
raises, the query stops, the container restarts (docker-compose.yml) and Spark repeats the micro-batch
from the checkpoint; the guarded upserts make that repeat safe.
"""

import logging
import sys

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from ..common.kafka_contract import TOPIC
from .config import Settings, settings_from_env
from .microbatch import format_report, process_batch
from .windows import HISTORY_PROJECTION
from .writes import GAME_METRICS, WriteFailed, summarize

log = logging.getLogger(__name__)


class MongoStore:
    def __init__(self, db):
        self.db = db

    def read_history(self, flt: dict) -> list[dict]:
        return list(self.db[GAME_METRICS].find(flt, HISTORY_PROJECTION))

    def write(self, collection: str, ops: list) -> dict:
        if not ops:
            return summarize({})
        try:
            result = self.db[collection].bulk_write(ops, ordered=False)
        except BulkWriteError as exc:
            try:
                return summarize(exc.details)
            except WriteFailed as failed:
                raise failed from exc
        return summarize(result.bulk_api_result)


def make_handler(store, process=process_batch, logger=log):
    """The foreachBatch function: collect the micro-batch on the driver and process it."""

    def handle(df, batch_id):
        rows = df.select("topic", "value").collect()
        report = process(batch_id, [(r["topic"], r["value"]) for r in rows], store)
        logger.info(format_report(report))

    return handle


def start_query(spark, settings: Settings, handler):
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", ",".join(sorted(TOPIC.values())))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
        .selectExpr("topic", "CAST(value AS STRING) AS value")
    )
    return (
        source.writeStream.foreachBatch(handler)
        .option("checkpointLocation", settings.checkpoint_dir)
        .queryName("speed")
        .start()
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = settings_from_env()
    from pyspark.sql import SparkSession  # only in the Spark image

    client = MongoClient(settings.mongo_uri, tz_aware=True)
    store = MongoStore(client[settings.database])
    spark = SparkSession.builder.appName("gametrend-speed").getOrCreate()
    log.info("speed starting: topics %s, maxOffsetsPerTrigger %d, checkpoint %s",
             ",".join(sorted(TOPIC.values())), settings.max_offsets_per_trigger, settings.checkpoint_dir)
    log.warning("failOnDataLoss=false: if Kafka deleted events speed had not read yet (an outage longer than "
                "the topic retention), Spark logs a data-loss warning and continues from the oldest offset")
    query = start_query(spark, settings, make_handler(store))
    query.awaitTermination()  # raises when a micro-batch fails: the process exits non-zero
    return 0
