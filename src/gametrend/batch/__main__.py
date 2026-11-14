"""Entry point of the batch service: python -m gametrend.batch (docker-compose.yml, profile spark)."""

import logging
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient

from .aggregate import SparkMaster
from .config import settings_from_env
from .loop import FailureStreak, SparkDead, hourly_step, run_cycle, run_forever
from .mongo import BatchStore
from .sink import run_sink

log = logging.getLogger("gametrend.batch")


def spark_alive(spark) -> bool:
    try:
        spark.range(1).count()
        return True
    except Exception:
        return False


def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = settings_from_env()
    from pyspark.sql import SparkSession  # only in the Spark image

    spark = SparkSession.builder.appName("gametrend-batch").getOrCreate()  # defaults: infra/spark/spark-defaults.conf
    client = MongoClient(settings.mongo_uri, tz_aware=True)
    store = BatchStore(client, client[settings.database])
    master = SparkMaster(spark, settings.master_dir)
    sink_streak, hourly_streak = FailureStreak("sink"), FailureStreak("hourly aggregates")
    log.info("batch starting: interval %ss, recompute %d h, retention %d h, maxOffsetsPerTrigger %d, master %s",
             settings.interval_s, settings.recompute_hours, settings.retention_hours,
             settings.max_offsets_per_trigger, settings.master_dir)

    def cycle():
        run_cycle(
            sink=lambda: run_sink(spark, settings),
            hourly=lambda: hourly_step(master, store, settings.recompute, settings.retention),
            sink_streak=sink_streak, hourly_streak=hourly_streak,
            clock=lambda: datetime.now(timezone.utc),  # failure log only, never processing logic
            spark_alive=lambda: spark_alive(spark),
        )

    try:
        run_forever(cycle, settings.interval_s, time.sleep)
    except SparkDead as exc:
        log.error("%s: exiting so docker compose restarts the service", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
