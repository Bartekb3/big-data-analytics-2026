"""The foreachBatch wiring on a real (local) Spark DataFrame. Runs in the Spark image: pytest -m spark."""

import pytest

from gametrend.speed.job import make_handler
from gametrend.speed.writes import GAME_METRICS

from .helpers import RT, ST, FakeStore, game_json, steam_json, t

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("speed-test").getOrCreate()
    yield session
    session.stop()


def test_handler_on_a_spark_dataframe(spark):
    df = spark.createDataFrame(
        [(RT, game_json(1, t("13:01"), 10, 100)), (ST, steam_json(730, t("13:01"), 5)), (RT, "broken")],
        "topic string, value string",
    )
    store = FakeStore()
    lines = []

    class Log:
        def info(self, line):
            lines.append(line)

    make_handler(store, logger=Log())(df, 0)
    assert len(store.written[GAME_METRICS]) == 2
    assert "invalid=1" in lines[0]
