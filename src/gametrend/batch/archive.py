"""Kafka record -> one flat row of the Parquet master dataset (architecture/contracts/parquet-master.md).

Pure logic, no Spark: the sink (sink.py) calls `map_frames` inside `mapInPandas`. Validation is the shared
contract check (gametrend.common.kafka_contract); the columns are derived from it, so the layout is not a
second copy of the contract.

Layout: one wide dataset for both topics. Columns: `platform`, every contract field (names as in Kafka,
absent fields null), the Kafka metadata, and `date` (UTC date of `ts`, partition column with `platform`).
"""

import json

from ..common.kafka_contract import PLATFORM_OF_TOPIC, field_types, is_valid, parse_date, parse_utc

FIELD_TYPES = field_types()  # contract field -> type tag

KAFKA_COLUMNS = ("kafka_partition", "kafka_offset")  # metadata, never part of the event identity
PARTITION_COLUMNS = ("platform", "date")

# column -> type tag; `platform` and `date` are derived, the Kafka metadata is an integer
COLUMN_TYPES = {"platform": "str", **FIELD_TYPES, **{c: "int" for c in KAFKA_COLUMNS}, "date": "date"}
COLUMNS = tuple(COLUMN_TYPES)

# The columns that identify an event's content: exact copies on these are duplicates (replays,
# restarts). The Kafka metadata differs between copies, so it is left out; `date` follows from `ts`.
EVENT_COLUMNS = tuple(c for c in COLUMNS if c not in KAFKA_COLUMNS and c != "date")

_SPARK_TYPE = {"int": "bigint", "str": "string", "iso": "timestamp", "date": "date", "str_list": "array<string>"}
_PANDAS_DTYPE = {"int": "Int64", "str": object, "iso": "datetime64[ns, UTC]", "date": object, "str_list": object}

SPARK_SCHEMA = ", ".join(f"`{c}` {_SPARK_TYPE[t]}" for c, t in COLUMN_TYPES.items())


def _convert(tag: str, value):
    if tag == "iso":
        return parse_utc(value)
    if tag == "date":
        return parse_date(value).date()
    if tag == "str_list":
        return list(value)
    return value


def to_row(topic: str, value, partition: int, offset: int) -> dict | None:
    """One Kafka record -> a flat row (only the event's present fields), or None if it fails the contract."""
    platform = PLATFORM_OF_TOPIC.get(topic)
    if platform is None or value is None:
        return None
    try:
        event = json.loads(value)
    except (ValueError, TypeError):
        return None
    if not is_valid(platform, event):
        return None
    try:
        row = {name: _convert(FIELD_TYPES[name], v) for name, v in event.items()}
    except ValueError:  # passes the regex but is no real date, e.g. 2026-02-30
        return None
    row["platform"] = platform
    row["kafka_partition"] = int(partition)
    row["kafka_offset"] = int(offset)
    row["date"] = row["ts"].date()
    return row


def rows_frame(rows: list[dict]):
    """Rows -> pandas DataFrame with every column in COLUMNS and fixed dtypes.

    Integers are nullable `Int64` (never float, so a counter above 2^53 stays exact and a missing value
    stays null, never 0 or NaN); timestamps UTC; strings, dates and lists as objects with None.
    """
    import pandas as pd

    data = {}
    for column, tag in COLUMN_TYPES.items():
        values = [row.get(column) for row in rows]
        if tag == "iso":
            data[column] = pd.to_datetime(pd.Series(values, dtype=object), utc=True)
        else:
            data[column] = pd.Series(values, dtype=_PANDAS_DTYPE[tag])
    return pd.DataFrame(data, columns=list(COLUMNS))


def map_frames(frames, count_invalid):
    """The `mapInPandas` function: frames with `topic`, `value`, `kafka_partition`, `kafka_offset` ->
    frames of master rows. Invalid records are dropped and reported through `count_invalid(n)`."""
    for frame in frames:
        rows, invalid = [], 0
        for topic, value, partition, offset in zip(frame["topic"], frame["value"],
                                                   frame["kafka_partition"], frame["kafka_offset"]):
            row = to_row(topic, value, partition, offset)
            if row is None:
                invalid += 1
            else:
                rows.append(row)
        count_invalid(invalid)
        yield rows_frame(rows)
