"""Kafka record -> master row, without Spark (architecture/contracts/parquet-master.md)."""

import json
from datetime import date, datetime, timezone

import pandas as pd

from gametrend.batch.archive import (COLUMNS, EVENT_COLUMNS, KAFKA_COLUMNS, SPARK_SCHEMA, map_frames, rows_frame,
                                     to_row)
from gametrend.common.kafka_contract import field_types

from .helpers import RT, ST, TS, game, roblox_meta, sort, steam_meta, steam_players, votes

T = datetime(2026, 11, 6, 13, 1, tzinfo=timezone.utc)


def test_columns_are_platform_contract_fields_kafka_metadata_and_date():
    assert COLUMNS[0] == "platform"
    assert set(field_types()) <= set(COLUMNS)
    assert COLUMNS[-3:] == ("kafka_partition", "kafka_offset", "date")
    assert len(COLUMNS) == len(set(COLUMNS))


def test_event_columns_leave_out_kafka_metadata_and_date():
    assert not set(KAFKA_COLUMNS) & set(EVENT_COLUMNS)
    assert "date" not in EVENT_COLUMNS
    assert {"platform", "kind", "id", "ts", "country", "sort"} <= set(EVENT_COLUMNS)


def test_spark_schema_uses_native_types():
    for part in ("`visits` bigint", "`ts` timestamp", "`updated` timestamp", "`released` date",
                 "`genres` array<string>", "`kafka_offset` bigint", "`date` date", "`platform` string"):
        assert part in SPARK_SCHEMA


def test_game_event_row():
    row = to_row(RT, game(visits=2**53 + 1), 2, 17)
    assert row == {"kind": "game", "ts": T, "id": 1, "playing": 10, "visits": 2**53 + 1, "favorites": 1,
                   "updated": datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc), "platform": "roblox",
                   "kafka_partition": 2, "kafka_offset": 17, "date": date(2026, 11, 6)}


def test_every_kind_keeps_only_its_fields():
    assert set(to_row(RT, votes(), 0, 0)) == {"kind", "ts", "id", "up", "down", "platform", *KAFKA_COLUMNS, "date"}
    s = to_row(RT, sort(), 0, 0)
    assert (s["country"], s["sort"], s["rank"], s["playing"]) == ("us", "top-trending", 1, 10)
    assert set(to_row(ST, steam_players(), 0, 0)) == {"kind", "ts", "id", "players", "platform", *KAFKA_COLUMNS,
                                                      "date"}


def test_meta_events_convert_dates_and_keep_absent_fields_absent():
    r = to_row(RT, roblox_meta(created="2020-01-02T03:04:05.000Z"), 0, 0)
    assert r["created"] == datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert "genre_l1" not in r and "description" not in r
    s = to_row(ST, steam_meta(released="2025-02-26", genres=["Action", "Indie"]), 0, 0)
    assert s["released"] == date(2025, 2, 26)
    assert s["genres"] == ["Action", "Indie"]
    assert s["platform"] == "steam"


def test_date_partition_is_the_utc_date_of_ts():
    assert to_row(RT, votes(ts="2026-11-06T23:59:59.999Z"), 0, 0)["date"] == date(2026, 11, 6)
    assert to_row(RT, votes(ts="2026-11-07T00:00:00.000Z"), 0, 0)["date"] == date(2026, 11, 7)


def test_invalid_records_give_none():
    assert to_row("other.topic", game(), 0, 0) is None
    assert to_row(RT, None, 0, 0) is None
    assert to_row(RT, "{broken", 0, 0) is None
    assert to_row(RT, json.dumps({**json.loads(game()), "extra": 1}), 0, 0) is None  # contract fails
    assert to_row(RT, steam_players(), 0, 0) is None  # Steam kind on the Roblox topic
    assert to_row(RT, game(ts="2026-02-30T12:00:00.000Z"), 0, 0) is None  # passes the regex, no real date
    assert to_row(ST, steam_meta(released="2025-02-30"), 0, 0) is None


def test_frame_has_every_column_and_fixed_dtypes():
    frame = rows_frame([to_row(RT, game(visits=2**53 + 1), 0, 1), to_row(ST, steam_players(players=0), 0, 2)])
    assert list(frame.columns) == list(COLUMNS)
    assert str(frame["visits"].dtype) == "Int64"
    assert str(frame["players"].dtype) == "Int64"
    assert str(frame["kafka_offset"].dtype) == "Int64"
    assert str(frame["ts"].dtype) == "datetime64[ns, UTC]"
    assert str(frame["updated"].dtype) == "datetime64[ns, UTC]"
    assert frame["visits"][0] == 2**53 + 1  # exact, not a float


def test_missing_stays_null_never_zero_or_nan():
    frame = rows_frame([to_row(RT, game(), 0, 1), to_row(ST, steam_players(players=0), 0, 2)])
    assert frame["players"][0] is pd.NA       # a Roblox game event has no `players`
    assert frame["players"][1] == 0           # a real zero stays zero
    assert frame["visits"][1] is pd.NA        # Steam has no visits: null, not 0 / NaN
    assert frame["updated"][1] is pd.NaT
    assert frame["country"][0] is None
    assert frame["released"][0] is None


def test_empty_frame_keeps_columns_and_dtypes():
    frame = rows_frame([])
    assert list(frame.columns) == list(COLUMNS)
    assert len(frame) == 0
    assert str(frame["visits"].dtype) == "Int64"
    assert str(frame["ts"].dtype) == "datetime64[ns, UTC]"


def test_map_frames_drops_and_counts_invalid_records():
    frames = [
        pd.DataFrame({"topic": [RT, RT, ST], "value": [game(), "{broken", steam_players()],
                      "kafka_partition": [0, 0, 0], "kafka_offset": [1, 2, 3]}),
        pd.DataFrame({"topic": [RT], "value": [None], "kafka_partition": [1], "kafka_offset": [4]}),
    ]
    counts = []
    out = list(map_frames(iter(frames), counts.append))
    assert counts == [1, 1]
    assert [len(f) for f in out] == [2, 0]
    assert list(out[0]["platform"]) == ["roblox", "steam"]
    assert list(out[0]["kafka_offset"]) == [1, 3]


def test_ts_unchanged_through_the_row():
    assert to_row(RT, votes(ts=TS), 0, 0)["ts"] == T
