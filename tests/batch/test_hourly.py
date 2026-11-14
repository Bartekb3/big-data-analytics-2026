"""Hourly aggregates of one game, without Spark (game_metrics_hourly, metric definitions)."""

import random
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from gametrend.batch.hourly import (OUTPUT_COLUMNS, aggregate_frame, aggregate_game, frame_events, group_by_hour,
                                    to_document, to_int)
from gametrend.batch.hours import to_micros

H = datetime(2026, 11, 6, 13, tzinfo=timezone.utc)


def t(hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(2026, 11, 6, hh, mm, ss, tzinfo=timezone.utc)


UPDATED = datetime(2026, 11, 1, 8, tzinfo=timezone.utc)


def game(at, playing, visits, updated=UPDATED):
    return {"kind": "game", "ts": at, "playing": playing, "players": None, "visits": visits, "updated": updated,
            "up": None, "down": None}


def steam(at, players):
    return {"kind": "players", "ts": at, "playing": None, "players": players, "visits": None, "updated": None,
            "up": None, "down": None}


def votes(at, up, down):
    return {"kind": "votes", "ts": at, "playing": None, "players": None, "visits": None, "updated": None,
            "up": up, "down": down}


ROBLOX = [
    game(t(12, 58), 5, 1000),               # previous hour: predecessor only
    game(t(13, 1), 10, 1010),
    game(t(13, 6), 30, 1030),
    game(t(13, 7), 20, 1040),               # same bucket as 13:06, later: wins
    game(t(13, 56), 15, 1100, updated=datetime(2026, 11, 6, 13, 30, tzinfo=timezone.utc)),
    votes(t(13, 2), 50, 5),
    votes(t(13, 52), 60, 6),
]


def test_roblox_hour():
    assert aggregate_game(ROBLOX, [H]) == [{
        "hour_ts": H, "players": 15, "peak_players": 30, "new_visits": 100,  # 30 at 13:06, replaced in its bucket
        "updated": datetime(2026, 11, 6, 13, 30, tzinfo=timezone.utc), "up": 60, "down": 6,
    }]


def test_previous_hour_values_are_not_returned():
    assert [d["hour_ts"] for d in aggregate_game(ROBLOX, [H])] == [H]


def test_peak_in_the_middle_of_a_bucket_counts():
    events = [game(t(13, 0), 10, 1000), game(t(13, 1), 80, 1001), game(t(13, 2), 12, 1002),
              game(t(13, 3), 11, 1003), game(t(13, 4), 10, 1004)]  # one bucket, peak not its last observation
    doc = aggregate_game(events, [H])[0]
    assert doc["peak_players"] == 80
    assert doc["players"] == 10  # last bucket's (last) observation


def test_peak_ignores_the_predecessor_hour():
    doc = aggregate_game([game(t(12, 58), 500, 1000), game(t(13, 1), 10, 1010)], [H])[0]
    assert doc["peak_players"] == 10


def test_steam_hour_has_players_only():
    assert aggregate_game([steam(t(13, 0), 7), steam(t(13, 15), 0), steam(t(13, 30), 3)], [H]) == [
        {"hour_ts": H, "players": 3, "peak_players": 7}]


def test_zero_players_is_kept():
    assert aggregate_game([steam(t(13, 15), 0)], [H]) == [{"hour_ts": H, "players": 0, "peak_players": 0}]


def test_new_visits_absent_without_a_valid_delta():
    doc = aggregate_game([game(t(13, 1), 10, 1000)], [H])[0]  # no predecessor
    assert "new_visits" not in doc
    reset = aggregate_game([game(t(12, 58), 5, 1000), game(t(13, 1), 10, 900)], [H])[0]
    assert "new_visits" not in reset


def test_zero_delta_gives_zero_new_visits():
    assert aggregate_game([game(t(12, 58), 5, 1000), game(t(13, 1), 10, 1000)], [H])[0]["new_visits"] == 0


def test_predecessor_more_than_an_hour_older_gives_no_delta():
    doc = aggregate_game([game(t(12, 0), 5, 1000), game(t(13, 0, 1), 10, 1010)], [H])[0]
    assert "new_visits" not in doc


def test_several_hours_equal_one_hour_at_a_time():
    events = ROBLOX + [game(t(14, 3), 9, 1150), game(t(14, 40), 8, 1160)]
    both = aggregate_game(events, [H, t(14, 0)])
    one_by_one = aggregate_game(events, [H]) + aggregate_game(events, [t(14, 0)])
    assert both == one_by_one
    assert both[1]["new_visits"] == 60


def test_exact_duplicates_change_nothing():
    assert aggregate_game(ROBLOX + ROBLOX, [H]) == aggregate_game(ROBLOX, [H])


def test_same_key_with_different_values_is_order_independent():
    events = ROBLOX + [game(t(13, 56), 18, 1100), votes(t(13, 52), 61, 6),
                       game(t(13, 56), 15, 1100, updated=datetime(2026, 11, 6, 13, 40, tzinfo=timezone.utc))]
    expected = aggregate_game(events, [H])
    for seed in range(20):
        shuffled = list(events)
        random.Random(seed).shuffle(shuffled)
        assert aggregate_game(shuffled, [H]) == expected
    assert expected[0]["players"] == 18          # equal observed_at: higher players wins
    assert expected[0]["up"] == 61               # equal ts: higher votes win
    assert expected[0]["updated"] == datetime(2026, 11, 6, 13, 40, tzinfo=timezone.utc)


def test_votes_only_hour():
    assert aggregate_game([votes(t(13, 5), 1, 2)], [H]) == [{"hour_ts": H, "up": 1, "down": 2}]


def test_hour_without_metric_fields_gives_no_document():
    assert aggregate_game([], [H]) == []
    assert aggregate_game([game(t(12, 58), 5, 1000)], [H]) == []  # only the predecessor hour


def test_to_int():
    assert to_int(np.float64(5.0)) == 5
    assert to_int(np.int64(7)) == 7
    assert to_int(float("nan")) is None
    assert to_int(pd.NA) is None
    assert to_int(None) is None
    with pytest.raises(ValueError):
        to_int(1.5)
    with pytest.raises(ValueError):
        to_int(float(2**60))  # not exact as a float


def _frame(rows):
    return pd.DataFrame(rows, columns=["platform", "id", "kind", "ts_us", "playing", "players", "visits",
                                       "updated_us", "up", "down"])


def test_frame_with_float_nulls_like_spark_gives_the_same_result():
    nan = float("nan")
    frame = _frame([
        ["roblox", 1.0, "game", to_micros(t(12, 58)), 5.0, nan, 1000.0, to_micros(UPDATED), nan, nan],
        ["roblox", 1.0, "game", to_micros(t(13, 1)), 10.0, nan, 1010.0, to_micros(UPDATED), nan, nan],
        ["roblox", 1.0, "votes", to_micros(t(13, 2)), nan, nan, nan, nan, 50.0, 5.0],
    ])
    events = frame_events(frame)
    assert events[0]["players"] is None and events[0]["ts"] == t(12, 58)
    assert events[2]["visits"] is None and events[2]["updated"] is None
    out = aggregate_frame(frame, [H])
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert len(out) == 1
    row = out.iloc[0]
    assert (row["players"], row["new_visits"], row["up"]) == (10, 10, 50)
    assert str(out["players"].dtype) == "Int64"


def test_aggregate_frame_without_documents_keeps_the_schema():
    nan = float("nan")
    out = aggregate_frame(_frame([["steam", 730, "players", to_micros(t(12, 0)), nan, 5, nan, nan, nan, nan]]), [H])
    assert len(out) == 0 and list(out.columns) == list(OUTPUT_COLUMNS)


def test_to_document_drops_nulls():
    doc = to_document({"platform": "steam", "id": 730, "hour_us": to_micros(H), "players": 3, "peak_players": 7,
                       "new_visits": None, "updated_us": None, "up": None, "down": None})
    assert doc == {"platform": "steam", "id": 730, "hour_ts": H, "players": 3, "peak_players": 7}


def test_group_by_hour():
    a, b = {"hour_ts": H, "id": 1}, {"hour_ts": t(14, 0), "id": 1}
    assert group_by_hour([a, b, {"hour_ts": H, "id": 2}]) == {H: [a, {"hour_ts": H, "id": 2}], t(14, 0): [b]}
