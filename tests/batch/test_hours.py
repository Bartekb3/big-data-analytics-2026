from datetime import date, datetime, timedelta, timezone

import pytest

from gametrend.batch.hours import (chunks, data_now, from_micros, hours_to_compute, partition_dates, to_micros)


def t(day: int, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, ss, tzinfo=timezone.utc)


K = timedelta(hours=2)


def test_micros_round_trip():
    ts = datetime(2026, 11, 6, 13, 1, 2, 345678, tzinfo=timezone.utc)
    assert from_micros(to_micros(ts)) == ts
    assert to_micros(datetime(1970, 1, 1, tzinfo=timezone.utc)) == 0


def test_to_micros_rejects_naive():
    with pytest.raises(ValueError):
        to_micros(datetime(2026, 11, 6))


def test_partition_dates_ignores_other_entries_and_sorts():
    tree = {"platform=roblox": ["date=2026-11-07", "date=2026-11-06", "part-0.parquet"],
            "platform=steam": [], "_spark_metadata": ["0"]}
    assert partition_dates(tree) == {"roblox": [date(2026, 11, 6), date(2026, 11, 7)]}


def test_data_now_is_the_latest_over_platforms():
    assert data_now({"roblox": t(6, 13, 4), "steam": t(6, 13, 0)}) == t(6, 13, 4)
    assert data_now({"roblox": None, "steam": t(6, 9)}) == t(6, 9)
    assert data_now({}) is None
    assert data_now({"roblox": None}) is None


def test_empty_collection_starts_at_the_first_event_hour():
    assert hours_to_compute(t(6, 10, 30), None, t(6, 13, 5), K) == [t(6, 10), t(6, 11), t(6, 12)]


def test_open_hour_is_not_computed():
    assert hours_to_compute(t(6, 13, 0), None, t(6, 13, 59, 59), K) == []
    assert hours_to_compute(t(6, 13, 0), None, t(6, 14, 0), K) == [t(6, 13)]  # closed exactly at 14:00


def test_recompute_window_behind_the_latest_aggregate():
    assert hours_to_compute(None, t(6, 12), t(6, 14, 10), K) == [t(6, 10), t(6, 11), t(6, 12), t(6, 13)]
    assert hours_to_compute(None, t(6, 12), t(6, 14, 10), timedelta(0)) == [t(6, 12), t(6, 13)]


def test_recompute_window_does_not_start_before_the_archive():
    assert hours_to_compute(t(6, 11, 30), t(6, 12), t(6, 13, 10), K) == [t(6, 11), t(6, 12)]


def test_a_long_pause_leaves_no_gap():
    hours = hours_to_compute(None, t(6, 12), t(7, 12, 1), K)
    assert hours[0] == t(6, 10) and hours[-1] == t(7, 11) and len(hours) == 26


def test_nothing_when_no_data():
    assert hours_to_compute(None, None, t(6, 13), K) == []


def test_negative_recompute_rejected():
    with pytest.raises(ValueError):
        hours_to_compute(None, t(6, 12), t(6, 14), timedelta(hours=-1))


def test_chunks():
    hours = [t(6, h) for h in range(5)]
    assert chunks(hours, 2) == [hours[:2], hours[2:4], hours[4:]]
    assert chunks([], 3) == []
    with pytest.raises(ValueError):
        chunks(hours, 0)
