from datetime import datetime, timedelta, timezone

import pytest

from gametrend.batch.retention import hours_to_sweep, split_ids
from gametrend.common.retention import retention_cutoff


def t(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, tzinfo=timezone.utc)


def test_hours_up_to_the_cutoff_hour_excluded():
    assert hours_to_sweep(t(6, 10, 25), t(6, 13)) == [t(6, 10), t(6, 11), t(6, 12)]


def test_hour_of_the_cutoff_is_not_swept():
    assert hours_to_sweep(t(6, 13, 0), t(6, 13)) == []
    assert hours_to_sweep(t(6, 12, 59), t(6, 13)) == [t(6, 12)]


def test_nothing_older_than_the_cutoff():
    assert hours_to_sweep(None, t(6, 13)) == []


def test_naive_cutoff_rejected():
    with pytest.raises(ValueError):
        hours_to_sweep(t(6, 10), datetime(2026, 11, 6, 13))


def test_cutoff_from_data_now_with_demo_retention():
    cutoff = retention_cutoff(t(8, 13, 40), timedelta(hours=48))
    assert cutoff == t(6, 13)
    assert hours_to_sweep(t(6, 11, 5), cutoff) == [t(6, 11), t(6, 12)]


def test_split_ids_keeps_games_without_an_hourly_document():
    assert split_ids([3, 1, 2, 2], [1, 2, 9]) == ([1, 2], [3])
    assert split_ids([1], []) == ([], [1])
    assert split_ids([], [1]) == ([], [])
