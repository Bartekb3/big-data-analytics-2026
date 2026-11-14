from datetime import datetime, timedelta, timezone

import pytest

from gametrend.common.retention import GAME_METRICS_RETENTION, MIN_RETENTION, retention_cutoff

UTC = timezone.utc


def test_cutoff_on_full_hour():
    assert retention_cutoff(datetime(2026, 11, 16, 10, tzinfo=UTC)) == datetime(2026, 11, 2, 10, tzinfo=UTC)


def test_cutoff_mid_hour_floors():
    assert retention_cutoff(datetime(2026, 11, 16, 10, 59, 59, tzinfo=UTC)) == datetime(2026, 11, 2, 10, tzinfo=UTC)


def test_cutoff_with_other_retention():
    now = datetime(2026, 11, 4, 10, 30, tzinfo=UTC)
    assert retention_cutoff(now, timedelta(hours=48)) == datetime(2026, 11, 2, 10, tzinfo=UTC)


def test_cutoff_rejects_naive_now():
    with pytest.raises(ValueError):
        retention_cutoff(datetime(2026, 11, 16, 10))


def test_minimum_retention():
    now = datetime(2026, 11, 16, 10, tzinfo=UTC)
    assert retention_cutoff(now, MIN_RETENTION) == datetime(2026, 11, 15, 9, tzinfo=UTC)
    with pytest.raises(ValueError):
        retention_cutoff(now, MIN_RETENTION - timedelta(seconds=1))


def test_default_is_fourteen_days():
    assert GAME_METRICS_RETENTION == timedelta(days=14)
