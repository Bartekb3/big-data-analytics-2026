"""Retention cutoff of game_metrics, shared by batch retention and history reads.

Retention value: architecture/contracts/mongodb-collections.md (game_metrics).
Where "now" comes from: architecture/contracts/conventions.md#time.
"""

from datetime import datetime, timedelta

from gametrend.common.metrics import _check_ts, hour_start

GAME_METRICS_RETENTION = timedelta(days=14)
# speed reads game_metrics back to 24 h + 20 min for growth_24h; less retention would break it
MIN_RETENTION = timedelta(hours=25)


def retention_cutoff(now: datetime, retention: timedelta = GAME_METRICS_RETENTION) -> datetime:
    """Start of the UTC hour of `now - retention`. `now` is passed in, never the wall clock."""
    now = _check_ts(now, "now")
    if not isinstance(retention, timedelta):
        raise ValueError(f"retention must be a timedelta, got {retention!r}")
    if retention < MIN_RETENTION:
        raise ValueError(f"retention {retention} is below the minimum {MIN_RETENTION} (growth_24h history)")
    return hour_start(now - retention)
