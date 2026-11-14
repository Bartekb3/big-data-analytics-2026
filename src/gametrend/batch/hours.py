"""Which hours to compute, and "now" of the master dataset. Pure logic.

"Now" is data time: the latest event `ts` in Parquet (architecture/contracts/conventions.md#time).
Range of a run: closed hours from (latest hour in game_metrics_hourly - K) up to the open hour of "now",
derived from MongoDB itself, without a separate state file (architecture/batch/README.md).
"""

from datetime import date, datetime, timedelta, timezone

from ..common.metrics import _check_ts, hour_start

HOUR = timedelta(hours=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def from_micros(us: int) -> datetime:
    """Epoch microseconds (Spark `unix_micros`) -> aware UTC datetime."""
    return _EPOCH + timedelta(microseconds=int(us))


def to_micros(ts: datetime) -> int:
    return (_check_ts(ts) - _EPOCH) // timedelta(microseconds=1)


def partition_dates(tree: dict[str, list[str]]) -> dict[str, list[date]]:
    """Directory names of the master dataset -> {platform: its dates, ascending}.

    `tree`: {"platform=roblox": ["date=2026-11-06", ...], ...}. Other names (`_spark_metadata`, files)
    are ignored. A directory can hold only uncommitted files, so callers treat a date as a candidate.
    """
    out: dict[str, list[date]] = {}
    for platform_dir, date_dirs in tree.items():
        if not platform_dir.startswith("platform="):
            continue
        dates = sorted(date.fromisoformat(d.split("=", 1)[1]) for d in date_dirs if d.startswith("date="))
        if dates:
            out[platform_dir.split("=", 1)[1]] = dates
    return out


def data_now(latest_by_platform: dict[str, datetime | None]) -> datetime | None:
    """The latest event `ts` over the platforms; None when the master dataset has no event yet."""
    values = [_check_ts(v) for v in latest_by_platform.values() if v is not None]
    return max(values) if values else None


def hours_to_compute(first_ts: datetime | None, last_done: datetime | None, now: datetime,
                     recompute: timedelta) -> list[datetime]:
    """Closed hours to (re)compute, ascending.

    `last_done`: the latest `hour_ts` in game_metrics_hourly (None when empty: start at the hour of
    `first_ts`, the earliest event in Parquet). From `last_done - recompute` on, so late events of the
    last `recompute` hours are taken into account. An hour is closed when it ends at or before the
    hour of `now`.
    """
    if recompute < timedelta(0):
        raise ValueError(f"recompute must be >= 0, got {recompute}")
    end = hour_start(now)
    if last_done is None:
        if first_ts is None:
            return []
        start = hour_start(first_ts)
    else:
        start = hour_start(_check_ts(last_done) - recompute)
        if first_ts is not None:
            start = max(start, hour_start(first_ts))
    hours = []
    h = start
    while h < end:
        hours.append(h)
        h += HOUR
    return hours


def chunks(hours: list[datetime], size: int) -> list[list[datetime]]:
    """Consecutive groups of at most `size` hours (one Spark job each)."""
    if size < 1:
        raise ValueError(f"size must be >= 1, got {size}")
    return [hours[i:i + size] for i in range(0, len(hours), size)]
