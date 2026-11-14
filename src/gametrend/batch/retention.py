"""Retention sweep of game_metrics. Pure logic.

Runs every cycle, independently of the hours being recomputed, over every hour older than the cutoff
(common.retention.retention_cutoff) that still has 5-minute documents. It deletes the 5-minute documents of
a (game, hour) only when that game has a game_metrics_hourly document for that hour; the others are kept
(and counted), so data is never lost when an hourly aggregate is missing.

No transaction is needed: the hourly document is committed before the sweep, and for the moment both
exist the history read (common `read_metrics`) picks one source per game and hour.
"""

from datetime import datetime

from ..common.metrics import _check_ts, hour_start
from .hours import HOUR


def hours_to_sweep(oldest_ts: datetime | None, cutoff: datetime) -> list[datetime]:
    """Hours from the hour of the oldest 5-minute document up to (not including) the cutoff hour."""
    cutoff = _check_ts(cutoff, "cutoff")
    if oldest_ts is None:
        return []
    hours = []
    h = hour_start(oldest_ts)
    while h < cutoff:
        hours.append(h)
        h += HOUR
    return hours


def split_ids(ids_5min, ids_hourly) -> tuple[list[int], list[int]]:
    """(ids whose 5-minute documents may be deleted, ids that keep them: no hourly document), both sorted."""
    with_hourly = set(ids_hourly)
    present = set(ids_5min)
    return sorted(present & with_hourly), sorted(present - with_hourly)
