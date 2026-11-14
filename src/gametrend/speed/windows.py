"""Which `game_metrics` history speed reads per micro-batch (architecture/speed/README.md, Scoring).

Ranges are computed per game, then merged (all closed, touching ranges merged) into one query per platform.
Reading a bit too much is harmless: the exact choice (predecessor `ts < first ts`, growth +-15 min) is made
by the pure functions afterwards.
"""

from datetime import datetime, timedelta

from bson.int64 import Int64

from ..common.metrics import BUCKET, GROWTH_HORIZONS, GROWTH_TOLERANCE, MAX_VISIT_GAP, Observation

# observed_at lies in [ts, ts + 5 min), so a document's ts can be up to 5 min before its observed_at
_TS_LAG = BUCKET

HISTORY_PROJECTION = {"_id": 0, "platform": 1, "id": 1, "ts": 1, "observed_at": 1, "players": 1, "visits": 1}

Range = tuple[datetime, datetime]


def predecessor_range(first: Observation) -> Range:
    """`ts` range holding the predecessor of the first bucket for `new_visits` (at most 1 h older)."""
    return first.bucket - MAX_VISIT_GAP - _TS_LAG, first.bucket


def growth_range(first: Observation, last: Observation, horizon: timedelta) -> Range:
    """`ts` range holding every growth reference of the buckets first..last for one horizon."""
    return (first.observed_at - horizon - GROWTH_TOLERANCE - _TS_LAG,
            last.observed_at - horizon + GROWTH_TOLERANCE)


def game_ranges(buckets: list[Observation]) -> list[Range]:
    """History ranges of one game; `buckets` are its micro-batch buckets in time order (latest_per_bucket)."""
    if not buckets:
        return []
    first, last = buckets[0], buckets[-1]
    ranges = [growth_range(first, last, h) for h in GROWTH_HORIZONS.values()]
    if any(b.visits is not None for b in buckets):
        ranges.append(predecessor_range(first))
    return ranges


def merge_ranges(ranges) -> list[Range]:
    """Union of closed ranges, sorted; overlapping and touching ranges become one."""
    merged: list[list[datetime]] = []
    for start, end in sorted(ranges):
        if start > end:
            raise ValueError(f"empty range {start} > {end}")
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def history_filter(platform: str, ids, ranges) -> dict | None:
    """MongoDB filter for the history of `ids` within the merged `ranges`; None when nothing is needed."""
    merged = merge_ranges(ranges)
    ids = sorted(set(ids))
    if not merged or not ids:
        return None
    return {
        "platform": platform,
        "id": {"$in": [Int64(i) for i in ids]},
        "$or": [{"ts": {"$gte": s, "$lte": e}} for s, e in merged],
    }
