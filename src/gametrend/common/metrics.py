"""Metric functions shared by speed and batch: 5-minute buckets, new_visits, growth.

Semantics: architecture/contracts/mongodb-collections.md#metric-definitions.
Design and rules: architecture/common/README.md.

Pure functions on plain Python values. Input from pandas/numpy goes through
`gametrend.common.normalize` first; anything else (NaN, pd.NA, numpy ints,
naive datetimes) raises ValueError instead of being computed on.
"""

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# Values fixed by the contract (mongodb-collections.md): game_metrics.ts and Metric definitions.
BUCKET = timedelta(minutes=5)
MAX_VISIT_GAP = timedelta(hours=1)
GROWTH_TOLERANCE = timedelta(minutes=15)
GROWTH_HORIZONS = {
    "growth_1h": timedelta(hours=1),
    "growth_3h": timedelta(hours=3),
    "growth_24h": timedelta(hours=24),
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_HOUR = timedelta(hours=1)


def _check_ts(ts, name: str = "ts") -> datetime:
    if not isinstance(ts, datetime):
        raise ValueError(f"{name} must be a datetime, got {ts!r}")
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC), got naive {ts!r}")
    return ts.astimezone(timezone.utc)


def _check_count(value, name: str):
    """int or None; NaN, pd.NA, floats, numpy ints and bools are rejected."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{name} must be int or None, got {value!r} ({type(value).__name__}); "
            "convert pandas/numpy values with gametrend.common.normalize.to_python"
        )
    return value


def _floor(ts: datetime, step: timedelta) -> datetime:
    ts = _check_ts(ts)
    return ts - (ts - _EPOCH) % step


def bucket_start(ts: datetime) -> datetime:
    """Start of the 5-minute UTC bucket containing `ts`."""
    return _floor(ts, BUCKET)


def hour_start(ts: datetime) -> datetime:
    """Start of the UTC hour containing `ts`."""
    return _floor(ts, _HOUR)


@dataclass(frozen=True)
class Observation:
    """One player/visit observation of one game. Missing values are None."""

    observed_at: datetime
    players: int | None = None
    visits: int | None = None

    def __post_init__(self):
        object.__setattr__(self, "observed_at", _check_ts(self.observed_at, "observed_at"))
        _check_count(self.players, "players")
        _check_count(self.visits, "visits")

    @property
    def bucket(self) -> datetime:
        return bucket_start(self.observed_at)


def _none_lowest(value):
    return (0, 0) if value is None else (1, value)


def _bucket_rank(obs: Observation):
    # Latest observed_at wins. Equal observed_at: decided by the values, never by input order,
    # so batch gets the same result however Spark orders the rows.
    return (obs.observed_at, _none_lowest(obs.players), _none_lowest(obs.visits))


def latest_per_bucket(observations) -> list[Observation]:
    """One observation per 5-minute bucket (the latest `observed_at`), sorted by time."""
    best: dict[datetime, Observation] = {}
    for obs in observations:
        if not isinstance(obs, Observation):
            raise ValueError(f"expected Observation, got {obs!r}")
        current = best.get(obs.bucket)
        if current is None or _bucket_rank(obs) > _bucket_rank(current):
            best[obs.bucket] = obs
    return [best[b] for b in sorted(best)]


def last_with_visits(buckets) -> Observation | None:
    """The latest observation that has `visits`: the predecessor for the next `visit_deltas` call."""
    found = None
    for obs in buckets:
        if obs.visits is not None and (found is None or obs.observed_at > found.observed_at):
            found = obs
    return found


def visit_delta(prev_visits, curr_visits) -> int | None:
    """Nonnegative difference of two cumulative visit counters; None if missing or the counter reset."""
    prev = _check_count(prev_visits, "prev_visits")
    curr = _check_count(curr_visits, "curr_visits")
    if prev is None or curr is None or curr < prev:
        return None
    return curr - prev


def visit_deltas(buckets, previous: Observation | None = None, max_gap: timedelta = MAX_VISIT_GAP) -> list[int | None]:
    """`new_visits` for each bucket of one game, chained in time order.

    `buckets`: output of `latest_per_bucket` (one per bucket, strictly increasing).
    `previous`: the latest earlier bucket with visits (from MongoDB in speed, from the previous
    hour of Parquet in batch), strictly before the first bucket; None if there is none.

    A bucket without visits gets None and does not become the predecessor. A gap above
    `max_gap` (measured on observed_at, the boundary itself is allowed) or a counter reset gives None.
    A delta belongs to the hour of its (later) bucket.
    """
    buckets = list(buckets)
    _check_order(buckets, previous)
    prev = previous if previous is not None and previous.visits is not None else None
    out: list[int | None] = []
    for obs in buckets:
        if obs.visits is None:
            out.append(None)
            continue
        if prev is None or obs.observed_at - prev.observed_at > max_gap:
            out.append(None)
        else:
            out.append(visit_delta(prev.visits, obs.visits))
        prev = obs
    return out


def _check_order(buckets: list, previous) -> None:
    chain = ([previous] if previous is not None else []) + buckets
    for obs in chain:
        if not isinstance(obs, Observation):
            raise ValueError(f"expected Observation, got {obs!r}")
    for a, b in zip(chain, chain[1:]):
        if not a.bucket < b.bucket:
            raise ValueError(
                f"buckets must be one per 5-minute bucket and strictly increasing "
                f"(use latest_per_bucket); got {a.observed_at} then {b.observed_at}"
            )


def hourly_new_visits(deltas) -> int | None:
    """Sum of the valid deltas of one hour; None if the hour has no valid delta (missing is not zero)."""
    total = None
    for d in deltas:
        d = _check_count(d, "delta")
        if d is None:
            continue
        if d < 0:
            raise ValueError(f"visit deltas are never negative, got {d}")
        total = d if total is None else total + d
    return total


def growth(curr_players, ref_players) -> float | None:
    """Percentage change (curr - ref) / ref * 100; None if either is missing or ref is zero."""
    curr = _check_count(curr_players, "curr_players")
    ref = _check_count(ref_players, "ref_players")
    if curr is None or ref is None or ref == 0:
        return None
    return (curr - ref) / ref * 100


def _player_index(history) -> tuple[list[datetime], list[Observation]]:
    candidates = []
    for obs in history:
        if not isinstance(obs, Observation):
            raise ValueError(f"expected Observation, got {obs!r}")
        if obs.players is not None:
            candidates.append(obs)
    candidates.sort(key=lambda o: o.observed_at)
    return [o.observed_at for o in candidates], candidates


def _nearest(index, target: datetime, tolerance: timedelta) -> Observation | None:
    times, candidates = index
    best = None
    i = bisect_left(times, target - tolerance)
    while i < len(times) and times[i] <= target + tolerance:
        diff = abs(times[i] - target)
        # strict < keeps the earlier observation on a tie (times are ascending)
        if best is None or diff < best[0]:
            best = (diff, candidates[i])
        i += 1
    return None if best is None else best[1]


def find_reference(history, target: datetime, tolerance: timedelta = GROWTH_TOLERANCE) -> Observation | None:
    """The observation with players nearest to `target` within +-tolerance (on observed_at); ties -> earlier."""
    return _nearest(_player_index(history), _check_ts(target, "target"), tolerance)


def growths(buckets, history) -> list[dict[str, float]]:
    """`growth_1h/3h/24h` for each bucket; a key is absent when it cannot be computed.

    `history`: every known observation of the game, MongoDB documents merged with the
    buckets being computed (merge through `latest_per_bucket` so the newer version wins).
    """
    index = _player_index(history)
    out: list[dict[str, float]] = []
    for obs in buckets:
        if not isinstance(obs, Observation):
            raise ValueError(f"expected Observation, got {obs!r}")
        values: dict[str, float] = {}
        if obs.players is not None:
            for name, horizon in GROWTH_HORIZONS.items():
                ref = _nearest(index, obs.observed_at - horizon, GROWTH_TOLERANCE)
                g = growth(obs.players, ref.players if ref is not None else None)
                if g is not None:
                    values[name] = g
        out.append(values)
    return out
