from datetime import datetime, timedelta, timezone

import pytest

from gametrend.common.metrics import (
    Observation,
    bucket_start,
    find_reference,
    growth,
    growths,
    hour_start,
    hourly_new_visits,
    last_with_visits,
    latest_per_bucket,
    visit_delta,
    visit_deltas,
)

UTC = timezone.utc
MS = timedelta(milliseconds=1)


def t(h, m=0, s=0, us=0, day=2):
    return datetime(2026, 11, day, h, m, s, us, tzinfo=UTC)


def obs(at, players=None, visits=None):
    return Observation(at, players, visits)


# --- buckets ---------------------------------------------------------------


def test_bucket_start_on_boundary():
    assert bucket_start(t(10, 5)) == t(10, 5)


def test_bucket_start_just_before_boundary():
    assert bucket_start(t(10, 4, 59, 999999)) == t(10, 0)


def test_bucket_start_across_hour_and_day():
    assert bucket_start(t(10, 59, 59)) == t(10, 55)
    assert bucket_start(t(23, 59, 59)) == t(23, 55)
    assert bucket_start(t(0, 0, 0, day=3)) == t(0, 0, day=3)


def test_bucket_start_converts_other_timezone_to_utc():
    cet = timezone(timedelta(hours=1))
    assert bucket_start(datetime(2026, 11, 2, 11, 7, tzinfo=cet)) == t(10, 5)


def test_bucket_start_rejects_naive():
    with pytest.raises(ValueError):
        bucket_start(datetime(2026, 11, 2, 10, 7))


def test_hour_start():
    assert hour_start(t(10, 59, 59)) == t(10)


def test_observation_rejects_naive_and_nan():
    with pytest.raises(ValueError):
        Observation(datetime(2026, 11, 2, 10, 0), 1, 1)
    with pytest.raises(ValueError):
        Observation(t(10), float("nan"), 1)
    with pytest.raises(ValueError):
        Observation(t(10), 1, 2.0)


def test_latest_per_bucket_latest_wins_and_sorted():
    a = obs(t(10, 1), 10, 100)
    b = obs(t(10, 4), 11, 110)
    c = obs(t(10, 6), 12, 120)
    assert latest_per_bucket([c, b, a]) == [b, c]


def test_latest_per_bucket_same_observed_at_is_order_independent():
    a = obs(t(10, 1), 10, 100)
    b = obs(t(10, 1), 10, 105)
    c = obs(t(10, 1), None, 999)
    assert latest_per_bucket([a, b, c]) == [b]
    assert latest_per_bucket([c, b, a]) == [b]


def test_latest_per_bucket_keeps_votes_only_bucket():
    v = obs(t(10, 2))
    assert latest_per_bucket([v]) == [v]


def test_last_with_visits():
    a = obs(t(10, 1), 1, 100)
    b = obs(t(10, 6), 1, None)
    assert last_with_visits([a, b]) == a
    assert last_with_visits([b]) is None


# --- new_visits ------------------------------------------------------------


def test_visit_delta():
    assert visit_delta(100, 150) == 50
    assert visit_delta(100, 100) == 0
    assert visit_delta(100, 90) is None
    assert visit_delta(None, 90) is None
    assert visit_delta(100, None) is None


def test_visit_deltas_chain_with_and_without_previous():
    buckets = [obs(t(10, 0), 1, 100), obs(t(10, 5), 1, 130), obs(t(10, 10), 1, 130)]
    assert visit_deltas(buckets) == [None, 30, 0]
    assert visit_deltas(buckets, previous=obs(t(9, 55), 1, 90)) == [10, 30, 0]


def test_visit_deltas_reset_gives_none_and_continues():
    buckets = [obs(t(10, 0), 1, 100), obs(t(10, 5), 1, 5), obs(t(10, 10), 1, 25)]
    assert visit_deltas(buckets) == [None, None, 20]


def test_visit_deltas_bucket_without_visits_does_not_move_predecessor():
    buckets = [obs(t(10, 0), 1, 100), obs(t(10, 5), 1, None), obs(t(10, 10), 1, 140)]
    assert visit_deltas(buckets) == [None, None, 40]


def test_visit_deltas_previous_without_visits_is_ignored():
    assert visit_deltas([obs(t(10, 0), 1, 100)], previous=obs(t(9, 55), 1, None)) == [None]


@pytest.mark.parametrize(
    "gap, expected",
    [(timedelta(hours=1) - MS, 10), (timedelta(hours=1), 10), (timedelta(hours=1) + MS, None)],
)
def test_visit_deltas_max_gap_boundary(gap, expected):
    prev = obs(t(9, 0, 30), 1, 100)
    assert visit_deltas([obs(prev.observed_at + gap, 1, 110)], previous=prev) == [expected]


def test_visit_deltas_large_counters():
    big = 2**31 + 5
    assert visit_deltas([obs(t(10, 0), 1, big), obs(t(10, 5), 1, big + 7_000_000_000)]) == [None, 7_000_000_000]


def test_visit_deltas_reset_inside_one_bucket_uses_latest_only():
    # 10:01 reset to 5, 10:04 back up: only the 10:04 observation counts for the bucket
    raw = [obs(t(9, 58), 1, 100), obs(t(10, 1), 1, 5), obs(t(10, 4), 1, 120)]
    assert visit_deltas(latest_per_bucket(raw)) == [None, 20]


def test_visit_deltas_rejects_unsorted_or_duplicate_buckets():
    with pytest.raises(ValueError):
        visit_deltas([obs(t(10, 5), 1, 1), obs(t(10, 0), 1, 1)])
    with pytest.raises(ValueError):
        visit_deltas([obs(t(10, 1), 1, 1), obs(t(10, 3), 1, 1)])
    with pytest.raises(ValueError):
        visit_deltas([obs(t(10, 0), 1, 1)], previous=obs(t(10, 2), 1, 1))


def test_visit_deltas_rejects_pandas_na():
    pd = pytest.importorskip("pandas")
    with pytest.raises(ValueError):
        visit_deltas([obs(t(10, 0), 1, pd.NA)])


def test_chunked_equals_whole_sequence():
    """Speed (micro-batches chained through `previous`) and batch (whole hour) give the same numbers."""
    visits = [100, 120, None, 150, 10, 30, 30, 45, None, 80, 95, 300, 310, 320]
    buckets = [obs(t(10, 40) + i * timedelta(minutes=5), 1, v) for i, v in enumerate(visits)]
    whole = visit_deltas(buckets)

    chunked: list = []
    previous = None
    for start in range(0, len(buckets), 3):
        chunk = buckets[start : start + 3]
        chunked += visit_deltas(chunk, previous=previous)
        previous = last_with_visits(chunk) or previous
    assert chunked == whole
    assert whole == [None, 20, None, 30, None, 20, 0, 15, None, 35, 15, 205, 10, 10]

    # hourly sums: a delta belongs to the hour of its bucket
    by_hour: dict = {}
    for b, d in zip(buckets, whole):
        by_hour.setdefault(hour_start(b.observed_at), []).append(d)
    assert hourly_new_visits(by_hour[t(10)]) == 50
    assert hourly_new_visits(by_hour[t(11)]) == 310


# --- hourly sum ------------------------------------------------------------


def test_hourly_new_visits():
    assert hourly_new_visits([None, None]) is None
    assert hourly_new_visits([]) is None
    assert hourly_new_visits([0, 0]) == 0
    assert hourly_new_visits([None, 5, None, 7]) == 12


def test_hourly_new_visits_rejects_nan_and_negative():
    with pytest.raises(ValueError):
        hourly_new_visits([1, float("nan")])
    with pytest.raises(ValueError):
        hourly_new_visits([-1])


# --- growth ----------------------------------------------------------------


def test_growth():
    assert growth(150, 100) == 50.0
    assert growth(0, 100) == -100.0
    assert growth(5, 0) is None
    assert growth(5, None) is None
    assert growth(None, 5) is None


def test_find_reference_tolerance_boundary():
    target = t(10)
    inside = obs(target + timedelta(minutes=15), 10)
    outside = obs(target - timedelta(minutes=15) - MS, 20)
    assert find_reference([inside, outside], target) == inside
    assert find_reference([outside], target) is None


def test_find_reference_tie_goes_to_earlier():
    early = obs(t(9, 55), 10)
    late = obs(t(10, 5), 20)
    assert find_reference([late, early], t(10)) == early


def test_find_reference_skips_without_players_and_empty():
    nearest_votes_only = obs(t(10, 0))
    further = obs(t(10, 10), 7)
    assert find_reference([nearest_votes_only, further], t(10)) == further
    assert find_reference([], t(10)) is None


def test_growths_reference_from_mongo_batch_and_merged():
    current = obs(t(12), 150)
    one_hour_ago_mongo = obs(t(11, 1), 100)
    # the same bucket arrived again in the micro-batch with a newer observation
    one_hour_ago_batch = obs(t(11, 3), 120)

    assert growths([current], [one_hour_ago_mongo]) == [{"growth_1h": 50.0}]
    assert growths([current], [one_hour_ago_batch, current]) == [{"growth_1h": 25.0}]
    merged = latest_per_bucket([one_hour_ago_mongo, one_hour_ago_batch, current])
    assert growths([current], merged) == [{"growth_1h": 25.0}]


def test_growths_all_horizons_and_absent_keys():
    current = obs(t(12, day=3), 0)
    history = [obs(t(11, day=3), 50), obs(t(9, day=3), 0), obs(t(12, day=2), 200)]
    assert growths([current], history) == [{"growth_1h": -100.0, "growth_24h": -100.0}]
    assert growths([obs(t(12, day=3))], history) == [{}]
