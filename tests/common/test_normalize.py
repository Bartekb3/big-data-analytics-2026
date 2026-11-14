from datetime import datetime, timezone

import pytest

from gametrend.common.metrics import Observation, visit_deltas
from gametrend.common.normalize import to_observation, to_python

UTC = timezone.utc


def test_plain_values_pass_through():
    assert to_python(None) is None
    assert to_python(5) == 5
    assert to_python("x") == "x"
    assert to_python(float("nan")) is None
    assert to_python(1.5) == 1.5


def test_naive_datetime_stays_naive_and_is_rejected_later():
    naive = datetime(2026, 11, 2, 10, 0)
    assert to_python(naive) == naive
    with pytest.raises(ValueError):
        to_observation(naive, 1, 1)


def test_pandas_and_numpy_values():
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    assert to_python(pd.NA) is None
    assert to_python(pd.NaT) is None
    assert to_python(np.nan) is None
    v = to_python(np.int64(2**40))
    assert v == 2**40 and type(v) is int
    ts = to_python(pd.Timestamp("2026-11-02T10:00:00Z"))
    assert type(ts) is datetime and ts == datetime(2026, 11, 2, 10, tzinfo=UTC)


def test_nullable_int64_column_with_gaps():
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(
        {
            "observed_at": pd.to_datetime(
                ["2026-11-02T10:00:00Z", "2026-11-02T10:05:00Z", "2026-11-02T10:10:00Z"], utc=True
            ),
            "players": pd.array([10, None, 12], dtype="Int64"),
            "visits": pd.array([2**31 + 1, None, 2**31 + 50], dtype="Int64"),
        }
    )
    buckets = [to_observation(r.observed_at, r.players, r.visits) for r in df.itertuples()]
    assert buckets[1] == Observation(datetime(2026, 11, 2, 10, 5, tzinfo=UTC))
    assert visit_deltas(buckets) == [None, None, 49]
