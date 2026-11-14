"""Explicit conversion of pandas/numpy values to plain Python values for the metric functions.

The metric functions reject NaN, pd.NA and numpy scalars; callers that read pandas
(batch applyInPandas) or other loose sources call these first. pandas and numpy are
recognised without being imported, so speed does not need them.
"""

import math
import numbers
from datetime import datetime, timezone

from gametrend.common.metrics import Observation


def to_python(value):
    """NaN/NaT/pd.NA -> None, numpy integer -> int, numpy float -> float, pd.Timestamp -> datetime (UTC).

    A naive datetime stays naive: the metric functions then reject it loudly.
    """
    if value is None:
        return None
    if type(value).__name__ in ("NAType", "NaTType"):
        return None
    if isinstance(value, datetime):
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value
    if isinstance(value, bool) or type(value).__name__ == "bool_":
        return bool(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        f = float(value)
        return None if math.isnan(f) else f
    return value


def to_observation(observed_at, players=None, visits=None) -> Observation:
    """Observation from loose values (e.g. one pandas row with nullable Int64 columns)."""
    return Observation(to_python(observed_at), to_python(players), to_python(visits))
