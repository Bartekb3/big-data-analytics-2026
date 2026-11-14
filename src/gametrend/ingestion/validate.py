"""Producer-only checks. The contract check itself (`is_valid`) is shared: gametrend.common.kafka_contract.

A record that fails validation is dropped by the producer and counted (contract § General).
"""

from datetime import datetime, timezone


def _parse_utc(value) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def after_mock_time(ts: str, mock_time: str | None) -> bool:
    """True if the event time `ts` is later than the mock's replay time of its answer (`X-Mock-Time`).

    Such an event is dropped: one future `ts` would move every data-time "now" (contracts/conventions.md, Time).
    Without a readable header nothing can be judged, so the event is kept.
    """
    limit = _parse_utc(mock_time)
    event_ts = _parse_utc(ts)
    if limit is None or event_ts is None:
        return False
    return event_ts > limit
