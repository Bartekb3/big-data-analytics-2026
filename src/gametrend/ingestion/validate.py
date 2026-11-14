"""Validation of events against the Kafka contract (architecture/contracts/kafka-topics.md).

A record that fails validation is dropped by the producer and counted (contract § General).
"""

import re
from datetime import datetime, timezone

ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _str(v) -> bool:
    return isinstance(v, str)


def _iso(v) -> bool:
    return isinstance(v, str) and bool(ISO_UTC.match(v))


def _date(v) -> bool:
    return isinstance(v, str) and bool(DATE.match(v))


def _str_list(v) -> bool:
    return isinstance(v, list) and all(isinstance(x, str) for x in v)


COMMON = {"kind": _str, "ts": _iso, "id": _int}

# (platform, kind) -> (required extra fields, optional extra fields); each field -> type check
SCHEMA = {
    ("roblox", "game"): ({"playing": _int, "visits": _int, "favorites": _int, "updated": _iso}, {}),
    ("roblox", "votes"): ({"up": _int, "down": _int}, {}),
    ("roblox", "sort"): ({"country": _str, "sort": _str, "rank": _int, "playing": _int}, {}),
    ("roblox", "meta"): ({"name": _str},
                         {"genre_l1": _str, "genre_l2": _str, "created": _iso, "description": _str}),
    ("steam", "players"): ({"players": _int}, {}),
    ("steam", "meta"): ({"name": _str},
                        {"genres": _str_list, "released": _date, "short_description": _str}),
}


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


def is_valid(platform: str, event: dict) -> bool:
    """True if the event has exactly the contract's fields, every required one, all with the right type."""
    schema = SCHEMA.get((platform, event.get("kind")))
    if schema is None:
        return False
    required, optional = schema
    checks = {**COMMON, **required, **optional}
    if set(event) - set(checks):
        return False  # only the fields listed in the contract are sent
    for name, check in {**COMMON, **required}.items():
        if name not in event or not check(event[name]):
            return False
    return all(check(event[name]) for name, check in optional.items() if name in event)
