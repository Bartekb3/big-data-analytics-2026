"""Kafka event contract shared by the producers (ingestion) and the consumers (speed, batch sink).

Fields, types and topics: architecture/contracts/kafka-topics.md. One implementation, so a contract
change is made in one place.
"""

import re
from datetime import datetime, timezone

TOPIC = {"roblox": "roblox.snapshots", "steam": "steam.players"}
PLATFORM_OF_TOPIC = {topic: platform for platform, topic in TOPIC.items()}

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


def is_valid(platform: str, event: dict) -> bool:
    """True if the event has exactly the contract's fields, every required one, all with the right type."""
    if not isinstance(event, dict):
        return False
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


# type tag of each check, for consumers that need a typed layout (batch: Parquet schema)
_TYPE_TAG = {_int: "int", _str: "str", _iso: "iso", _date: "date", _str_list: "str_list"}


def field_types() -> dict[str, str]:
    """Every contract field -> type tag (`int`, `str`, `iso`, `date`, `str_list`), common fields first,
    then in SCHEMA order. Raises ValueError if one name had two types in different kinds."""
    out = {name: _TYPE_TAG[check] for name, check in COMMON.items()}
    for required, optional in SCHEMA.values():
        for name, check in {**required, **optional}.items():
            tag = _TYPE_TAG[check]
            if out.setdefault(name, tag) != tag:
                raise ValueError(f"field {name!r} has two types: {out[name]} and {tag}")
    return out


def parse_utc(value: str) -> datetime:
    """Contract timestamp (`...Z`) -> aware UTC datetime. Raises ValueError on an impossible date."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_date(value: str) -> datetime:
    """Date-only value (`YYYY-MM-DD`) -> 00:00 UTC of that day (conventions § Time)."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
