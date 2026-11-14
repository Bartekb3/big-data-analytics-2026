"""Kafka records -> typed events. Validation is the shared contract check (gametrend.common.kafka_contract).

`sort` events are valid but not used by speed (architecture/speed/README.md, Event roles): counted and dropped.
Invalid records are counted and dropped; they never stop the stream.
"""

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ..common.kafka_contract import PLATFORM_OF_TOPIC, is_valid


@dataclass(frozen=True)
class PlayerEvent:
    """Roblox `game` or Steam `players` event. Steam carries only `players`; absent values are None."""

    platform: str
    id: int
    observed_at: datetime
    players: int
    visits: int | None = None
    favorites: int | None = None
    updated: datetime | None = None


@dataclass(frozen=True)
class VotesEvent:
    platform: str
    id: int
    observed_at: datetime
    up: int
    down: int


@dataclass(frozen=True)
class MetaEvent:
    """`meta` event; `fields` already uses the `games` field names and types (only present values)."""

    platform: str
    id: int
    observed_at: datetime
    fields: dict


@dataclass
class Parsed:
    players: list[PlayerEvent] = field(default_factory=list)
    votes: list[VotesEvent] = field(default_factory=list)
    metas: list[MetaEvent] = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)  # per kind, plus "invalid"


def parse_utc(value: str) -> datetime:
    """Contract timestamp (`...Z`) -> aware UTC datetime. Raises ValueError on an impossible date."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_date(value: str) -> datetime:
    """Date-only value (`YYYY-MM-DD`) -> 00:00 UTC of that day (conventions § Time)."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _meta_fields(platform: str, event: dict) -> dict:
    out = {"name": event["name"]}
    if platform == "roblox":
        for name in ("genre_l1", "genre_l2", "description"):
            if name in event:
                out[name] = event[name]
        if "created" in event:
            out["created"] = parse_utc(event["created"])
    else:
        if "genres" in event:
            out["genres"] = list(event["genres"])
        if "short_description" in event:
            out["description"] = event["short_description"]
        if "released" in event:
            out["released"] = parse_date(event["released"])
    return out


def to_event(platform: str, event: dict):
    """A validated event -> PlayerEvent / VotesEvent / MetaEvent, or None for `sort`."""
    kind = event["kind"]
    observed_at = parse_utc(event["ts"])
    gid = event["id"]
    if kind == "game":
        return PlayerEvent(platform, gid, observed_at, players=event["playing"], visits=event["visits"],
                           favorites=event["favorites"], updated=parse_utc(event["updated"]))
    if kind == "players":
        return PlayerEvent(platform, gid, observed_at, players=event["players"])
    if kind == "votes":
        return VotesEvent(platform, gid, observed_at, up=event["up"], down=event["down"])
    if kind == "meta":
        return MetaEvent(platform, gid, observed_at, _meta_fields(platform, event))
    return None  # sort


def parse_record(topic: str, value) -> tuple[str, object]:
    """One Kafka record -> (kind or "invalid", event or None)."""
    platform = PLATFORM_OF_TOPIC.get(topic)
    if platform is None or value is None:
        return "invalid", None
    try:
        event = json.loads(value)
    except (ValueError, TypeError):
        return "invalid", None
    if not is_valid(platform, event):
        return "invalid", None
    try:
        return event["kind"], to_event(platform, event)
    except ValueError:  # passes the regex but is no real date, e.g. 2026-02-30
        return "invalid", None


def parse_records(records) -> Parsed:
    """`records`: iterable of (topic, value) with value the JSON string (or bytes) of one event."""
    out = Parsed()
    for topic, value in records:
        kind, event = parse_record(topic, value)
        out.counts[kind] += 1
        if isinstance(event, PlayerEvent):
            out.players.append(event)
        elif isinstance(event, VotesEvent):
            out.votes.append(event)
        elif isinstance(event, MetaEvent):
            out.metas.append(event)
    return out
