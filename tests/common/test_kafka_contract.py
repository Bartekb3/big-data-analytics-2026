"""The Kafka event contract shared by ingestion, speed and the batch sink (architecture/contracts/kafka-topics.md)."""

from datetime import datetime, timezone

import pytest

from gametrend.common import kafka_contract
from gametrend.common.kafka_contract import (PLATFORM_OF_TOPIC, TOPIC, field_types, is_valid, parse_date,
                                             parse_utc)

TS = "2026-11-06T12:00:00.000Z"


def test_valid_events_of_every_kind():
    assert is_valid("roblox", {"kind": "game", "ts": TS, "id": 1, "playing": 0, "visits": 5, "favorites": 2,
                               "updated": TS})
    assert is_valid("roblox", {"kind": "votes", "ts": TS, "id": 1, "up": 1, "down": 0})
    assert is_valid("roblox", {"kind": "sort", "ts": TS, "id": 1, "country": "us", "sort": "top-trending",
                               "rank": 1, "playing": 9})
    assert is_valid("roblox", {"kind": "meta", "ts": TS, "id": 1, "name": "A"})
    assert is_valid("steam", {"kind": "players", "ts": TS, "id": 1, "players": 0})
    assert is_valid("steam", {"kind": "meta", "ts": TS, "id": 1, "name": "A", "genres": ["Action"],
                              "released": "2025-02-26"})


def test_invalid_events():
    base = {"kind": "votes", "ts": TS, "id": 1, "up": 1, "down": 0}
    assert not is_valid("roblox", {**base, "down": None})
    assert not is_valid("roblox", {k: v for k, v in base.items() if k != "down"})
    assert not is_valid("roblox", {**base, "up": "1"})
    assert not is_valid("roblox", {**base, "up": True})  # bool is not an integer here
    assert not is_valid("roblox", {**base, "extra": 1})  # only the contract's fields
    assert not is_valid("roblox", {**base, "ts": "2026-11-06 12:00"})
    assert not is_valid("roblox", {**base, "id": "1"})
    assert not is_valid("steam", base)  # votes is not a Steam kind
    assert not is_valid("steam", {"kind": "meta", "ts": TS, "id": 1, "name": "A", "released": "Feb 2025"})
    assert not is_valid("roblox", {"kind": "meta", "ts": TS, "id": 1})  # name is required


def test_non_dict_is_invalid():
    assert not is_valid("roblox", None)
    assert not is_valid("roblox", [1, 2])
    assert not is_valid("roblox", "game")


def test_topic_platform_maps_are_inverse():
    assert TOPIC == {"roblox": "roblox.snapshots", "steam": "steam.players"}
    assert {PLATFORM_OF_TOPIC[t]: t for t in PLATFORM_OF_TOPIC} == TOPIC


def test_field_types_cover_every_field_common_first():
    types = field_types()
    assert list(types)[:3] == ["kind", "ts", "id"]
    assert types["playing"] == "int"  # in `game` and `sort`, one type
    assert types["visits"] == "int"
    assert types["updated"] == "iso"
    assert types["released"] == "date"
    assert types["genres"] == "str_list"
    assert types["country"] == "str"
    fields = {name for required, optional in kafka_contract.SCHEMA.values() for name in {**required, **optional}}
    assert set(types) == fields | {"kind", "ts", "id"}


def test_field_types_reject_one_name_with_two_types(monkeypatch):
    schema = dict(kafka_contract.SCHEMA)
    schema[("steam", "players")] = ({"players": kafka_contract._str}, {})
    schema[("roblox", "votes")] = ({"players": kafka_contract._int}, {})
    monkeypatch.setattr(kafka_contract, "SCHEMA", schema)
    with pytest.raises(ValueError, match="players"):
        field_types()


def test_parse_utc_and_parse_date():
    assert parse_utc(TS) == datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    assert parse_utc(TS).tzinfo is not None
    assert parse_date("2025-02-26") == datetime(2025, 2, 26, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        parse_utc("2026-02-30T12:00:00.000Z")  # passes the contract regex, no real date
    with pytest.raises(ValueError):
        parse_date("2025-02-30")
