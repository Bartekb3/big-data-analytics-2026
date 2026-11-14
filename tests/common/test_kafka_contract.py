"""The Kafka event contract shared by ingestion and speed (architecture/contracts/kafka-topics.md)."""

from gametrend.common.kafka_contract import PLATFORM_OF_TOPIC, TOPIC, is_valid

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
