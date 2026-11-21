import json
from datetime import datetime, timezone

from gametrend.ingestion import events as ingestion_events
from gametrend.speed.parse import MetaEvent, PlayerEvent, VotesEvent, parse_record, parse_records

from ..ingestion.conftest import load
from .helpers import RT, ST, game_json, steam_json, t, votes_json

TS = "2026-11-06T12:00:03.412Z"
AT = datetime(2026, 11, 6, 12, 0, 3, 412000, tzinfo=timezone.utc)


def test_game_event():
    kind, e = parse_record(RT, game_json(5, AT, playing=7, visits=2**40, favorites=3, updated=t("01:00")))
    assert kind == "game"
    assert e == PlayerEvent("roblox", 5, AT, players=7, visits=2**40, favorites=3, updated=t("01:00"))
    assert e.observed_at.tzinfo is not None


def test_votes_and_steam_players():
    assert parse_record(RT, votes_json(5, AT, 10, 2)) == ("votes", VotesEvent("roblox", 5, AT, 10, 2))
    assert parse_record(ST, steam_json(730, AT, 0)) == ("players", PlayerEvent("steam", 730, AT, players=0))


def test_roblox_meta_maps_types_and_keeps_missing_absent():
    value = json.dumps({"kind": "meta", "ts": TS, "id": 5, "name": "A", "genre_l1": "RPG",
                        "created": "2020-04-21T02:15:38.373Z"})
    kind, e = parse_record(RT, value)
    assert kind == "meta"
    assert e == MetaEvent("roblox", 5, AT, {"name": "A", "genre_l1": "RPG",
                                            "created": datetime(2020, 4, 21, 2, 15, 38, 373000, tzinfo=timezone.utc)})


def test_steam_meta_renames_description_and_dates_released():
    value = json.dumps({"kind": "meta", "ts": TS, "id": 730, "name": "CS", "genres": ["Action"],
                        "released": "2012-08-21", "short_description": "shoot"})
    _, e = parse_record(ST, value)
    assert e.fields == {"name": "CS", "genres": ["Action"], "description": "shoot",
                        "released": datetime(2012, 8, 21, tzinfo=timezone.utc)}


def test_sort_is_counted_and_dropped():
    value = json.dumps({"kind": "sort", "ts": TS, "id": 5, "country": "us", "sort": "top-trending", "rank": 1,
                        "playing": 9})
    assert parse_record(RT, value) == ("sort", None)


def test_invalid_records():
    good = json.loads(votes_json(5, AT, 1, 0))
    assert parse_record(RT, "not json")[0] == "invalid"
    assert parse_record(RT, None)[0] == "invalid"
    assert parse_record("other.topic", json.dumps(good))[0] == "invalid"
    assert parse_record(ST, json.dumps(good))[0] == "invalid"  # votes is not a Steam kind
    assert parse_record(RT, json.dumps({k: v for k, v in good.items() if k != "up"}))[0] == "invalid"
    assert parse_record(RT, json.dumps({**good, "up": "1"}))[0] == "invalid"
    assert parse_record(RT, json.dumps({**good, "ts": "2026-02-30T12:00:00.000Z"}))[0] == "invalid"  # no such day
    assert parse_record(RT, "[1, 2]")[0] == "invalid"


def test_parse_records_splits_and_counts():
    parsed = parse_records([
        (RT, game_json(1, AT, 1, 1)), (RT, votes_json(1, AT, 1, 0)), (ST, steam_json(2, AT, 3)), (RT, "x"),
        (RT, json.dumps({"kind": "meta", "ts": TS, "id": 1, "name": "A"})),
    ])
    assert len(parsed.players) == 2 and len(parsed.votes) == 1 and len(parsed.metas) == 1
    assert parsed.counts == {"game": 1, "votes": 1, "players": 1, "meta": 1, "invalid": 1}


def test_bytes_value_is_accepted():
    assert parse_record(ST, steam_json(2, AT, 3).encode())[0] == "players"


def test_events_built_by_ingestion_from_recorded_payloads_parse():
    """Producer and consumer agree on the contract: every event ingestion builds is read by speed."""
    req = "2026-11-06T12:00:05.000Z"
    built = [e for o in ingestion_events.parse_roblox_games(load("roblox_games"), req).observations for e in o.events]
    built += [e for o in ingestion_events.parse_roblox_votes(load("roblox_votes"), req).observations
              for e in o.events]
    parsed = parse_records([(RT, json.dumps(e)) for e in built])
    assert parsed.counts["invalid"] == 0
    assert parsed.counts["game"] > 0 and parsed.counts["votes"] > 0 and parsed.counts["meta"] > 0
