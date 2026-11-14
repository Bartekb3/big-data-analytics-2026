from gametrend.ingestion import events
from gametrend.ingestion.validate import is_valid

from .conftest import load, mocked

OBS = "2026-11-06T12:00:03.412Z"
REQ = "2026-11-06T12:00:05.000Z"


def all_events(parsed):
    return [e for o in parsed.observations for e in o.events]


# ------------------------------------------------------------------ helpers


def test_iso_utc_normalises_to_milliseconds_and_z():
    assert events.iso_utc("2020-04-21T02:15:38.37Z") == "2020-04-21T02:15:38.370Z"
    assert events.iso_utc("2026-10-16T19:32:59.826+02:00") == "2026-10-16T17:32:59.826Z"
    assert events.iso_utc("2026-10-16T17:32:59") == "2026-10-16T17:32:59.000Z"
    assert events.iso_utc("yesterday") is None
    assert events.iso_utc(None) is None


def test_steam_release_date_formats():
    assert events.steam_release_date({"coming_soon": False, "date": "Feb 26, 2025"}) == "2025-02-26"
    assert events.steam_release_date({"coming_soon": False, "date": "26 Feb, 2025"}) == "2025-02-26"
    assert events.steam_release_date({"coming_soon": False, "date": "Jul 9, 2013"}) == "2013-07-09"


def test_steam_release_date_missing_when_not_a_full_date_or_unreleased():
    assert events.steam_release_date({"coming_soon": False, "date": "To be announced"}) is None
    assert events.steam_release_date({"coming_soon": False, "date": "Q1 2027"}) is None
    assert events.steam_release_date({"coming_soon": True, "date": "Oct 28, 2026"}) is None
    assert events.steam_release_date(None) is None


# ------------------------------------------------------------------ Roblox games


def test_roblox_games_fixture_real_api_gives_game_and_meta_per_item():
    body = load("roblox_games")
    parsed = events.parse_roblox_games(body, REQ)
    assert parsed.dropped == 0
    assert len(parsed.observations) == len(body["data"])
    game, meta = parsed.observations[0].events
    item = body["data"][0]
    assert game == {"kind": "game", "ts": REQ, "id": item["id"], "playing": item["playing"],
                    "visits": item["visits"], "favorites": item["favoritedCount"],
                    "updated": events.iso_utc(item["updated"])}
    assert meta["name"] == item["name"] and meta["genre_l1"] == item["genre_l1"]
    assert meta["created"] == "2020-04-21T02:15:38.373Z"
    assert all(is_valid("roblox", e) for e in all_events(parsed))


def test_roblox_games_mock_uses_observed_at_per_item():
    body = load("roblox_games")
    body = {"data": [mocked(it, OBS) for it in body["data"]]}
    parsed = events.parse_roblox_games(body, None)
    assert {o.ts for o in parsed.observations} == {OBS}
    assert parsed.observations[0].key == ("roblox_games", body["data"][0]["id"])
    assert all("_mock" not in e for e in all_events(parsed))


def test_roblox_games_mock_item_without_observed_at_is_dropped():
    body = load("roblox_games")
    parsed = events.parse_roblox_games({"data": body["data"][:1]}, None)
    assert parsed.observations == [] and parsed.dropped == 1


def test_roblox_meta_leaves_out_missing_and_empty_values():
    item = {"id": 5, "name": "X", "description": "", "genre_l1": None, "playing": 1, "visits": 2,
            "favoritedCount": 3, "updated": "2026-11-01T00:00:00Z", "created": "bad"}
    _, meta = events.parse_roblox_games({"data": [item]}, REQ).observations[0].events
    assert meta == {"kind": "meta", "ts": REQ, "id": 5, "name": "X"}
    assert is_valid("roblox", meta)


def test_roblox_game_with_missing_or_null_counter_fails_validation():
    item = {"id": 5, "name": "X", "playing": None, "favoritedCount": 3, "updated": "2026-11-01T00:00:00Z"}
    game, _ = events.parse_roblox_games({"data": [item]}, REQ).observations[0].events
    assert "visits" not in game and game["playing"] is None
    assert not is_valid("roblox", game)


def test_roblox_games_body_that_is_not_an_object_or_has_bad_items():
    assert events.parse_roblox_games(None, REQ).dropped == 1
    assert events.parse_roblox_games({"errors": []}, REQ).dropped == 1
    parsed = events.parse_roblox_games({"data": [None, {"id": "x"}, {"name": "no id"}]}, REQ)
    assert parsed.observations == [] and parsed.dropped == 3


# ------------------------------------------------------------------ votes and sorts


def test_roblox_votes_fixture():
    body = load("roblox_votes")
    parsed = events.parse_roblox_votes(body, REQ)
    first = parsed.observations[0].events[0]
    assert first == {"kind": "votes", "ts": REQ, "id": 1686885941, "up": 8652738, "down": 1422073}
    assert all(is_valid("roblox", e) for e in all_events(parsed))


def test_roblox_sorts_page_fixture_one_event_per_game_per_games_sort():
    body = load("roblox_sorts")
    parsed = events.parse_roblox_sorts_page(body, "us", REQ)
    evs = all_events(parsed)
    game_sorts = [s for s in body["sorts"] if s["contentType"] == "Games"]
    assert len(evs) == sum(len(s["games"]) for s in game_sorts)
    first = evs[0]
    g = game_sorts[0]["games"][0]
    assert first == {"kind": "sort", "ts": REQ, "id": g["universeId"], "country": "us",
                     "sort": game_sorts[0]["sortId"], "rank": 1, "playing": g["playerCount"]}
    assert [e["rank"] for e in evs[:3]] == [1, 2, 3]
    assert all(is_valid("roblox", e) for e in evs)
    assert parsed.observations[0].key == ("roblox_sorts", "us")


def test_roblox_sorts_page_mock_time_is_on_the_page():
    body = mocked(load("roblox_sorts"), OBS)
    parsed = events.parse_roblox_sorts_page(body, "pl", None)
    assert parsed.observations[0].ts == OBS
    assert {e["ts"] for e in all_events(parsed)} == {OBS}


def test_roblox_sorts_page_without_sorts_is_dropped():
    assert events.parse_roblox_sorts_page({"header": {}}, "us", REQ).dropped == 1


# ------------------------------------------------------------------ Steam


def test_steam_players_real_and_mock():
    resp = load("steam_current_players")["730"]  # the fixture holds several answers keyed by appid
    parsed = events.parse_steam_players(resp, 730, REQ)
    assert all_events(parsed) == [{"kind": "players", "ts": REQ, "id": 730, "players": 586936}]
    parsed = events.parse_steam_players(mocked(resp, OBS), 730, None)
    assert all_events(parsed)[0]["ts"] == OBS


def test_steam_players_result_42_gives_no_event_and_no_drop():
    parsed = events.parse_steam_players({"response": {"result": 42}}, 1, REQ)
    assert parsed.observations == [] and parsed.dropped == 0


def test_steam_players_zero_is_kept():
    parsed = events.parse_steam_players({"response": {"player_count": 0, "result": 1}}, 1, REQ)
    assert all_events(parsed)[0]["players"] == 0


def test_steam_appdetails_fixture():
    body = load("steam_appdetails")
    parsed = events.parse_steam_appdetails(body, 3241660, REQ)
    (meta,) = all_events(parsed)
    assert meta["name"] == "R.E.P.O."
    assert meta["genres"] == ["Action", "Early Access"]
    assert meta["released"] == "2025-02-26"
    assert meta["short_description"].startswith("An online co-op horror game")
    assert set(meta) == {"kind", "ts", "id", "name", "genres", "released", "short_description"}
    assert is_valid("steam", meta)


def test_steam_appdetails_mock_time_inside_the_app_object():
    body = load("steam_appdetails")
    body = {"3241660": mocked(body["3241660"], OBS)}
    (meta,) = all_events(events.parse_steam_appdetails(body, 3241660, None))
    assert meta["ts"] == OBS


def test_steam_appdetails_success_false_and_tba_date():
    assert events.parse_steam_appdetails({"7": {"success": False}}, 7, REQ).observations == []
    body = {"7": {"success": True, "data": {"name": "Soon", "genres": [],
                                            "release_date": {"coming_soon": False, "date": "To be announced"}}}}
    (meta,) = all_events(events.parse_steam_appdetails(body, 7, REQ))
    assert meta == {"kind": "meta", "ts": REQ, "id": 7, "name": "Soon"}
