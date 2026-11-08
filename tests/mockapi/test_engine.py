import random

import pytest

from gametrend.mockapi import config
from gametrend.mockapi.clock import parse_utc
from gametrend.mockapi.engine import Engine
from tests.collector.fakes import load_fixture
from tests.mockapi.helpers import T0, SetClock, at, games, players, rec, sorts_cycle, write

P = config.FEED_PATH


def q(**params) -> dict:
    return {k: [str(v)] for k, v in params.items()}


def engine(tmp_path, records, start=0, **settings):
    write(tmp_path, records)
    clock = SetClock(at(start))
    return Engine(tmp_path, clock, config.Settings(data_dir=tmp_path, **settings), rng=random.Random(1)), clock


def ask(eng, feed, **params):
    return eng.answer(P[feed], q(**params))


# ------------------------------------------------------------------ per ID


def test_games_are_served_as_of_the_replay_time(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 100, 2: 200}), games(70, {1: 110, 2: 210})])
    clock.set(5)
    assert ask(eng, "roblox_games", universeIds="1,2").headers["X-Mock-Reason"] == "no-data"
    clock.set(69.9)
    body = ask(eng, "roblox_games", universeIds="2,1,3").body
    assert [(d["id"], d["playing"]) for d in body["data"]] == [(2, 200), (1, 100)]  # request order, unknown 3 left out
    assert body["data"][0]["_mock"] == {"observed_at": "2026-11-02T14:00:10.000Z"}
    clock.set(70)
    assert [d["playing"] for d in ask(eng, "roblox_games", universeIds="1").body["data"]] == [110]


def test_items_pass_unchanged_apart_from_mock(tmp_path):
    fixture = load_fixture("roblox_games")
    eng, clock = engine(tmp_path, [rec("roblox_games", 10, {"universeIds": "x"}, body=fixture)])
    clock.set(20)
    ids = ",".join(str(d["id"]) for d in fixture["data"])
    served = ask(eng, "roblox_games", universeIds=ids).body["data"]
    assert [{k: v for k, v in d.items() if k != "_mock"} for d in served] == fixture["data"]


def test_replay_time_is_never_overtaken_however_often_polled(tmp_path):
    rng = random.Random(7)
    records = []
    for cycle in range(30):  # games every 60 s, votes every 300 s, Steam every 300 s, sorts every 900 s
        records.append(games(cycle * 60 + rng.random(), {i: cycle for i in range(1, 6)}))
        if cycle % 5 == 0:
            records.append(games(cycle * 60 + 1, {i: cycle for i in range(1, 6)}, feed="roblox_votes"))
            records.append(players(cycle * 60 + 2, 440, cycle))
        if cycle % 15 == 0:
            records += sorts_cycle(cycle * 60 + 3, tag=str(cycle))
    eng, clock = engine(tmp_path, records)
    t = 0.0
    while t < 1800:
        t += rng.choice((0.05, 0.5, 3, 17))  # polled much faster and slower than recorded
        clock.set(t)
        for feed, params in (("roblox_games", {"universeIds": "1,3,5"}), ("roblox_votes", {"universeIds": "2,4"}),
                             ("steam_players", {"appid": 440}), ("roblox_sorts", {"country": "us"})):
            resp = ask(eng, feed, **params)
            if resp.status != 200:
                continue
            units = resp.body["data"] if "data" in resp.body else [resp.body]
            for unit in units:
                assert parse_utc(unit["_mock"]["observed_at"]) <= clock.now()


def test_stale_ids_and_feeds_are_not_served_as_current(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), games(400, {2: 2})])
    clock.set(10 + 600 + 1)  # ID 1 older than 10 min, ID 2 fresh
    assert [d["id"] for d in ask(eng, "roblox_games", universeIds="1,2").body["data"]] == [2]
    clock.set(400 + 600 + 1)
    resp = ask(eng, "roblox_games", universeIds="2")
    assert (resp.status, resp.headers["X-Mock-Reason"]) == (503, "no-data")


def test_steam_404_result_42_is_an_observation_and_unknown_appids_are_mock_404(tmp_path):
    eng, clock = engine(tmp_path, [players(10, 440, 89079), players(11, 1422450, None)])
    clock.set(20)
    ok = ask(eng, "steam_players", appid=440)
    assert (ok.status, ok.body["response"]["player_count"]) == (200, 89079)
    none = ask(eng, "steam_players", appid=1422450)
    assert (none.status, none.body["response"], "X-Mock-Reason" in none.headers) == (404, {"result": 42}, False)
    unknown = ask(eng, "steam_players", appid=1)
    assert (unknown.status, unknown.headers["X-Mock-Reason"]) == (404, "unknown-id")
    assert ask(eng, "steam_players", appid="x").status == 400


def test_appdetails_has_no_freshness_limit(tmp_path):
    body = load_fixture("steam_appdetails")
    appid = int(next(iter(body)))
    eng, clock = engine(tmp_path, [rec("steam_appdetails", 10, {"appids": appid}, body=body),
                                   games(3000, {1: 1})])  # archive continues for another hour
    clock.set(3500)
    resp = ask(eng, "steam_appdetails", appids=appid)
    assert resp.status == 200 and list(resp.body) == [str(appid)]  # top-level keys stay appids, as in the real API
    served = resp.body[str(appid)]
    assert served["_mock"] == {"observed_at": "2026-11-02T14:00:10.000Z"}
    assert {k: v for k, v in served.items() if k != "_mock"} == body[str(appid)]


# ------------------------------------------------------------------ errors


def test_archived_error_window_is_replayed_until_the_next_usable_attempt(tmp_path):
    limits = {"x-ratelimit-remaining": "0", "retry-after": "7"}
    eng, clock = engine(tmp_path, [
        games(10, {1: 1}),
        rec("roblox_games", 20, {"universeIds": "1"}, status=429, body={"errors": [{"code": 4}]}, rate_limit=limits),
        rec("roblox_games", 27, {"universeIds": "1"}, error="ConnectTimeout: boom", status=None),
        games(40, {1: 2}),
    ])
    clock.set(21)
    resp = ask(eng, "roblox_games", universeIds="1")
    assert (resp.status, resp.body, resp.headers["retry-after"]) == (429, {"errors": [{"code": 4}]}, "7")
    clock.set(30)
    resp = ask(eng, "roblox_games", universeIds="1")
    assert (resp.status, resp.headers["X-Mock-Reason"]) == (504, "network-error")
    clock.set(41)
    assert ask(eng, "roblox_games", universeIds="1").body["data"][0]["playing"] == 2


def test_error_window_ends_after_its_tail_without_a_later_attempt(tmp_path):
    eng, clock = engine(tmp_path, [players(10, 440, 5), rec("steam_players", 20, {"appid": 440}, status=500,
                                                            body={"e": 1})])
    clock.set(25)
    assert ask(eng, "steam_players", appid=440).status == 500
    clock.set(20 + config.ERROR_WINDOW_TAIL.total_seconds() + 1)
    assert ask(eng, "steam_players", appid=440).body["response"]["player_count"] == 5


def test_error_replay_can_be_switched_off(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), rec("roblox_games", 20, {"universeIds": "1"}, status=503,
                                                          body={})], replay_errors=False)
    clock.set(21)
    assert ask(eng, "roblox_games", universeIds="1").status == 200


def test_200_without_json_is_served_as_recorded_text(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), rec("roblox_games", 20, {"universeIds": "1"},
                                                          body_text="<html>oops</html>")])
    clock.set(21)
    resp = ask(eng, "roblox_games", universeIds="1")
    assert (resp.status, resp.body) == (200, "<html>oops</html>")


def test_injected_errors_are_marked(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1})], inject_p=1.0, inject_delay_s=35)
    clock.set(20)
    seen = {ask(eng, "roblox_games", universeIds="1").status for _ in range(30)}
    assert seen == {429, 500, 504}
    resp = ask(eng, "roblox_games", universeIds="1")
    assert resp.headers["X-Mock-Reason"] == "injected"
    assert all(ask(eng, "roblox_games", universeIds="1").delay_s in (0, 35) for _ in range(10))


# ------------------------------------------------------------------ sorts


def test_sorts_cycle_is_served_only_after_its_last_page(tmp_path):
    eng, clock = engine(tmp_path, sorts_cycle(100, pages=3))
    clock.set(101.5)  # pages at 100 and 101 seen, 102 not yet
    assert ask(eng, "roblox_sorts", country="us").headers["X-Mock-Reason"] == "no-data"
    clock.set(102)
    first_page = ask(eng, "roblox_sorts", country="us").body
    assert first_page["sorts"][0]["sortId"] == "s0"


def test_sorts_tokens_page_through_one_cycle_even_when_a_newer_one_appears(tmp_path):
    eng, clock = engine(tmp_path, sorts_cycle(100, tag="old") + sorts_cycle(1000, tag="new"))
    clock.set(200)
    page = ask(eng, "roblox_sorts", country="us").body
    clock.set(1100)  # the new cycle is complete now
    got = [page["sorts"][0]["sortId"]]
    while page["nextSortsPageToken"]:
        page = ask(eng, "roblox_sorts", country="us", sortsPageToken=page["nextSortsPageToken"]).body
        got.append(page["sorts"][0]["sortId"])
    assert got == ["olds0", "olds1", "olds2"]
    assert ask(eng, "roblox_sorts", country="us").body["sorts"][0]["sortId"] == "news0"
    bad = ask(eng, "roblox_sorts", country="us", sortsPageToken="real1")
    assert (bad.status, bad.headers["X-Mock-Reason"]) == (400, "unknown-token")


def test_stale_sorts_cycle_is_not_served(tmp_path):
    eng, clock = engine(tmp_path, sorts_cycle(10) + [games(3000, {1: 1})])
    clock.set(12 + 45 * 60 + 1)
    assert ask(eng, "roblox_sorts", country="us").status == 503
    assert ask(eng, "roblox_sorts", country="pl").status == 503


# ------------------------------------------------------------------ rare lists, ids, end


def test_rare_lists(tmp_path):
    charts = load_fixture("steam_most_played")
    eng, clock = engine(tmp_path, [
        rec("steam_charts", 10, body=charts),
        rec("steam_store_new", 11, {"filter": "popularnew", "start": 0}, body={"start": 0, "results_html": "a"}),
        rec("steam_store_new", 12, {"filter": "popularnew", "start": 100}, body={"start": 100, "results_html": "b"}),
        rec("roblox_search", 13, {"searchQuery": "Hollow Knight"}, body={"searchResults": [{"x": 1}]}),
    ])
    clock.set(20)
    assert ask(eng, "steam_charts").body["response"] == charts["response"]
    assert ask(eng, "steam_store_new", start=100).body["results_html"] == "b"
    assert ask(eng, "steam_store_new").body["results_html"] == "a"
    assert ask(eng, "steam_store_new", start=200).headers["X-Mock-Reason"] == "unknown-id"
    assert ask(eng, "roblox_search", searchQuery="Hollow Knight").body["searchResults"] == [{"x": 1}]
    assert ask(eng, "roblox_search", searchQuery="other").body == {"searchResults": []}


def test_ids_lists_fresh_ids_only(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), games(500, {2: 2}, feed="roblox_votes"),
                                   players(20, 440, 1)])
    clock.set(10 + 600 + 1)  # game 1 is stale, votes of 2 and Steam 440 fresh
    assert eng.answer("/_mock/ids", {}).body == {"roblox": [2], "steam": [440]}


def test_end_of_archive_and_unknown_paths(tmp_path):
    eng, clock = engine(tmp_path, [games(3500, {1: 1})])
    clock.set(3599)
    assert ask(eng, "roblox_games", universeIds="1").status == 200
    clock.set(3600)
    resp = ask(eng, "roblox_games", universeIds="1")
    assert (resp.status, resp.headers["X-Mock-Reason"]) == (503, "end")
    assert eng.answer("/_mock/clock", {}).body["ended"] is True
    assert eng.answer("/nope", {}).status == 404


def test_every_response_carries_the_replay_time(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1})])
    clock.set(20)
    assert ask(eng, "roblox_games", universeIds="1").headers["X-Mock-Time"] == "2026-11-02T14:00:20.000Z"


@pytest.mark.parametrize("restart_at", [0, 900, 1700])
def test_restart_gives_the_same_answers(tmp_path, restart_at):
    records = [games(s, {1: s, 2: -s}) for s in range(0, 1800, 60)] + [players(s, 440, s) for s in range(0, 1800, 300)]
    write(tmp_path, records)
    a = Engine(tmp_path, SetClock(T0))
    for t in range(0, 1750, 50):  # the first engine runs from the start
        a.clock.t = at(t)
        a.answer(P["roblox_games"], q(universeIds="1"))
    b = Engine(tmp_path, SetClock(at(restart_at)))
    for engine_ in (a, b):
        engine_.clock.t = at(1750)
    for feed, params in (("roblox_games", q(universeIds="1,2")), ("steam_players", q(appid=440))):
        assert a.answer(P[feed], params).body == b.answer(P[feed], params).body
