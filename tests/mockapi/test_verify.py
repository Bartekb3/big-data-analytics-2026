import random

from gametrend.mockapi import config
from gametrend.mockapi.engine import Engine
from gametrend.mockapi.verify import Oracle
from tests.mockapi.helpers import SetClock, at, games, players, rec, sorts_cycle, write

P = config.FEED_PATH


def archive(rng: random.Random) -> list[dict]:
    out = []
    for cycle in range(40):  # 40 min: games every 60 s, the rest slower; a 429 burst and a network error
        t = cycle * 60
        out.append(games(t + rng.random(), {i: cycle * 10 + i for i in range(1, 6)}))
        if cycle in (12, 13):
            out.append(rec("roblox_games", t + 20, {"universeIds": "1"}, status=429, body={"errors": []},
                           rate_limit={"retry-after": "7"}))
        if cycle % 5 == 0:
            out.append(games(t + 1, {i: cycle for i in range(2, 5)}, feed="roblox_votes"))
            out.append(players(t + 2, 440, cycle))
            out.append(players(t + 3, 9, None))  # Steam 404 result 42
        if cycle == 21:
            out.append(rec("steam_players", t + 4, {"appid": 440}, status=None, error="ReadTimeout"))
        if cycle % 15 == 0:
            out += sorts_cycle(t + 5, tag=str(cycle))
        if cycle % 20 == 0:
            out.append(rec("steam_charts", t + 8, body={"response": {"ranks": [cycle]}}))
            out.append(rec("steam_store_new", t + 9, {"start": 0}, body={"results_html": str(cycle)}))
            out.append(rec("roblox_search", t + 10, {"searchQuery": "Valheim"}, body={"searchResults": [cycle]}))
            out.append(rec("steam_appdetails", t + 11, {"appids": 440}, body={"440": {"success": True}}))
    return out


def requests_at():
    yield "roblox_games", {"universeIds": "1,3,5,7"}
    yield "roblox_votes", {"universeIds": "2,4,6"}
    yield "steam_players", {"appid": 440}
    yield "steam_players", {"appid": 9}
    yield "steam_players", {"appid": 1}
    yield "steam_charts", {}
    yield "steam_store_new", {"start": 0}
    yield "roblox_search", {"searchQuery": "Valheim"}
    yield "roblox_search", {"searchQuery": "unknown"}
    yield "steam_appdetails", {"appids": 440}
    yield "roblox_sorts", {"country": "us"}


def test_engine_and_oracle_agree_on_every_answer(tmp_path):
    rng = random.Random(3)
    write(tmp_path, archive(rng))
    clock = SetClock(at(0))
    eng = Engine(tmp_path, clock, config.Settings(data_dir=tmp_path))
    oracle = Oracle(tmp_path, at(0), at(4000))
    t, checked, statuses = 0.0, 0, set()
    while t < 4000:  # past the archive end (15:00)
        t += rng.choice((0.3, 4, 11, 37))
        clock.set(t)
        for feed, params in requests_at():
            query = {k: [str(v)] for k, v in params.items()}
            while True:  # follow sorts pages
                resp = eng.answer(P[feed], query)
                found = oracle.problems(feed, query, clock.now(), resp.status, resp.body,
                                        resp.headers.get("X-Mock-Reason"))
                assert found == [], (t, feed, found)
                checked += 1
                statuses.add((feed, resp.status))
                token = isinstance(resp.body, dict) and resp.body.get("nextSortsPageToken")
                if feed != "roblox_sorts" or resp.status != 200 or not token:
                    break
                query = {"country": ["us"], "sortsPageToken": [token]}
    assert checked > 3000
    assert {("roblox_games", 429), ("steam_players", 504), ("steam_players", 404), ("roblox_sorts", 200),
            ("roblox_games", 503)} <= statuses  # error windows, Steam 404, sorts and end were all compared


def test_oracle_flags_a_wrong_answer(tmp_path):
    write(tmp_path, [games(10, {1: 100}), games(70, {1: 110})])
    oracle = Oracle(tmp_path, at(0), at(200))
    a = at(80)
    item = {"id": 1, "playing": 110, "name": "game 1", "_mock": {"observed_at": "2026-11-02T14:01:10.000Z"}}
    q = {"universeIds": ["1"]}
    assert oracle.problems("roblox_games", q, a, 200, {"data": [item]}, None) == []
    old = {**item, "playing": 100, "_mock": {"observed_at": "2026-11-02T14:00:10.000Z"}}
    assert oracle.problems("roblox_games", q, a, 200, {"data": [old]}, None)  # an older observation
    assert oracle.problems("roblox_games", q, at(60), 200, {"data": [item]}, None)  # one from the future
    assert oracle.problems("roblox_games", q, a, 200, {"data": []}, None)  # a missing one
    assert oracle.problems("roblox_games", q, a, 503, {}, "no-data")  # a wrong error
