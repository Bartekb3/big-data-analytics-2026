import random

import pytest

from gametrend.mockapi import config
from gametrend.mockapi.check import BATCH, MAX_SPACING, Stats, keep_order, spacing
from gametrend.mockapi.engine import Engine
from tests.mockapi.helpers import SetClock, at, games, players, write


def test_spacing_follows_the_recording_rate():
    assert spacing("roblox_games", 24) == 3  # the pacer: 60 s / 24 would be faster than the collector may go
    assert spacing("roblox_votes", 24) == 12.5  # 300 s / 24 batches
    assert spacing("steam_players", 178) == 300 / 178
    assert spacing("roblox_sorts", 3) == 300  # batch-like: no 15 s rule, one country every 5 min


def test_spacing_of_a_stream_source_stays_below_15_s_with_few_ids():
    for feed in ("roblox_games", "roblox_votes", "steam_players"):
        assert spacing(feed, 1) == MAX_SPACING < 15


def test_steam_is_not_polled_faster_than_its_pacer_with_many_ids():
    assert spacing("steam_players", 1000) == 1


@pytest.mark.parametrize("phase", (0.7, 1.5, 2.9))
def test_polling_at_the_recording_rate_gives_no_repeats_and_keeps_the_15_s_rule(tmp_path, phase):
    rng = random.Random(1)
    roblox = list(range(1000, 2155))  # 24 batches, as in the real ID list
    steam = list(range(1, 179))
    rounds = {"roblox_games": [roblox[i:i + BATCH] for i in range(0, len(roblox), BATCH)]}
    rounds["roblox_votes"] = rounds["roblox_games"]
    rounds["steam_players"] = steam
    records = []
    t = 0.0
    while t < 1800:  # recorded as the collector does: games back to back, the others in a burst every 5 min
        for batch in rounds["roblox_games"]:
            records.append(games(t + rng.gauss(0, 0.005), {i: 1 for i in batch}))
            t += 3
    for cycle in range(6):
        for k, batch in enumerate(rounds["roblox_votes"]):
            records.append(games(cycle * 300 + 1 + k, {i: 1 for i in batch}, feed="roblox_votes"))
        for k, appid in enumerate(steam):
            records.append(players(cycle * 300 + 2 + k, appid, 5))
    write(tmp_path, records)
    clock = SetClock(at(0))
    eng = Engine(tmp_path, clock, config.Settings(data_dir=tmp_path))

    start, end = 400 + phase, 1700
    for feed, requests in rounds.items():
        step = spacing(feed, len(requests))
        assert step < 15
        seen, repeats, n = set(), 0, 0
        while (now := start + n * step) < end:
            clock.set(now)
            request = requests[n % len(requests)]
            n += 1
            if feed == "steam_players":
                resp = eng.answer(config.FEED_PATH[feed], {"appid": [str(request)]})
                units = [(request, resp.body["_mock"]["observed_at"])]
            else:
                resp = eng.answer(config.FEED_PATH[feed], {"universeIds": [",".join(map(str, request))]})
                units = [(d["id"], d["_mock"]["observed_at"]) for d in resp.body["data"]]
            assert resp.status == 200
            repeats += sum(u in seen for u in units)
            seen.update(units)
        assert repeats == 0, feed
        eng = Engine(tmp_path, SetClock(at(0)), config.Settings(data_dir=tmp_path))  # the next feed starts over
        clock = eng.clock


def test_refresh_keeps_the_polling_order():
    assert keep_order([5, 1, 9], [1, 2, 5, 7]) == [5, 1, 2, 7]  # 9 left, 2 and 7 join at the end
    assert keep_order([], [3, 1]) == [3, 1]


def test_stats_count_repeats_lead_and_age():
    s = Stats()
    s.observe("roblox_games", 1, "2026-11-02T14:00:00.000Z", "2026-11-02T14:00:05.000Z")
    s.observe("roblox_games", 1, "2026-11-02T14:00:00.000Z", "2026-11-02T14:00:09.000Z")
    assert (s.new["roblox_games"], s.repeat["roblox_games"], s.max_lead["roblox_games"]) == (1, 1, -5.0)
    assert s.max_age["roblox_games"] == 9.0


def test_gaps_follow_the_start_times_whatever_order_parallel_requests_booked_them():
    s = Stats()
    s.times["roblox_votes"] += [12.5, 0.0, 25.0]
    assert s.gaps("roblox_votes") == [12.5, 12.5]
