import logging
import threading

import pytest
import requests

from gametrend.collector.archive import Archive
from gametrend.collector.http import Fetcher, Pacer, Stopped, backoff_delay
from gametrend.collector.stats import read_lines

from tests.collector.fakes import FakeResponse, FakeSession


def test_pacer_spaces_request_starts(clock):
    p = Pacer(3.0, clock)
    p.wait()          # first request: no wait
    clock.advance(1)  # request took 1 s
    p.wait()          # waits the remaining 2 s
    p.wait()          # waits 3 s
    assert clock.sleeps == [2.0, 3.0]


def test_pacer_penalize_delays_next_request(clock):
    p = Pacer(1.0, clock)
    p.wait()
    p.penalize(10)
    p.wait()
    assert clock.sleeps == [10.0]


def test_backoff_grows_and_is_capped():
    assert [backoff_delay(a, rand=lambda: 0.0) for a in range(6)] == [2.5, 5.0, 10.0, 20.0, 40.0, 60.0]
    assert backoff_delay(10, rand=lambda: 0.999999) < 120.0


def make_fetcher(tmp_path, clock, responder, max_retries=4, stop=None):
    session = FakeSession(responder)
    archive = Archive(tmp_path)
    f = Fetcher(session, clock, archive, {"roblox_games": 3.0, "steam_api": 1.0},
                max_retries=max_retries, rand=lambda: 0.0, stop=stop)
    return f, session, archive


def archived(tmp_path, feed):
    return [r for p in sorted((tmp_path / feed).glob("date=*/*.jsonl")) for r in read_lines(p)]


def test_429_is_retried_with_backoff_and_every_attempt_archived(tmp_path, clock):
    answers = iter([FakeResponse("u", 429, {"errors": []}), FakeResponse("u", 200, {"data": []})])
    f, session, archive = make_fetcher(tmp_path, clock, lambda url, params: next(answers))
    rec = f.get("steam_players", "https://api.steampowered.com/x", {"appid": 730})
    archive.close()
    assert rec["status"] == 200
    assert len(session.calls) == 2
    assert clock.sleeps == [2.5]  # back-off of attempt 1 is longer than the 1 s pacer interval
    assert [r["status"] for r in archived(tmp_path, "steam_players")] == [429, 200]
    assert f.counts[("steam_players", 429)] == 1 and f.counts[("steam_players", 200)] == 1


def test_pacer_interval_wins_over_shorter_backoff(tmp_path, clock):
    answers = iter([FakeResponse("u", 429, {}), FakeResponse("u", 200, {"data": []})])
    f, _, archive = make_fetcher(tmp_path, clock, lambda url, params: next(answers))
    f.get("roblox_games", "https://games.roblox.com/v1/games", {"universeIds": "1"})
    archive.close()
    assert clock.sleeps == [3.0]


def test_404_is_final(tmp_path, clock):
    f, session, archive = make_fetcher(tmp_path, clock, lambda url, params: FakeResponse(url, 404, {"response": {"result": 42}}))
    rec = f.get("steam_players", "https://api/x", {"appid": 1422450})
    archive.close()
    assert rec["status"] == 404 and len(session.calls) == 1


def test_network_error_is_archived_and_retries_stop_at_max(tmp_path, clock):
    f, session, archive = make_fetcher(tmp_path, clock, lambda url, params: requests.ConnectTimeout("down"),
                                       max_retries=2)
    rec = f.get("steam_players", "https://api.steampowered.com/x", {"appid": 730})
    archive.close()
    assert len(session.calls) == 3
    assert "status" not in rec and rec["error"].startswith("ConnectTimeout")
    lines = archived(tmp_path, "steam_players")
    assert len(lines) == 3 and lines[0]["url"] == "https://api.steampowered.com/x?appid=730"


def warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_clean_200_logs_nothing(tmp_path, clock, caplog):
    f, _, archive = make_fetcher(tmp_path, clock, lambda url, params: FakeResponse(url, 200, {"data": []}))
    f.get("steam_players", "https://api/x", {"appid": 730})
    archive.close()
    assert warnings(caplog) == []


def test_final_4xx_is_logged_once(tmp_path, clock, caplog):
    f, _, archive = make_fetcher(tmp_path, clock,
                                 lambda url, params: FakeResponse("https://api/x?appid=1422450", 404, {}))
    f.get("steam_players", "https://api/x", {"appid": 1422450})
    archive.close()
    assert warnings(caplog) == ["steam_players: HTTP 404 (final) https://api/x?appid=1422450"]


def test_200_without_json_is_logged(tmp_path, clock, caplog):
    f, _, archive = make_fetcher(tmp_path, clock, lambda url, params: FakeResponse("u", 200, text="<html>"))
    f.get("steam_players", "https://api/x")
    archive.close()
    assert warnings(caplog) == ["steam_players: HTTP 200 without a JSON body (final) u"]


def test_recovered_429_logs_only_the_backoff(tmp_path, clock, caplog):
    answers = iter([FakeResponse("u", 429, {}), FakeResponse("u", 200, {"data": []})])
    f, _, archive = make_fetcher(tmp_path, clock, lambda url, params: next(answers))
    f.get("steam_players", "https://api/x")
    archive.close()
    assert warnings(caplog) == ["steam_players: HTTP 429 on attempt 1, backing off 2s"]


def test_giving_up_is_logged(tmp_path, clock, caplog):
    f, _, archive = make_fetcher(tmp_path, clock, lambda url, params: FakeResponse("u", 503, {}), max_retries=1)
    f.get("steam_players", "https://api/x")
    archive.close()
    assert warnings(caplog)[-1] == "steam_players: gave up after 2 attempts, last: HTTP 503 u"


def test_stop_during_backoff_sends_no_retry(tmp_path, clock, caplog):
    stop = threading.Event()

    def responder(url, params):
        stop.set()  # Ctrl+C arrives while the first request is in flight
        return FakeResponse("u", 429, {})

    f, session, archive = make_fetcher(tmp_path, clock, responder, stop=stop)
    rec = f.get("steam_players", "https://api/x")
    archive.close()
    assert rec["status"] == 429 and len(session.calls) == 1
    assert warnings(caplog) == ["steam_players: stopping, not retrying after HTTP 429 u"]


def test_stop_before_a_request_sends_nothing(tmp_path, clock):
    stop = threading.Event()
    stop.set()
    f, session, archive = make_fetcher(tmp_path, clock, lambda url, params: FakeResponse(url, 200, {}), stop=stop)
    with pytest.raises(Stopped):
        f.get("steam_players", "https://api/x")
    archive.close()
    assert session.calls == []
