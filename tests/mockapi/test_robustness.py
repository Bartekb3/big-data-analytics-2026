import gzip
import json
import threading
import time

import pytest

from gametrend.collector.archive import Archive
from gametrend.collector.records import make_record
from gametrend.mockapi.archive import FeedReader
from gametrend.mockapi.server import encode
from tests.mockapi.helpers import at, games, rec, sorts_cycle, write
from tests.mockapi.test_engine import ask, engine, q


def test_end_of_archive_is_left_when_a_new_hour_arrives(tmp_path):
    eng, clock = engine(tmp_path, [games(3500, {1: 1})])
    clock.set(3600)
    assert ask(eng, "roblox_games", universeIds="1").headers["X-Mock-Reason"] == "end"
    write(tmp_path, [games(3650, {1: 2})])
    clock.set(3665)  # one rescan interval later
    resp = ask(eng, "roblox_games", universeIds="1")
    assert resp.status == 200 and resp.body["data"][0]["playing"] == 2
    assert eng.answer("/_mock/clock", {}).body["ended"] is False


def test_overlapping_requests_never_answer_with_data_after_their_replay_time(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), games(20, {1: 2})])
    clock.set(15)
    out = {}
    eng._lock.acquire()  # another request is being answered
    waiting = threading.Thread(target=lambda: out.update(r=ask(eng, "roblox_games", universeIds="1")))
    waiting.start()
    time.sleep(0.1)
    clock.set(25)
    eng.advance(at(25))  # the request holding the lock is later and has moved the state on
    eng._lock.release()
    waiting.join()
    assert out["r"].body["data"][0]["_mock"]["observed_at"] <= out["r"].headers["X-Mock-Time"]


@pytest.mark.parametrize("feed,params", [
    ("roblox_games", {"universeIds": "1"}), ("roblox_votes", {"universeIds": "1"}), ("roblox_sorts", {"country": "us"}),
    ("steam_players", {"appid": 440}), ("steam_appdetails", {"appids": 440}), ("steam_charts", {}),
    ("steam_store_new", {"start": 0}), ("roblox_search", {"searchQuery": "x"}),
])
def test_recorded_200_with_json_that_is_not_an_object_is_served_as_recorded(tmp_path, feed, params):
    eng, clock = engine(tmp_path, [{**rec(feed, 10, params), "body": None}])
    clock.set(11)
    resp = ask(eng, feed, **params)
    assert (resp.status, resp.body) == (200, None)
    assert encode(resp)[0] == b"null"
    clock.set(10 + 121)  # after the error window's tail: no data, an ordinary answer
    assert ask(eng, feed, **params).status in (404, 503, 200)


def test_truncated_file_gives_what_it_has_is_counted_and_the_next_hour_is_read(tmp_path):
    write(tmp_path, [games(i, {1: i}) for i in range(0, 3000, 3)])
    write(tmp_path, [games(3610, {1: 9999})])
    path = tmp_path / "roblox_games" / "date=2026-11-02" / "14.jsonl.gz"
    path.write_bytes(path.read_bytes()[:len(path.read_bytes()) // 2])
    reader = FeedReader(tmp_path / "roblox_games")
    got = list(reader.take_until(at(3700)))
    assert 0 < len(got) < 1000 + 1
    assert got[-1]["body"]["data"][0]["playing"] == 9999
    assert reader.counts["broken_files"] == 1


def test_unreadable_first_file_does_not_stop_the_mock(tmp_path):
    write(tmp_path, [games(10, {1: 1})])
    write(tmp_path, [games(3610, {1: 2})])
    path = tmp_path / "roblox_games" / "date=2026-11-02" / "14.jsonl.gz"
    path.write_bytes(b"not gzip at all")
    eng, clock = engine(tmp_path, [], start=3620)
    assert ask(eng, "roblox_games", universeIds="1").body["data"][0]["playing"] == 2


def test_lone_surrogate_goes_through_the_collector_and_the_mock_unchanged(tmp_path):
    content = b'{"data":[{"id":1,"playing":1,"description":"broken \\ud83d emoji"}]}'
    record = make_record("roblox_games", "https://games.roblox.com/v1/games?universeIds=1", at(10), at(10), status=200,
                         headers={}, content=content)
    archive = Archive(tmp_path)
    archive.write("roblox_games", record, at(10))
    archive.close()
    plain = tmp_path / "roblox_games" / "date=2026-11-02" / "14.jsonl"
    with gzip.open(plain.with_name("14.jsonl.gz"), "wb") as fh:
        fh.write(plain.read_bytes())
    plain.unlink()
    eng, clock = engine(tmp_path, [], start=20)
    body, _ = encode(ask(eng, "roblox_games", universeIds="1"))
    assert json.loads(body)["data"][0]["description"] == "broken \ud83d emoji"


def test_requests_the_real_apis_reject(tmp_path):
    eng, clock = engine(tmp_path, [games(10, {1: 1}), games(10, {1: 1}, feed="roblox_votes")], start=11)
    ids = ",".join(map(str, range(1, 52)))
    assert ask(eng, "roblox_games", universeIds=ids).status == 400
    assert ask(eng, "roblox_votes", universeIds=ids).status == 200  # measured: the real votes endpoint accepts 51
    assert ask(eng, "roblox_games").status == 400
    assert ask(eng, "roblox_votes").status == 400


def test_blank_sorts_token_means_the_first_page_as_on_the_real_api(tmp_path):
    eng, clock = engine(tmp_path, sorts_cycle(10), start=20)
    plain = ask(eng, "roblox_sorts", country="us")
    assert eng.answer("/explore-api/v1/get-sorts", q(country="us", sortsPageToken="")).body == plain.body
