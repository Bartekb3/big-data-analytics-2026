import gzip
import logging
from datetime import timedelta
from pathlib import Path

import pytest

from gametrend.mockapi.archive import FeedReader, archive_bounds, hour_start
from gametrend.mockapi.clock import ReplayClock, parse_utc
from tests.mockapi.helpers import T0, at, games, write


def test_replay_clock_maps_wall_time_onto_archive_time():
    wall = [T0 + timedelta(days=5)]
    clock = ReplayClock(a0=T0, start=wall[0], speed=2.0, wall=lambda: wall[0])
    assert clock.now() == T0
    wall[0] += timedelta(seconds=30)
    assert clock.now() == at(60)
    with pytest.raises(ValueError):
        ReplayClock(T0, T0, speed=0)


def test_parse_utc_accepts_z_and_offsets():
    assert parse_utc("2026-11-02T14:00:00.000Z") == T0
    assert parse_utc("2026-11-02T15:00:00+01:00") == T0


def test_hour_start_of_archive_files():
    assert hour_start(Path("raw/f/date=2026-11-02/14.jsonl.gz")) == T0
    assert hour_start(Path("raw/f/date=2026-11-02/14.2.jsonl.gz")) == T0
    assert hour_start(Path("raw/f/date=2026-11-02/14.jsonl")) is None
    assert hour_start(Path("raw/f/2026-11-02/14.jsonl.gz")) is None


def test_files_of_one_hour_are_merged_in_time_order(tmp_path):
    write(tmp_path, [games(10, {1: 1}), games(30, {1: 3})])
    write(tmp_path, [games(20, {1: 2}), games(40, {1: 4})], name="14.1.jsonl.gz")
    reader = FeedReader(tmp_path / "roblox_games")
    got = [r["body"]["data"][0]["playing"] for r in reader.take_until(at(3600))]
    assert got == [1, 2, 3, 4]


def test_records_come_once_and_never_after_until(tmp_path):
    write(tmp_path, [games(s, {1: s}) for s in (10, 20, 30, 4000)])
    reader = FeedReader(tmp_path / "roblox_games")
    assert [r["_t"] for r in reader.take_until(at(15))] == [at(10)]
    assert [r["_t"] for r in reader.take_until(at(15))] == []
    assert [r["_t"] for r in reader.take_until(at(30))] == [at(20), at(30)]
    assert [r["_t"] for r in reader.take_until(at(5000))] == [at(4000)]  # next hour, known from the listing


def test_broken_lines_are_skipped_and_counted(tmp_path):
    write(tmp_path, [games(10, {1: 1})])
    path = tmp_path / "roblox_games" / "date=2026-11-02" / "14.jsonl.gz"
    with gzip.open(path, "at", encoding="utf-8") as fh:
        fh.write('{"feed": "roblox_games", "requested_at": "2026-11-02T14:\n')
    reader = FeedReader(tmp_path / "roblox_games")
    assert len(list(reader.take_until(at(3600)))) == 1
    assert reader.counts["broken_lines"] == 1


def test_warm_up_skips_hours_before_the_window(tmp_path):
    write(tmp_path, [games(10, {1: 1}), games(7300, {1: 2})])  # 14:00 and 16:01
    reader = FeedReader(tmp_path / "roblox_games", since=at(7200 - 600))
    assert [r["body"]["data"][0]["playing"] for r in reader.take_until(at(8000))] == [2]


def test_follow_mode_picks_up_new_hours(tmp_path):
    write(tmp_path, [games(10, {1: 1})])
    reader = FeedReader(tmp_path / "roblox_games", rescan_every=timedelta(seconds=60))
    assert len(list(reader.take_until(at(3700)))) == 1
    write(tmp_path, [games(3720, {1: 2})])  # the 15:00 hour is copied in later
    assert list(reader.take_until(at(3730))) == []  # listed 30 s ago: not rescanned yet
    assert len(list(reader.take_until(at(3770)))) == 1


def test_late_file_of_a_replayed_hour_is_ignored_with_a_warning(tmp_path, caplog):
    write(tmp_path, [games(10, {1: 1}), games(3700, {1: 2})])
    reader = FeedReader(tmp_path / "roblox_games", rescan_every=timedelta(0))
    assert len(list(reader.take_until(at(3600)))) == 1
    write(tmp_path, [games(20, {1: 9})], name="14.1.jsonl.gz")
    with caplog.at_level(logging.WARNING):
        got = list(reader.take_until(at(7200)))
    assert [r["body"]["data"][0]["playing"] for r in got] == [2]
    assert "14.1.jsonl.gz" in caplog.text


def test_archive_bounds(tmp_path):
    write(tmp_path, [games(10, {1: 1}), games(3700, {1: 2})])
    write(tmp_path, [games(5, {1: 1}, feed="roblox_votes")])
    assert archive_bounds(tmp_path, ["roblox_games", "roblox_votes", "steam_players"]) == (at(5), at(7200))
    assert archive_bounds(tmp_path, ["steam_players"]) == (None, None)
