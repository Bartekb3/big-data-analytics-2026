import gzip
import json
import threading
from datetime import datetime, timezone

from gametrend.collector import archive as archive_module
from gametrend.collector.archive import Archive, compress, gz_target, hour_path

H13 = datetime(2026, 11, 2, 13, 59, 59, tzinfo=timezone.utc)
H14 = datetime(2026, 11, 2, 14, 0, 1, tzinfo=timezone.utc)


def read_gz(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def test_hour_path_uses_utc_date_and_hour(tmp_path):
    assert hour_path(tmp_path, "roblox_games", H13) == tmp_path / "roblox_games" / "date=2026-11-02" / "13.jsonl"


def test_write_appends_lines_and_rotates_closed_hour_to_gzip(tmp_path):
    a = Archive(tmp_path)
    a.write("roblox_games", {"n": 1}, H13)
    a.write("roblox_games", {"n": 2}, H13)
    plain13 = hour_path(tmp_path, "roblox_games", H13)
    assert [json.loads(line) for line in plain13.read_text().splitlines()] == [{"n": 1}, {"n": 2}]

    a.write("roblox_games", {"n": 3}, H14)  # new hour -> previous hour compressed
    assert not plain13.exists()
    assert read_gz(plain13.with_name("13.jsonl.gz")) == [{"n": 1}, {"n": 2}]
    assert hour_path(tmp_path, "roblox_games", H14).read_text().strip() == '{"n":3}'
    a.close()


def test_feeds_are_separate_files(tmp_path):
    a = Archive(tmp_path)
    a.write("steam_players", {"x": 1}, H13)
    a.write("roblox_votes", {"y": 1}, H13)
    a.close()
    assert hour_path(tmp_path, "steam_players", H13).exists()
    assert hour_path(tmp_path, "roblox_votes", H13).exists()


def test_compress_stale_after_restart_skips_other_feeds_and_drops_empty(tmp_path):
    a = Archive(tmp_path)
    a.write("roblox_games", {"n": 1}, H13)
    a.write("steam_players", {"n": 1}, H13)
    a.close()  # simulated crash/stop: files stay plain
    empty = hour_path(tmp_path, "roblox_games", H14)
    empty.touch()

    b = Archive(tmp_path)
    b.compress_stale(["roblox_games"])
    assert read_gz(tmp_path / "roblox_games" / "date=2026-11-02" / "13.jsonl.gz") == [{"n": 1}]
    assert not empty.exists()
    assert hour_path(tmp_path, "steam_players", H13).exists()  # not ours -> untouched


def test_gz_target_never_overwrites(tmp_path):
    plain = tmp_path / "13.jsonl"
    plain.write_text('{"a":1}\n')
    (tmp_path / "13.jsonl.gz").write_bytes(b"old")
    assert gz_target(plain) == tmp_path / "13.1.jsonl.gz"
    assert compress(plain) == tmp_path / "13.1.jsonl.gz"
    assert (tmp_path / "13.jsonl.gz").read_bytes() == b"old"


def test_compression_is_lossless_gzip_level_6(tmp_path):
    plain = tmp_path / "13.jsonl"
    content = "".join(json.dumps({"n": i, "name": "Grow a Garden \U0001f336"}, ensure_ascii=False) + "\n"
                      for i in range(1000)).encode()
    plain.write_bytes(content)
    target = compress(plain)
    assert gzip.decompress(target.read_bytes()) == content
    assert archive_module.GZIP_LEVEL == 6


def test_compressing_one_feed_does_not_block_other_feeds(tmp_path, monkeypatch):
    started, release = threading.Event(), threading.Event()
    real_compress = archive_module.compress

    def slow_compress(path):
        started.set()
        release.wait(5)
        return real_compress(path)

    monkeypatch.setattr(archive_module, "compress", slow_compress)
    a = Archive(tmp_path)
    a.write("roblox_games", {"n": 1}, H13)
    rotating = threading.Thread(target=a.write, args=("roblox_games", {"n": 2}, H14))
    rotating.start()
    assert started.wait(5)                     # roblox_games is now compressing hour 13

    other = threading.Thread(target=a.write, args=("steam_players", {"n": 1}, H14))
    other.start()
    other.join(2)
    assert not other.is_alive()                # another feed wrote while compression was running

    release.set()
    rotating.join(5)
    a.close()
    assert read_gz(tmp_path / "roblox_games" / "date=2026-11-02" / "13.jsonl.gz") == [{"n": 1}]
