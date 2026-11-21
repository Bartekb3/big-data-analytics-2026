from datetime import datetime, timezone

import pytest

from gametrend.collector.archive import Archive
from gametrend.collector import feeds
from gametrend.collector.cli import expand_jobs, next_wait, run_job
from gametrend.collector.http import Stopped
from gametrend.collector.config import JOBS
from gametrend.collector.stats import archive_summary, summarize_records


def test_summarize_records_counts_outcomes_and_time_span():
    recs = [{"requested_at": "2026-11-02T12:00:03.000Z", "status": 200},
            {"requested_at": "2026-11-02T12:00:00.000Z", "status": 429},
            {"requested_at": "2026-11-02T12:00:06.000Z", "status": 404},
            {"requested_at": "2026-11-02T12:00:09.000Z", "error": "ReadTimeout"}]
    s = summarize_records(recs)
    assert (s["calls"], s["ok"], s["http_429"], s["other_status"], s["errors"]) == (4, 1, 1, 1, 1)
    assert (s["first"], s["last"]) == ("2026-11-02T12:00:00.000Z", "2026-11-02T12:00:09.000Z")


def test_archive_summary_reads_plain_and_gzip_and_ignores_partial_line(tmp_path):
    a = Archive(tmp_path)
    a.write("roblox_games", {"requested_at": "2026-11-02T12:59:00.000Z", "status": 200},
            datetime(2026, 11, 2, 12, 59, tzinfo=timezone.utc))
    a.write("roblox_games", {"requested_at": "2026-11-02T13:00:00.000Z", "status": 200},
            datetime(2026, 11, 2, 13, 0, tzinfo=timezone.utc))
    a.close()
    with open(tmp_path / "roblox_games" / "date=2026-11-02" / "13.jsonl", "a") as fh:
        fh.write('{"requested_at": "2026-11-02T13:0')  # crash mid-line
    [row] = archive_summary(tmp_path)
    assert (row["feed"], row["date"], row["calls"], row["ok"]) == ("roblox_games", "2026-11-02", 2, 2)


def test_expand_jobs_groups_and_dedup():
    assert expand_jobs(["discovery", "roblox-sorts"]) == ["roblox-sorts", "steam-lists", "roblox-search"]
    assert expand_jobs(["steam-players"]) == ["steam-players"]
    with pytest.raises(ValueError):
        expand_jobs(["nope"])


def test_next_wait_uses_interval_or_quick_retry_when_nothing_found():
    search = JOBS["roblox-search"]
    assert next_wait(search, 40, 50.0) == search.interval_s - 50.0
    assert next_wait(search, 0, 1.0) == search.retry_empty_s - 1.0
    assert next_wait(JOBS["roblox-games"], 2000, 200.0) == 0.0  # cycle longer than interval: start at once


def test_run_job_exits_quietly_when_stopped(monkeypatch, clock, stop, caplog):
    calls = []

    def cycle(ctx):
        calls.append(1)
        raise Stopped("roblox_sorts")

    monkeypatch.setitem(feeds.CYCLES, "roblox-sorts", cycle)

    class Counts:
        counts = {}

    ctx = feeds.Ctx(settings=None, fetcher=Counts(), ids=None, clock=clock, stop=stop)
    run_job("roblox-sorts", ctx, once=False)  # would loop forever if Stopped were not handled
    assert calls == [1]
    assert not [r for r in caplog.records if r.levelname == "ERROR"]


def test_stop_signals_exist_on_this_platform():
    import signal

    from gametrend.collector.cli import stop_signals

    sigs = stop_signals()
    assert signal.SIGINT in sigs and signal.SIGTERM in sigs
    assert (getattr(signal, "SIGHUP", None) in sigs) == hasattr(signal, "SIGHUP")  # absent on Windows
