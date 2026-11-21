"""The batch cycle with fake collaborators (no Spark, no MongoDB)."""

from datetime import datetime, timedelta, timezone

import pytest

from gametrend.batch.loop import (HOURS_PER_JOB, FailureStreak, HourlyReport, SparkDead, hourly_step, run_cycle,
                                  run_forever)
from gametrend.batch.mongo import SweepReport


def t(day: int, hh: int, mm: int = 0) -> datetime:
    return datetime(2026, 11, day, hh, mm, tzinfo=timezone.utc)


K = timedelta(hours=2)
RETENTION = timedelta(hours=48)


class FakeMaster:
    def __init__(self, latest, earliest=None, fail_hour=None):
        self.latest, self.earliest, self.fail_hour = latest, earliest, fail_hour
        self.computed = []

    def latest_ts(self):
        return self.latest

    def earliest_ts(self):
        return self.earliest

    def compute(self, hours):
        self.computed.append(list(hours))
        if self.fail_hour in hours:
            raise ValueError("NaN in hour")
        return [{"platform": "roblox", "id": 1, "hour_ts": h} for h in hours if h.hour % 2 == 0]


class FakeStore:
    def __init__(self, last=None):
        self.last = last
        self.written = []
        self.cutoffs = []

    def last_hour(self):
        return self.last

    def write_hour(self, docs):
        self.written.append(docs)

    def sweep(self, cutoff):
        self.cutoffs.append(cutoff)
        return SweepReport(hours=1, deleted=3)


def test_first_run_computes_from_the_archive_start_and_sweeps_with_data_now():
    master, store = FakeMaster({"roblox": t(8, 13, 40), "steam": t(8, 13, 30)}, earliest=t(6, 10, 5)), FakeStore()
    report = hourly_step(master, store, K, RETENTION)
    assert report.now == t(8, 13, 40)
    assert report.hours[0] == t(6, 10) and report.hours[-1] == t(8, 12)
    assert [len(c) for c in master.computed] == [HOURS_PER_JOB, HOURS_PER_JOB, 3]
    assert len(store.written) == len(report.hours)  # one write (transaction) per hour, empty hours included
    assert report.documents == sum(len(d) for d in store.written)
    assert store.cutoffs == [t(6, 13)]  # data "now" minus 48 h, floored to the hour
    assert "hours=51" in report.format()


def test_later_run_recomputes_behind_the_latest_aggregate():
    master, store = FakeMaster({"roblox": t(8, 15, 1)}), FakeStore(last=t(8, 13))
    report = hourly_step(master, store, K, RETENTION)
    assert report.hours == [t(8, 11), t(8, 12), t(8, 13), t(8, 14)]


def test_failing_hour_fails_the_step_and_writes_nothing_after_it():
    master = FakeMaster({"roblox": t(8, 15, 1)}, fail_hour=t(8, 13))
    store = FakeStore(last=t(8, 12))
    with pytest.raises(ValueError):
        hourly_step(master, store, K, RETENTION)
    assert store.written == []  # the chunk containing 13:00 failed as a whole
    assert store.cutoffs == []  # no sweep after a failure


def test_no_data_does_nothing():
    master, store = FakeMaster({}), FakeStore()
    report = hourly_step(master, store, K, RETENTION)
    assert report.now is None and store.written == [] and store.cutoffs == []
    assert "no data" in report.format()


def test_failure_streak_counts_and_resets():
    s = FailureStreak("hourly aggregates")
    assert s.succeeded() is None
    assert s.failed(t(6, 10)) == "hourly aggregates failed 1 cycle(s) in a row, since 2026-11-06T10:00:00+00:00"
    assert "failed 2 cycle(s) in a row, since 2026-11-06T10:00:00" in s.failed(t(6, 11))
    assert "recovered after 2" in s.succeeded()
    assert s.count == 0 and s.since is None


class Log:
    def __init__(self):
        self.lines = []

    def info(self, msg):
        self.lines.append(("INFO", msg))

    def error(self, msg, exc_info=False):
        self.lines.append(("ERROR", msg))


class Ok:
    def __init__(self, text):
        self.text = text

    def format(self):
        return self.text


def boom():
    raise RuntimeError("boom")


def test_sink_failure_does_not_stop_the_hourly_step_and_vice_versa():
    log, calls = Log(), []
    sink_s, hourly_s = FailureStreak("sink"), FailureStreak("hourly aggregates")
    run_cycle(boom, lambda: calls.append("hourly") or Ok("hourly ok"), sink_s, hourly_s, lambda: t(6, 10),
              lambda: True, log)
    assert calls == ["hourly"]
    run_cycle(lambda: calls.append("sink") or Ok("sink ok"), boom, sink_s, hourly_s, lambda: t(6, 11),
              lambda: True, log)
    assert calls == ["hourly", "sink"]
    levels = [level for level, _ in log.lines]
    assert levels == ["ERROR", "INFO", "INFO", "INFO", "ERROR"]  # sink failed; hourly ok; sink ok + recovered; hourly failed


def test_every_failing_cycle_logs_an_error_with_the_streak():
    log = Log()
    sink_s, hourly_s = FailureStreak("sink"), FailureStreak("hourly aggregates")
    for hour in (10, 11, 12):
        run_cycle(lambda: Ok("sink ok"), boom, sink_s, hourly_s, lambda h=hour: t(6, h), lambda: True, log)
    errors = [msg for level, msg in log.lines if level == "ERROR"]
    assert len(errors) == 3
    assert "failed 3 cycle(s) in a row, since 2026-11-06T10:00:00" in errors[-1]


def test_dead_spark_ends_the_process():
    with pytest.raises(SparkDead):
        run_cycle(boom, lambda: Ok("x"), FailureStreak("sink"), FailureStreak("h"), lambda: t(6, 10),
                  lambda: False, Log())


def test_run_forever_pauses_between_cycles():
    calls, sleeps = [], []
    run_forever(lambda: calls.append(1), 60, sleeps.append, cycles=3)
    assert len(calls) == 3 and sleeps == [60, 60]


def test_report_format_mentions_kept_documents():
    r = HourlyReport(now=t(8, 13), hours=[t(8, 12)], documents=5, cutoff=t(6, 13),
                     sweep=SweepReport(hours=2, deleted=10, kept_games=1, oldest_kept=t(6, 9)))
    text = r.format()
    assert "deleted=10" in text and "kept_without_hourly=1" in text
