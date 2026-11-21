"""The batch cycle: sink -> "now" -> hourly aggregates -> retention sweep, then a pause.

Failure policy (architecture/batch/README.md):
- the sink is isolated: if the aggregates fail, the sink still archives on the next cycle (Kafka keeps
  events only 72 h, aggregates can always be recomputed from Parquet);
- the aggregates and the sweep share one step: the sweep only runs after the aggregates succeeded. One
  failing hour fails the whole step, so the latest hourly aggregate does not move past it and no hole
  is left behind the recompute window;
- every failing cycle logs an ERROR with how many cycles in a row failed and since when;
- a dead SparkSession ends the process (non-zero exit), docker compose restarts it.

The orchestration takes its collaborators as arguments, so it is tested without Spark and MongoDB.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime

from ..common.retention import retention_cutoff
from .hourly import group_by_hour
from .hours import chunks, data_now, hours_to_compute

log = logging.getLogger(__name__)

HOURS_PER_JOB = 24  # hours per Spark job: bounds what is collected on the driver on a first run


@dataclass
class FailureStreak:
    """Consecutive failures of one step. `at` is the wall time of the cycle, used only for the log."""

    name: str
    count: int = 0
    since: datetime | None = None

    def failed(self, at: datetime) -> str:
        if self.count == 0:
            self.since = at
        self.count += 1
        return f"{self.name} failed {self.count} cycle(s) in a row, since {self.since.isoformat()}"

    def succeeded(self) -> str | None:
        """A message when a streak ends, else None."""
        if self.count == 0:
            return None
        msg = f"{self.name} recovered after {self.count} failed cycle(s) since {self.since.isoformat()}"
        self.count, self.since = 0, None
        return msg


@dataclass
class HourlyReport:
    now: datetime | None = None
    hours: list = field(default_factory=list)
    documents: int = 0
    cutoff: datetime | None = None
    sweep: object = None

    def format(self) -> str:
        if self.now is None:
            return "hourly: no data in the master dataset yet"
        span = f"{self.hours[0].isoformat()}..{self.hours[-1].isoformat()}" if self.hours else "none"
        sweep = self.sweep
        kept = ""
        if sweep is not None and sweep.kept_games:
            kept = f" kept_without_hourly={sweep.kept_games} (oldest hour {sweep.oldest_kept.isoformat()})"
        swept = f" swept_hours={sweep.hours} deleted={sweep.deleted}{kept}" if sweep is not None else ""
        return (f"hourly: now={self.now.isoformat()} hours={len(self.hours)} [{span}] documents={self.documents}"
                f" cutoff={self.cutoff.isoformat()}{swept}")


def hourly_step(master, store, recompute, retention) -> HourlyReport:
    """Aggregates of the closed hours in range, one transaction per hour, then the retention sweep.

    `master`: latest_ts() / earliest_ts() / compute(hours); `store`: last_hour() / write_hour(docs) /
    sweep(cutoff). Any exception propagates: nothing after a failing hour is written.
    """
    report = HourlyReport()
    report.now = data_now(master.latest_ts())
    if report.now is None:
        return report
    last_done = store.last_hour()
    first_ts = master.earliest_ts() if last_done is None else None
    report.hours = hours_to_compute(first_ts, last_done, report.now, recompute)
    for chunk in chunks(report.hours, HOURS_PER_JOB):
        by_hour = group_by_hour(master.compute(chunk))
        for hour in chunk:  # ascending: the latest written hour never passes a failed one
            docs = by_hour.get(hour, [])
            store.write_hour(docs)
            report.documents += len(docs)
    report.cutoff = retention_cutoff(report.now, retention)
    report.sweep = store.sweep(report.cutoff)
    return report


class SparkDead(Exception):
    pass


def run_cycle(sink, hourly, sink_streak: FailureStreak, hourly_streak: FailureStreak, clock, spark_alive,
              logger=log) -> None:
    """One cycle. `sink()` and `hourly()` return objects with `format()`; `clock()` gives the wall time
    for the failure log; `spark_alive()` is checked after a failure. Raises SparkDead to end the process."""
    for streak, step in ((sink_streak, sink), (hourly_streak, hourly)):
        try:
            report = step()
        except Exception:
            logger.error(streak.failed(clock()), exc_info=True)
            if not spark_alive():
                raise SparkDead(f"SparkSession is not usable after the {streak.name} failure")
            continue
        logger.info(report.format())
        recovered = streak.succeeded()
        if recovered:
            logger.info(recovered)


def run_forever(cycle, interval_s: float, sleep, cycles: int | None = None) -> None:
    """Run `cycle()` then pause `interval_s` seconds, forever (or `cycles` times, for tests)."""
    done = 0
    while cycles is None or done < cycles:
        cycle()
        done += 1
        if cycles is None or done < cycles:
            sleep(interval_s)
