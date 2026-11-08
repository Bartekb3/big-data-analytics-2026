"""Command line: python -m gametrend.collector run <job|group>... | stats

Each job runs in its own thread with its own cadence. Run discovery and snapshots as separate
processes if you want them fully independent; never run the same job twice at the same time.
Ctrl+C, kill (SIGTERM) and closing the terminal window (SIGHUP) all stop cleanly.
"""

import argparse
import logging
import signal
import sys
import threading

from . import config, feeds
from .archive import Archive
from .clock import RealClock
from .http import Fetcher, Stopped, new_session
from .idlist import IdStore
from .stats import archive_summary

log = logging.getLogger("gametrend.collector")


def expand_jobs(names: list[str]) -> list[str]:
    """Job and group names -> unique job names in order."""
    out: dict[str, None] = {}
    for name in names:
        if name in config.JOB_GROUPS:
            out.update(dict.fromkeys(config.JOB_GROUPS[name]))
        elif name in config.JOBS:
            out[name] = None
        else:
            raise ValueError(f"unknown job or group: {name}")
    return list(out)


def next_wait(job: config.Job, n: int, elapsed_s: float) -> float:
    """Seconds until the next cycle: the job interval, or retry_empty_s if the cycle found nothing."""
    interval = job.retry_empty_s if n == 0 else job.interval_s
    return max(0.0, interval - elapsed_s)


def run_job(name: str, ctx: feeds.Ctx, once: bool) -> None:
    job = config.JOBS[name]
    cycle = feeds.CYCLES[name]
    while not ctx.stop.is_set():
        started = ctx.clock.monotonic()
        before = dict(ctx.fetcher.counts)
        n = 0
        try:
            n = cycle(ctx)
            calls = {k: v - before.get(k, 0) for k, v in list(ctx.fetcher.counts.items())
                     if k[0] in job.feeds and v != before.get(k, 0)}
            log.info("%s: cycle done in %.0fs, %d games, calls %s", name, ctx.clock.monotonic() - started, n,
                     {f"{f}:{s}": c for (f, s), c in calls.items()})
        except Stopped:
            return
        except Exception:
            log.exception("%s: cycle failed", name)
        if once:
            return
        ctx.clock.sleep(next_wait(job, n, ctx.clock.monotonic() - started))


def cmd_run(args) -> int:
    jobs = expand_jobs(args.jobs)
    settings = config.settings_from_env(args.data_dir)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):  # Ctrl+C, kill, terminal window closed
        signal.signal(sig, lambda *_: stop.set())

    clock = RealClock(stop)
    archive = Archive(settings.raw_dir)
    my_feeds = [f for j in jobs for f in config.JOBS[j].feeds]
    archive.compress_stale(my_feeds)
    fetcher = Fetcher(new_session(), clock, archive, settings.pacer_interval,
                      max_retries=settings.max_retries, timeout_s=settings.timeout_s, stop=stop)
    ctx = feeds.Ctx(settings, fetcher, IdStore(settings.ids_path), clock, stop)
    log.info("data dir %s, jobs %s", settings.data_dir, ", ".join(jobs))

    threads = [threading.Thread(target=run_job, args=(j, ctx, args.once), name=j, daemon=True) for j in jobs]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=0.5)
    archive.close()
    log.info("stopped")
    return 0


def cmd_stats(args) -> int:
    settings = config.settings_from_env(args.data_dir)
    rows = archive_summary(settings.raw_dir)
    if not rows:
        print(f"no data in {settings.raw_dir}")
        return 0
    cols = ["feed", "date", "calls", "ok", "http_429", "other_status", "errors", "first", "last", "mb_on_disk"]
    print("\t".join(cols))
    for r in rows:
        print("\t".join(str(r[c]) for c in cols))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m gametrend.collector", description=__doc__.splitlines()[0])
    p.add_argument("--data-dir", help="default: $COLLECTOR_DATA_DIR or ~/gametrend-data")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run jobs until Ctrl+C")
    r.add_argument("jobs", nargs="+", help=f"jobs {list(config.JOBS)} or groups {list(config.JOB_GROUPS)}")
    r.add_argument("--once", action="store_true", help="run one cycle of each job, then exit")
    sub.add_parser("stats", help="summary of the raw archive per feed and day")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    try:
        return cmd_run(args) if args.cmd == "run" else cmd_stats(args)
    except ValueError as e:
        p.error(str(e))


if __name__ == "__main__":
    sys.exit(main())
