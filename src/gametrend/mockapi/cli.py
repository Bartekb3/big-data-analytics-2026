"""Command line: `serve` runs the mock API, `check` polls it like the producers would and reports."""

import argparse
import logging
import os
import signal
import sys
import threading

from ..collector.records import utc_iso
from . import config
from .archive import archive_bounds
from .clock import ReplayClock, parse_utc, wall_now
from .engine import Engine
from .server import make_server

log = logging.getLogger("gametrend.mockapi")


def cmd_serve(args) -> int:
    data_dir = config.data_dir_from_env(args.data_dir)
    settings = config.Settings(data_dir=data_dir, replay_errors=args.errors == "replay", inject_p=args.inject)
    first, end = archive_bounds(settings.raw_dir, config.FEEDS)
    if first is None:
        log.error("no archive files (*.jsonl.gz) under %s", settings.raw_dir)
        return 1
    a0 = parse_utc(args.a0) if args.a0 else first
    start = parse_utc(args.start) if args.start else wall_now()
    clock = ReplayClock(a0, start, args.speed)
    engine = Engine(settings.raw_dir, clock, settings)

    stop = threading.Event()
    server = make_server(engine, args.host, args.port, stop)

    def on_signal(signum, _frame):
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, on_signal)

    log.info("archive %s: %s .. %s", settings.raw_dir, utc_iso(first), utc_iso(end))
    log.info("replay clock: A0=%s START=%s speed=%s (pass the same --a0/--start to continue after a restart)",
             utc_iso(a0), utc_iso(start), args.speed)
    log.info("errors: %s, injection p=%s; listening on http://%s:%d", args.errors, args.inject, args.host, args.port)
    server.serve_forever()
    log.info("stopped at replay time %s (to resume later: --a0 %s with a new --start)",
             utc_iso(clock.now()), utc_iso(clock.now()))
    return 0


def cmd_check(args) -> int:
    from .check import run_check

    return run_check(args.base_url, args.duration, args.timeout, args.data_dir)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m gametrend.mockapi", description=__doc__)
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging (every request)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the mock API")
    s.add_argument("--data-dir", help="collector data dir (default: $MOCKAPI_DATA_DIR, $COLLECTOR_DATA_DIR)")
    s.add_argument("--host", default=os.environ.get("MOCKAPI_HOST", "127.0.0.1"))
    s.add_argument("--port", type=int, default=int(os.environ.get("MOCKAPI_PORT", "8080")))
    s.add_argument("--a0", default=os.environ.get("REPLAY_A0"), help="replay start in archive time (default: archive start)")
    s.add_argument("--start", default=os.environ.get("REPLAY_START"), help="wall-clock instant of A0 (default: now)")
    s.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", "1")))
    s.add_argument("--errors", choices=("replay", "off"), default=os.environ.get("MOCKAPI_ERRORS", "replay"))
    s.add_argument("--inject", type=float, default=float(os.environ.get("MOCKAPI_INJECT", "0")),
                   help="probability of an injected error per request")
    s.set_defaults(func=cmd_serve)

    c = sub.add_parser("check", help="poll a running mock like the producers would and report gaps and leads")
    c.add_argument("--base-url", default="http://127.0.0.1:8080")
    c.add_argument("--duration", type=float, default=300, help="seconds of wall time")
    c.add_argument("--timeout", type=float, default=30)
    c.add_argument("--data-dir", help="collector data dir: also compare every answer with the archive")
    c.set_defaults(func=cmd_check)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    return args.func(args)
