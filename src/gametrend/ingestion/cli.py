"""Command line: python -m gametrend.ingestion run [--dry-run] [--speed N] ...

Settings also come from the environment (see .env.example); command-line options win.
"""

import argparse
import logging
import signal
import sys
import threading

from . import config
from .clock import SystemClock
from .http import HttpClient
from .runner import Producers
from .sink import KafkaSink, PrintSink
from .stats import Stats


def cmd_run(args) -> int:
    settings = config.settings_from_env(mode=args.mode, mock_url=args.mock_url, kafka_bootstrap=args.kafka,
                                        speed=args.speed)
    clock = SystemClock()
    stop = threading.Event()
    sink = PrintSink() if args.dry_run else KafkaSink(settings.kafka_bootstrap)
    producers = Producers(settings, HttpClient(settings, clock), sink, clock, Stats(), stop)

    def on_signal(signum, _frame):
        logging.getLogger(__name__).info("signal %s, stopping", signum)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):  # SIGTERM exists on Windows too; SIGHUP does not
        signal.signal(sig, on_signal)
    if not args.dry_run:
        logging.getLogger(__name__).info("Kafka at %s", settings.kafka_bootstrap)
    producers.run()
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m gametrend.ingestion")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="poll the APIs and send events to Kafka until stopped")
    r.add_argument("--mode", choices=("mock", "real"), help="default: $INGEST_MODE or mock")
    r.add_argument("--mock-url", help="default: $INGEST_MOCK_URL or http://127.0.0.1:8080")
    r.add_argument("--kafka", help="bootstrap servers, default: $KAFKA_BOOTSTRAP or 127.0.0.1:9094")
    r.add_argument("--speed", type=float,
                   help="divide every interval by this (match the mock's REPLAY_SPEED), default: $INGEST_SPEED or 1")
    r.add_argument("--dry-run", action="store_true", help="print events to stdout instead of sending them to Kafka")
    r.set_defaults(func=cmd_run)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s")
    return args.func(args)
