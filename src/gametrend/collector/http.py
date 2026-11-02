"""HTTP side of the collector: pacing, back-off, and archiving of every attempt."""

import logging
import random
import threading
from collections import Counter

import requests

from .archive import Archive
from .clock import Clock
from .config import FEED_PACER, USER_AGENT
from .records import is_ok, is_retryable, make_record

log = logging.getLogger(__name__)


class Stopped(Exception):
    """Raised instead of sending a request once the collector is stopping."""


class Pacer:
    """Keeps at least `interval` seconds between request starts that share this pacer (thread-safe)."""

    def __init__(self, interval: float, clock: Clock):
        self.interval = interval
        self._clock = clock
        self._next = float("-inf")
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock.monotonic()
            if self._next > now:
                self._clock.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval

    def penalize(self, seconds: float) -> None:
        """Push the next allowed request at least `seconds` into the future (after 429 / errors)."""
        with self._lock:
            self._next = max(self._next, self._clock.monotonic() + seconds)


def backoff_delay(attempt: int, base: float = 5.0, cap: float = 120.0, rand=random.random) -> float:
    """Exponential back-off with jitter: base * 2^attempt, capped, scaled by a random factor in [0.5, 1)."""
    return min(cap, base * 2**attempt) * (0.5 + rand() / 2)


def describe(rec: dict) -> str:
    """Short outcome of a record for log lines."""
    if "status" not in rec:
        return rec.get("error", "no response")
    if rec["status"] == 200 and "body" not in rec:
        return "HTTP 200 without a JSON body"
    return f"HTTP {rec['status']}"


class Fetcher:
    """Paced GETs; every attempt (including 429s and network errors) is written to the archive.

    Every response that does not end as a clean 200 with JSON is logged as a WARNING. Once `stop`
    is set, no new request or retry is sent.
    """

    def __init__(self, session, clock: Clock, archive: Archive, pacer_interval: dict[str, float],
                 max_retries: int = 6, timeout_s: float = 30.0, rand=random.random,
                 stop: threading.Event | None = None):
        self._session = session
        self._clock = clock
        self._archive = archive
        self._pacers = {key: Pacer(iv, clock) for key, iv in pacer_interval.items()}
        self._max_retries = max_retries
        self._timeout_s = timeout_s
        self._rand = rand
        self._stop = stop
        self.counts: Counter = Counter()  # (feed, outcome) -> n

    def get(self, feed: str, url: str, params: dict | None = None) -> dict:
        """Return the record of the last attempt (check it with records.is_ok).

        Raises Stopped if the collector is stopping before the first attempt is sent.
        """
        pacer = self._pacers[FEED_PACER[feed]]
        attempt = 0
        while True:
            pacer.wait()
            if self._stopping():
                raise Stopped(feed)
            rec = self._once(feed, url, params)
            self.counts[(feed, rec.get("status", "error"))] += 1
            if not is_retryable(rec):
                if not is_ok(rec):
                    log.warning("%s: %s (final) %s", feed, describe(rec), rec["url"])
                return rec
            if attempt == self._max_retries:
                log.warning("%s: gave up after %d attempts, last: %s %s", feed, attempt + 1, describe(rec), rec["url"])
                return rec
            if self._stopping():
                log.warning("%s: stopping, not retrying after %s %s", feed, describe(rec), rec["url"])
                return rec
            delay = backoff_delay(attempt, rand=self._rand)
            log.warning("%s: %s on attempt %d, backing off %.0fs", feed, describe(rec), attempt + 1, delay)
            pacer.penalize(delay)
            attempt += 1

    def _stopping(self) -> bool:
        return self._stop is not None and self._stop.is_set()

    def _once(self, feed: str, url: str, params: dict | None) -> dict:
        requested_at = self._clock.now()
        try:
            r = self._session.get(url, params=params, timeout=self._timeout_s)
            rec = make_record(feed, r.url, requested_at, self._clock.now(),
                              status=r.status_code, headers=r.headers, content=r.content)
        except requests.RequestException as e:
            full_url = requests.Request("GET", url, params=params).prepare().url
            rec = make_record(feed, full_url, requested_at, self._clock.now(), error=f"{type(e).__name__}: {e}")
        self._archive.write(feed, rec, requested_at)
        return rec


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s
