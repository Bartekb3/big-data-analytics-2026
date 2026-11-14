"""HTTP side of the producers: shared rate limiter per API group, back-off delays and one GET call.

Rules: architecture/ingestion/README.md (Design / rules, Mock and real APIs).
"""

import random
import threading
from dataclasses import dataclass, field
from datetime import datetime

import requests

from . import config
from .clock import Clock


class RateLimiter:
    """Minimum time between request starts, shared by every thread of one API group."""

    def __init__(self, interval: float, clock: Clock):
        self.interval = interval
        self.clock = clock
        self._next: float | None = None
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Reserve the next free slot and wait for it."""
        with self._lock:
            now = self.clock.monotonic()
            start = now if self._next is None else max(now, self._next)
            self._next = start + self.interval
        self.clock.sleep(start - now)


def backoff_delay(attempt: int, base: float, cap: float, rand=random.random) -> float:
    """Exponential back-off with jitter: about base * 2^(attempt-1), between half and all of it, at most cap."""
    if attempt <= 0:
        return 0.0
    return min(cap, base * 2 ** (attempt - 1)) * (0.5 + 0.5 * rand())


@dataclass
class Result:
    status: int | None  # None: no answer (timeout, connection error)
    body: object = None  # parsed JSON, or None when the body is not JSON
    headers: dict = field(default_factory=dict)
    error: str | None = None
    requested_at: datetime | None = None

    @property
    def outcome(self) -> str:
        return classify(self)


def classify(res: Result) -> str:
    """What the producer does with an answer (architecture/mockapi/README.md, Answers and what to do).

    ok           200 with a JSON object: parse it
    bad_body     200 whose body is not a JSON object: count one dropped record
    not_found    404 (unknown ID, Steam game without stats): no event, not an error
    end          503 with X-Mock-Reason: end (the mock's archive is over)
    stale_token  400 with X-Mock-Reason: unknown-token (sorts: start again from the first page)
    error        everything else (429, 5xx, other 4xx, timeout, network error): count it, back off
    """
    reason = res.headers.get("X-Mock-Reason") or res.headers.get("x-mock-reason")
    if res.status == 200:
        return "ok" if isinstance(res.body, dict) else "bad_body"
    if res.status == 404:
        return "not_found"
    if res.status == 503 and reason == "end":
        return "end"
    if res.status == 400 and reason == "unknown-token":
        return "stale_token"
    return "error"


class HttpClient:
    """GET with the group's rate limiter, a 10 s timeout and one requests.Session per thread."""

    def __init__(self, settings: config.Settings, clock: Clock, session_factory=None):
        self.settings = settings
        self.clock = clock
        self._session_factory = session_factory or new_session
        self._local = threading.local()
        self.limiters = {key: RateLimiter(settings.scaled(sec), clock) for key, sec in config.LIMITER_INTERVAL.items()}

    def _session(self):
        if not hasattr(self._local, "session"):
            self._local.session = self._session_factory()
        return self._local.session

    def get(self, feed: str, params: dict | None = None) -> Result:
        self.limiters[config.FEED_LIMITER[feed]].acquire()
        return self.get_url(self.settings.url(feed), params)

    def get_url(self, url: str, params: dict | None = None) -> Result:
        requested_at = self.clock.now()
        try:
            r = self._session().get(url, params=params, timeout=config.HTTP_TIMEOUT_S)
        except requests.RequestException as e:
            return Result(None, error=f"{type(e).__name__}: {e}", requested_at=requested_at)
        try:
            body = r.json()
        except ValueError:
            body = None
        return Result(r.status_code, body, dict(r.headers), requested_at=requested_at)


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = config.USER_AGENT
    adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=8)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s
