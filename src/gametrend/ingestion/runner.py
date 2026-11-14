"""The producers service: one loop per source, all sharing the HTTP client, the sink and the counters.

Loops (architecture/ingestion/README.md and the polling plan in architecture/mockapi/README.md):
- IDs: every 60 s, the IDs to poll (`/_mock/ids` until `tracked_games` exists);
- stream sources (Roblox games, Roblox votes, Steam players): round-robin batches, one request every
  max(pace, min(cycle / N, 14 s)); each request starts on schedule, without waiting for the previous answer;
- sorts: every 15 min per country, all pages;
- Steam appdetails: once per new Steam game, until it gets an answer (own loop, every 60 s).
"""

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from . import config, events
from .clock import Clock
from .http import HttpClient, Result, backoff_delay
from .sink import Sink
from .state import MetaTracker, RepeatFilter
from .stats import Stats, format_counts
from .schedule import Round, spacing
from .validate import is_valid

log = logging.getLogger(__name__)

PLATFORM = {"roblox_games": "roblox", "roblox_votes": "roblox", "roblox_sorts": "roblox",
            "steam_players": "steam", "steam_appdetails": "steam"}
SORTS_PAGE_ATTEMPTS = 5


def utc_iso(ts) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Producers:
    def __init__(self, settings: config.Settings, client: HttpClient, sink: Sink, clock: Clock,
                 stats: Stats | None = None, stop: threading.Event | None = None):
        self.settings = settings
        self.client = client
        self.sink = sink
        self.clock = clock
        self.stats = stats or Stats()
        self.stop = stop or threading.Event()
        self.repeats = RepeatFilter()
        self.meta = MetaTracker()
        self.rounds = {feed: Round(config.BATCH_SIZE[feed]) for feed in config.STREAM_PLAN}
        self.details_pending: set[int] = set()
        self.details_done: set[int] = set()
        self._attempts: dict[str, int] = {}
        self._penalty_until: dict[str, float] = {}
        self._lock = threading.Lock()
        self._session_id = str(uuid.uuid4())  # Roblox website endpoints expect a random session id

    # ------------------------------------------------------------------ pure-ish processing

    def request_ts(self, res: Result) -> str | None:
        """Event time: the request time on the real APIs, `_mock.observed_at` (None here) in mock mode."""
        return None if self.settings.mode == "mock" else utc_iso(res.requested_at)

    def emit(self, feed: str, parsed: events.Parsed, check_repeats: bool = True) -> int:
        """Send the valid, new events of parsed observations; returns the number of events handed to the sink."""
        platform = PLATFORM[feed]
        topic = config.TOPIC[platform]
        self.stats.add(feed, "dropped", parsed.dropped)
        sent = 0
        for obs in parsed.observations:
            if check_repeats and not self.repeats.is_new(obs.key, obs.ts):
                continue  # repeated observation: no event, not counted
            for event in obs.events:
                if not is_valid(platform, event):
                    self.stats.add(feed, "dropped")
                    continue
                if event["kind"] == "meta" and not self.meta.changed(platform, event):
                    continue
                self.sink.send(topic, event, self._on_delivery(feed, platform, event))
                sent += 1
        return sent

    def _on_delivery(self, feed: str, platform: str, event: dict):
        def delivered(ok: bool) -> None:
            self.stats.add(feed, "sent" if ok else "failed")
            if not ok and event["kind"] == "meta":
                self.meta.forget(platform, event["id"])  # send it again with the next observation
        return delivered

    def _failed(self, feed: str, res: Result) -> None:
        self.stats.add(feed, "errors")
        with self._lock:
            attempt = self._attempts.get(feed, 0) + 1
            self._attempts[feed] = attempt
            delay = backoff_delay(attempt, self.settings.scaled(config.BACKOFF_BASE_S), self.settings.backoff_cap_s)
            self._penalty_until[feed] = self.clock.monotonic() + delay
        log.warning("%s: %s, backing off %.1fs", feed, res.error or f"HTTP {res.status}", delay)

    def _succeeded(self, feed: str) -> None:
        with self._lock:
            self._attempts[feed] = 0

    def _ended(self, res: Result) -> bool:
        if res.outcome == "end" and self.settings.mode == "mock":
            if not self.stop.is_set():
                log.info("the mock's archive is over, stopping")
            self.stop.set()
            return True
        return False

    # ------------------------------------------------------------------ one request each

    def refresh_ids(self) -> bool:
        """IDs to poll from the mock; updates the rounds and the Steam games waiting for details."""
        res = self.client.get_url(self.settings.mock_url.rstrip("/") + config.MOCK_IDS_PATH)
        if self._ended(res):
            return False
        if res.outcome != "ok":
            log.warning("ID refresh failed: %s", res.error or f"HTTP {res.status}")
            return False
        roblox = [i for i in res.body.get("roblox", []) if isinstance(i, int)]
        steam = [i for i in res.body.get("steam", []) if isinstance(i, int)]
        self.rounds["roblox_games"].refresh(roblox)
        self.rounds["roblox_votes"].refresh(roblox)
        self.rounds["steam_players"].refresh(steam)
        with self._lock:
            self.details_pending |= set(steam) - self.details_done
        return True

    def poll_batch(self, feed: str, ids: list[int]) -> None:
        """One request of a stream source and the events from its answer."""
        params = {"appid": ids[0]} if feed == "steam_players" else {"universeIds": ",".join(map(str, ids))}
        res = self.client.get(feed, params)
        self.stats.add(feed, "requests")
        if self._ended(res):
            return
        outcome = res.outcome
        if outcome == "error":
            self._failed(feed, res)
            return
        self._succeeded(feed)
        if outcome == "bad_body":
            self.stats.add(feed, "dropped")
        elif outcome == "ok":
            ts = self.request_ts(res)
            if feed == "roblox_games":
                parsed = events.parse_roblox_games(res.body, ts)
            elif feed == "roblox_votes":
                parsed = events.parse_roblox_votes(res.body, ts)
            else:
                parsed = events.parse_steam_players(res.body, ids[0], ts)
            self.emit(feed, parsed)
        # not_found (unknown ID, Steam game without stats): no event, not an error

    def sorts_cycle(self, country: str) -> None:
        """All pages of the explore sorts of one country; skipped when its first page was already sent."""
        feed = "roblox_sorts"
        token, page, attempts, restarted, first_ts = "", 0, 0, False, None
        parsed_pages: list[events.Parsed] = []
        while page < config.SORTS_MAX_PAGES and not self.stop.is_set():
            params = {"sessionId": self._session_id, "device": "computer", "country": country}
            if token:
                params["sortsPageToken"] = token
            res = self.client.get(feed, params)
            self.stats.add(feed, "requests")
            if self._ended(res):
                return
            outcome = res.outcome
            if outcome == "stale_token" and not restarted:
                token, page, restarted, first_ts, parsed_pages = "", 0, True, None, []
                continue
            if outcome != "ok":
                if outcome == "bad_body":
                    self.stats.add(feed, "dropped")
                else:
                    self.stats.add(feed, "errors")
                attempts += 1
                if attempts >= SORTS_PAGE_ATTEMPTS:
                    log.warning("sorts %s: giving up this cycle after %d failed attempts", country, attempts)
                    return
                self.clock.sleep(backoff_delay(attempts, self.settings.scaled(config.BACKOFF_BASE_S),
                                               self.settings.backoff_cap_s))
                continue
            attempts = 0
            parsed = events.parse_roblox_sorts_page(res.body, country, self.request_ts(res))
            if page == 0:
                first_ts = parsed.observations[0].ts if parsed.observations else None
                if first_ts is not None and self.repeats.seen((feed, country), first_ts):
                    return  # this cycle was already sent
            parsed_pages.append(parsed)
            page += 1
            token = res.body.get("nextSortsPageToken") or ""
            if not token:
                break
        for parsed in parsed_pages:
            self.emit(feed, parsed, check_repeats=False)
        if first_ts is not None:
            self.repeats.remember((feed, country), first_ts)

    def fetch_details(self) -> None:
        """appdetails for every Steam game still waiting; a 404 or an error leaves it for the next refresh."""
        feed = "steam_appdetails"
        with self._lock:
            pending = sorted(self.details_pending)
        for appid in pending:
            if self.stop.is_set():
                return
            res = self.client.get(feed, {"appids": appid})
            self.stats.add(feed, "requests")
            if self._ended(res):
                return
            outcome = res.outcome
            if outcome == "error":
                self.stats.add(feed, "errors")
                continue
            if outcome == "not_found":
                continue
            if outcome == "ok":
                self.emit(feed, events.parse_steam_appdetails(res.body, appid, self.request_ts(res)))
            else:
                self.stats.add(feed, "dropped")
            with self._lock:
                self.details_pending.discard(appid)
                self.details_done.add(appid)

    # ------------------------------------------------------------------ loops

    def _sleep_until(self, t: float) -> None:
        while not self.stop.is_set():
            left = t - self.clock.monotonic()
            if left <= 0:
                return
            self.clock.sleep(min(left, 0.5))

    def _every(self, interval: float, job) -> None:
        nxt = self.clock.monotonic()
        while not self.stop.is_set():
            self._sleep_until(nxt)
            if self.stop.is_set():
                return
            try:
                job()
            except Exception:
                log.exception("unexpected error in %s", getattr(job, "__name__", job))
            nxt = max(nxt + interval, self.clock.monotonic())

    def stream_loop(self, feed: str, pool: ThreadPoolExecutor) -> None:
        pace, cycle = (self.settings.scaled(x) for x in config.STREAM_PLAN[feed])
        rnd = self.rounds[feed]
        nxt = self.clock.monotonic()
        while not self.stop.is_set():
            self._sleep_until(max(nxt, self._penalty_until.get(feed, 0.0)))
            if self.stop.is_set():
                return
            batch = rnd.next_batch()
            if not batch:
                nxt = self.clock.monotonic() + self.settings.scaled(1.0)
                continue
            pool.submit(self._safe, self.poll_batch, feed, batch)
            gap = spacing(pace, cycle, rnd.requests_per_round, config.MAX_SPACING_S)
            # keep the schedule: a late wake-up (coarse sleep, e.g. ~15 ms on Windows) is made up by the next
            # requests; after a longer stall (over 1 s behind) start afresh instead of bursting
            nxt = max(nxt + gap, self.clock.monotonic() - 1.0)

    def _safe(self, fn, *args) -> None:
        try:
            fn(*args)
        except Exception:
            log.exception("unexpected error in %s%s", fn.__name__, args[:1])

    def _sorts_all(self) -> None:
        for country in self.settings.countries:
            self.sorts_cycle(country)

    def _log_stats(self) -> None:
        self.sink.poll()
        log.info("last %.0fs: %s", config.STATS_EVERY_S, format_counts(self.stats.take()))

    def run(self) -> None:
        """Run until stopped (Ctrl+C, SIGTERM, or the end of the mock's archive)."""
        if self.settings.mode != "mock":
            raise NotImplementedError("real mode needs the tracked_games set as its ID source, not built yet")
        log.info("mode %s, APIs at %s, speed x%g", self.settings.mode, self.settings.mock_url, self.settings.speed)
        threads = []
        with ThreadPoolExecutor(self.settings.workers, thread_name_prefix="req") as pool:
            def start(name, target, *args):
                t = threading.Thread(target=target, args=args, name=name, daemon=True)
                t.start()
                threads.append(t)

            refresh = self.settings.scaled(config.IDS_REFRESH_S)
            start("ids", self._every, refresh, self.refresh_ids)
            # own loop: a first pass over hundreds of new Steam games takes minutes at the store's pace and
            # must not hold up the ID refresh
            start("details", self._every, refresh, self.fetch_details)
            for feed in config.STREAM_PLAN:
                start(feed, self.stream_loop, feed, pool)
            start("sorts", self._every, self.settings.scaled(config.SORTS_INTERVAL_S), self._sorts_all)
            start("stats", self._every, config.STATS_EVERY_S, self._log_stats)
            try:
                while not self.stop.is_set():
                    self.stop.wait(0.5)
            finally:
                self.stop.set()
                for t in threads:
                    t.join(timeout=config.HTTP_TIMEOUT_S + 5)
        left = self.sink.flush(30)
        if left:
            log.warning("%d records still unsent at exit", left)
        log.info("stopped; last counts: %s", format_counts(self.stats.take()))
