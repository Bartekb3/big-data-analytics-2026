"""Answers mock requests from the archive at replay time A (behaviour in architecture/mockapi/README.md).

Only records with requested_at <= A are ever applied, so no response contains data from after A,
however often it is called. Bodies of API items are passed unchanged; the engine only regroups them,
replaces sorts page tokens and adds `_mock.observed_at`.
"""

import random
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from ..collector.records import is_ok, utc_iso
from . import config
from .archive import FeedReader, archive_bounds

MOCK_PATHS = ("/_mock/clock", "/_mock/ids")


@dataclass
class Response:
    status: int
    body: object = None  # JSON-serialisable object, or str for a text body
    headers: dict = field(default_factory=dict)
    delay_s: float = 0.0  # injected "timeout": wait this long before answering


def query_of(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def first(query: dict[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    return values[0] if values else None


def with_mock(obj: dict, observed_at: str) -> dict:
    """Copy of an API object with `_mock.observed_at` added; nothing else changes."""
    return {**obj, "_mock": {"observed_at": observed_at}}


def with_mock_per_app(body: dict, observed_at: str) -> dict:
    """appdetails: `_mock` inside each app's object, so the top-level keys stay appids as in the real API."""
    return {k: with_mock(v, observed_at) if isinstance(v, dict) else v for k, v in body.items()}


def usable(feed: str, rec: dict) -> bool:
    """A recorded attempt that carries data (else it belongs to an error window)."""
    if not isinstance(rec.get("body"), dict):
        return False  # text or JSON that is not an object (e.g. `null`): served as recorded, as an error window
    return is_ok(rec) or (feed == "steam_players" and rec.get("status") == 404)  # result 42: no stats


def mock_error(status: int, reason: str, message: str | None = None) -> Response:
    return Response(status, {"mock_error": message or reason}, {"X-Mock-Reason": reason})


# ------------------------------------------------------------------ per-feed state


class Latest:
    """Latest usable observation per key: key -> (value, time, requested_at string)."""

    def __init__(self):
        self.items: dict = {}

    def put(self, key, value, t: datetime, observed_at: str) -> None:
        self.items[key] = (value, t, observed_at)

    def get(self, key, now: datetime, limit: timedelta | None):
        hit = self.items.get(key)
        if hit is None or (limit is not None and now - hit[1] > limit):
            return None
        return hit

    def fresh_keys(self, now: datetime, limit: timedelta | None) -> set:
        return {k for k, (_, t, _) in self.items.items() if limit is None or now - t <= limit}


def add_items(state: Latest, rec: dict) -> None:
    """Roblox games / votes: one entry per item of data[] (by its id)."""
    for item in rec["body"].get("data") or []:
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            state.put(item["id"], item, rec["_t"], rec["requested_at"])


def add_by_param(param: str, as_int: bool = True):
    """Whole record keyed by a query parameter of the recorded URL (Steam appid, store page, search query)."""

    def add(state: Latest, rec: dict) -> None:
        value = first(query_of(rec["url"]), param)
        if value is None and param == "start":
            value = "0"
        if value is None:
            return
        if as_int:
            try:
                value = int(value)
            except ValueError:
                return
        state.put(value, rec, rec["_t"], rec["requested_at"])

    return add


def add_whole(state: Latest, rec: dict) -> None:
    state.put(None, rec, rec["_t"], rec["requested_at"])


class SortsState:
    """Complete explore-sorts cycles per country. A page requested without sortsPageToken starts a cycle."""

    def __init__(self, kept: int = config.SORT_CYCLES_KEPT, max_pages: int = config.MAX_SORT_PAGES):
        self.max_pages = max_pages
        self.building: dict[str, list] = {}  # country -> [(body, t, observed_at), ...]
        self.complete: dict[str, deque] = {}  # country -> deque of page lists, newest last
        self._kept = kept

    def add(self, rec: dict) -> None:
        query = query_of(rec["url"])
        country = first(query, "country")
        if country is None:
            return
        if first(query, "sortsPageToken") is None:
            self._close(country)
            self.building[country] = []
        elif country not in self.building:
            return  # rest of a cycle whose first page is outside the warm-up window
        pages = self.building[country]
        pages.append((rec["body"], rec["_t"], rec["requested_at"]))
        if not rec["body"].get("nextSortsPageToken") or len(pages) >= self.max_pages:
            self._close(country)

    def _close(self, country: str) -> None:
        pages = self.building.pop(country, None)
        if pages:
            self.complete.setdefault(country, deque(maxlen=self._kept)).append(pages)

    @staticmethod
    def token(pages: list, n: int) -> str:
        return f"{int(pages[0][1].timestamp() * 1000)}-p{n}"

    def latest_time(self) -> datetime | None:
        times = [c[-1][-1][1] for c in self.complete.values() if c]
        return max(times) if times else None


# ------------------------------------------------------------------ engine


class Engine:
    def __init__(self, raw_dir: Path, clock, settings: config.Settings | None = None, rng: random.Random | None = None):
        self.raw_dir = raw_dir
        self.clock = clock
        self.settings = settings or config.Settings(data_dir=raw_dir.parent)
        self.rng = rng or random.Random()
        self._lock = threading.Lock()
        start = clock.now()
        self.readers = {
            feed: FeedReader(raw_dir / feed, since=start - limit if limit is not None else None,
                             rescan_every=config.RESCAN_EVERY)
            for feed, limit in config.FRESHNESS.items()
        }
        self.state = {
            "roblox_games": Latest(), "roblox_votes": Latest(), "steam_players": Latest(),
            "steam_appdetails": Latest(), "steam_charts": Latest(), "steam_store_new": Latest(),
            "roblox_search": Latest(), "roblox_sorts": SortsState(),
        }
        self._adders = {
            "roblox_games": add_items, "roblox_votes": add_items,
            "steam_players": add_by_param("appid"), "steam_appdetails": add_by_param("appids"),
            "steam_charts": add_whole, "steam_store_new": add_by_param("start", as_int=False),
            "roblox_search": add_by_param("searchQuery", as_int=False),
        }
        self.latest: dict[str, datetime] = {}  # feed -> time of the latest usable record
        self.window: dict[str, dict] = {}  # feed -> latest failed attempt of an open error window
        self.archive_start, self.archive_end = archive_bounds(raw_dir, config.FEEDS)
        self._bounds_at = start

    # ---- archive -> state

    def _rescan_bounds(self, now: datetime) -> None:
        if now - self._bounds_at >= config.RESCAN_EVERY:
            self.archive_start, self.archive_end = archive_bounds(self.raw_dir, config.FEEDS)
            self._bounds_at = now

    def advance(self, now: datetime) -> None:
        for feed, reader in self.readers.items():
            for rec in reader.take_until(now):
                self._apply(feed, rec)

    def _apply(self, feed: str, rec: dict) -> None:
        if not usable(feed, rec):
            self.window[feed] = rec
            return
        self.window.pop(feed, None)
        self.latest[feed] = rec["_t"]
        if feed == "roblox_sorts":
            self.state[feed].add(rec)
        else:
            self._adders[feed](self.state[feed], rec)

    # ---- requests

    def answer(self, path: str, query: dict[str, list[str]]) -> Response:
        with self._lock:
            now = self.clock.now()  # inside the lock: a request that waited must not get state newer than its time
            resp = self._answer(path.rstrip("/") or "/", query, now)
        resp.headers = {"X-Mock-Time": utc_iso(now), **resp.headers}
        return resp

    def _answer(self, path: str, query: dict, now: datetime) -> Response:
        self._rescan_bounds(now)  # before the end check: a new hour ends the `end` state
        if path == "/_mock/clock":
            return Response(200, self.clock_info(now))
        feed = config.PATH_FEED.get(path)
        if feed is None and path != "/_mock/ids":
            return mock_error(404, "unknown-path", f"unknown path {path}")
        if self.archive_end is None or now >= self.archive_end:
            return mock_error(503, "end", "end of archive")
        self.advance(now)
        if path == "/_mock/ids":
            return Response(200, self.ids(now))
        if self.settings.inject_p > 0 and self.rng.random() < self.settings.inject_p:
            return self._injected()
        limit = config.FRESHNESS[feed]
        failed = self.window.get(feed)
        if self.settings.replay_errors and failed and now - failed["_t"] <= config.ERROR_WINDOW_TAIL:
            return self._archived_error(failed)
        last = self.latest.get(feed)
        if last is None or (limit is not None and now - last > limit):
            return mock_error(503, "no-data", f"no observation of {feed} within its freshness limit")
        return getattr(self, f"_{feed}")(query, now, limit)

    def clock_info(self, now: datetime) -> dict:
        return {
            "replay_time": utc_iso(now),
            "speed": self.clock.speed,
            "archive_start": utc_iso(self.archive_start) if self.archive_start else None,
            "archive_end": utc_iso(self.archive_end) if self.archive_end else None,
            "ended": self.archive_end is None or now >= self.archive_end,
            "errors": "replay" if self.settings.replay_errors else "off",
            "inject": self.settings.inject_p,
        }

    def ids(self, now: datetime) -> dict:
        out = {}
        for platform, feeds in config.ID_FEEDS.items():
            keys = set()
            for feed in feeds:
                keys |= self.state[feed].fresh_keys(now, config.FRESHNESS[feed])
            out[platform] = sorted(keys)
        return out

    def _injected(self) -> Response:
        kind = self.rng.choice(("429", "500", "timeout"))
        headers = {"X-Mock-Reason": "injected"}
        if kind == "429":
            return Response(429, {"errors": [{"code": 4, "message": "Too many requests (injected)"}]},
                            {**headers, "Retry-After": "5"})
        if kind == "500":
            return Response(500, {"mock_error": "injected"}, headers)
        return Response(504, {"mock_error": "injected timeout"}, headers, delay_s=self.settings.inject_delay_s)

    @staticmethod
    def _archived_error(rec: dict) -> Response:
        headers = dict(rec.get("rate_limit") or {})
        if "status" not in rec:
            return Response(504, {"mock_error": rec.get("error", "network error")},
                            {**headers, "X-Mock-Reason": "network-error"})
        body = rec["body"] if "body" in rec else rec.get("body_text", "")
        return Response(rec["status"], body, headers)

    # ---- one handler per feed: (query, now, freshness limit) -> Response

    def _roblox_batch(self, feed: str, query: dict, now: datetime, limit) -> Response:
        try:
            ids = [int(x) for x in (first(query, "universeIds") or "").split(",") if x.strip()]
        except ValueError:
            return mock_error(400, "bad-request", "universeIds must be comma-separated integers")
        if not ids or (feed == "roblox_games" and len(ids) > config.MAX_GAMES_IDS):
            return mock_error(400, "bad-request", f"{len(ids)} IDs requested")
        data = []
        for uid in ids:
            hit = self.state[feed].get(uid, now, limit)
            if hit:
                data.append(with_mock(hit[0], hit[2]))
        return Response(200, {"data": data})

    def _roblox_games(self, query, now, limit):
        return self._roblox_batch("roblox_games", query, now, limit)

    def _roblox_votes(self, query, now, limit):
        return self._roblox_batch("roblox_votes", query, now, limit)

    def _whole(self, feed: str, key, now: datetime, limit, unknown: Response, add_mock=with_mock) -> Response:
        hit = self.state[feed].get(key, now, limit)
        if hit is None:
            return unknown
        rec, _, observed_at = hit
        return Response(rec["status"], add_mock(rec["body"], observed_at))

    def _int_param(self, feed: str, query: dict, name: str, now, limit, add_mock=with_mock) -> Response:
        try:
            key = int(first(query, name) or "")
        except ValueError:
            return mock_error(400, "bad-request", f"{name} must be an integer")
        return self._whole(feed, key, now, limit, mock_error(404, "unknown-id", f"no observation of {key}"), add_mock)

    def _steam_players(self, query, now, limit):
        return self._int_param("steam_players", query, "appid", now, limit)

    def _steam_appdetails(self, query, now, limit):
        return self._int_param("steam_appdetails", query, "appids", now, limit, with_mock_per_app)

    def _steam_charts(self, query, now, limit):
        return self._whole("steam_charts", None, now, limit, mock_error(503, "no-data"))

    def _steam_store_new(self, query, now, limit):
        start = first(query, "start") or "0"
        return self._whole("steam_store_new", start, now, limit,
                           mock_error(404, "unknown-id", f"no recorded page with start={start}"))

    def _roblox_search(self, query, now, limit):
        q = first(query, "searchQuery") or ""
        return self._whole("roblox_search", q, now, limit, Response(200, {"searchResults": []}))

    def _roblox_sorts(self, query, now, limit):
        sorts: SortsState = self.state["roblox_sorts"]
        country = first(query, "country")
        cycles = sorts.complete.get(country or "")
        if not cycles:
            return mock_error(503, "no-data", f"no complete sorts cycle for country {country}")
        token = first(query, "sortsPageToken") or None  # blank: the first page, as on the real API
        if token is None:
            pages, n = cycles[-1], 0
            if now - pages[-1][1] > limit:
                return mock_error(503, "no-data", f"latest sorts cycle of {country} is too old")
        else:
            pages, n = self._find_page(cycles, token)
            if pages is None:
                return mock_error(400, "unknown-token", "unknown or expired sortsPageToken")
        body, _, observed_at = pages[n]
        out = with_mock(body, observed_at)
        out["nextSortsPageToken"] = SortsState.token(pages, n + 1) if n + 1 < len(pages) else ""
        return Response(200, out)

    @staticmethod
    def _find_page(cycles, token: str):
        cycle_ms, _, page = token.partition("-p")
        for pages in cycles:
            if SortsState.token(pages, 0).split("-p")[0] == cycle_ms and page.isdigit() and int(page) < len(pages):
                return pages, int(page)
        return None, 0
