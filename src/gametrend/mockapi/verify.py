"""Oracle for `check --data-dir`: what the mock must answer at replay time A, computed straight from the
archive (all records sorted, looked up by bisect) instead of the engine's incremental state, so a mistake in
the engine shows up as a mismatch. Rules: architecture/mockapi/README.md.

The oracle reads the archive once at start: files that appear later (follow mode) are not in it, so use it
on an archive that is ahead of the replay time.
"""

import bisect
from dataclasses import dataclass, field
from datetime import datetime

from . import config
from .archive import HOUR, archive_bounds, hour_groups, merge_hour
from .engine import first, query_of, usable


def strip_mock(obj: dict) -> dict:
    return {k: v for k, v in obj.items() if k != "_mock"}


def keys_of(feed: str, rec: dict) -> list:
    """(key, value) pairs one usable record provides, keyed as clients ask for them."""
    if feed in ("roblox_games", "roblox_votes"):
        return [(it["id"], it) for it in rec["body"].get("data") or []
                if isinstance(it, dict) and isinstance(it.get("id"), int)]
    query = query_of(rec["url"])
    param = {"steam_players": "appid", "steam_appdetails": "appids", "steam_store_new": "start",
             "roblox_search": "searchQuery"}.get(feed)
    if param is None:
        return [(None, rec)]  # steam_charts: one whole response
    value = first(query, param)
    if value is None and param == "start":
        value = "0"
    if value is None:
        return []
    if feed in ("steam_players", "steam_appdetails"):
        try:
            value = int(value)
        except ValueError:
            return []
    return [(value, rec)]


@dataclass
class Cycle:
    pages: list = field(default_factory=list)
    closed_at: datetime | None = None  # when the cycle became complete (None: never)


class Oracle:
    def __init__(self, raw_dir, since: datetime, until: datetime):
        self.end = archive_bounds(raw_dir, config.FEEDS)[1]
        self.attempts: dict[str, list] = {}  # feed -> all records, in time order
        self.usable_t: dict[str, list] = {}  # feed -> times of usable records
        self.index: dict[str, dict] = {}  # feed -> key -> ([times], [(rec, value)])
        self.cycles: dict[str, list[Cycle]] = {}  # sorts: country -> cycles in time order
        for feed in config.FEEDS:
            limit = config.FRESHNESS[feed]
            lo = since - limit - HOUR if limit is not None else None
            recs = []
            for hour, paths in hour_groups(raw_dir / feed).items():
                if (lo is None or hour + HOUR > lo) and hour <= until:
                    recs.extend(merge_hour(paths, {}))
            recs.sort(key=lambda r: r["_t"])
            self.attempts[feed] = recs
            ok = [r for r in recs if usable(feed, r)]
            self.usable_t[feed] = [r["_t"] for r in ok]
            if feed == "roblox_sorts":
                self._build_cycles(ok)
                continue
            idx: dict = {}
            for r in ok:
                for key, value in keys_of(feed, r):
                    times, hits = idx.setdefault(key, ([], []))
                    times.append(r["_t"])
                    hits.append((r, value))
            self.index[feed] = idx

    def _build_cycles(self, recs: list) -> None:
        open_: dict[str, Cycle] = {}
        for r in recs:
            query = query_of(r["url"])
            country = first(query, "country")
            if country is None:
                continue
            if first(query, "sortsPageToken") is None:
                if country in open_:
                    open_.pop(country).closed_at = r["_t"]  # a new cycle completes the previous one
                open_[country] = Cycle()
                self.cycles.setdefault(country, []).append(open_[country])
            elif country not in open_:
                continue
            c = open_[country]
            c.pages.append(r)
            if not r["body"].get("nextSortsPageToken") or len(c.pages) >= config.MAX_SORT_PAGES:
                c.closed_at = r["_t"]
                del open_[country]

    def count(self, feed: str, lo: datetime, hi: datetime) -> int:
        """Observations (key, time) of a per-ID feed recorded with lo <= observed_at <= hi."""
        return sum(1 for times, _ in self.index[feed].values() for t in times if lo <= t <= hi)

    def keys(self, feed: str) -> list:
        return list(self.index.get(feed, {}))

    # ---- what the mock must answer

    def window(self, feed: str, a: datetime) -> dict | None:
        """The archived failure an open error window serves at A, or None."""
        recs = self.attempts[feed]
        i = bisect.bisect_right(recs, a, key=lambda r: r["_t"])
        if i and not usable(feed, recs[i - 1]) and a - recs[i - 1]["_t"] <= config.ERROR_WINDOW_TAIL:
            return recs[i - 1]
        return None

    def hit(self, feed: str, key, a: datetime):
        """(record, value) of the latest usable observation of key at A within the freshness limit, or None."""
        times, hits = self.index[feed].get(key, ([], []))
        i = bisect.bisect_right(times, a)
        if not i:
            return None
        limit = config.FRESHNESS[feed]
        if limit is not None and a - times[i - 1] > limit:
            return None
        return hits[i - 1]

    def feed_fresh(self, feed: str, a: datetime) -> bool:
        times = self.usable_t[feed]
        i = bisect.bisect_right(times, a)
        limit = config.FRESHNESS[feed]
        return bool(i) and (limit is None or a - times[i - 1] <= limit)

    def problems(self, feed: str, query: dict, a: datetime, status: int, body, reason: str | None,
                 replay_errors: bool = True) -> list[str]:
        """Differences between one mock response at A and what the archive says it must be ([] = correct)."""
        if self.end is None or a >= self.end:
            return _want(status == 503 and reason == "end", f"want 503 end, got {status} {reason}")
        failed = self.window(feed, a) if replay_errors else None
        if failed is not None:
            want = failed.get("status", 504)
            return _want(status == want, f"want archived error {want} (at {failed['requested_at']}), got {status}")
        if not self.feed_fresh(feed, a):
            return _want(status == 503 and reason == "no-data", f"want 503 no-data, got {status} {reason}")
        if feed in ("roblox_games", "roblox_votes"):
            return self._batch(feed, query, a, status, body)
        if feed == "roblox_sorts":
            return self._sorts(query, a, status, body)
        return self._whole(feed, query, a, status, body)

    def _batch(self, feed, query, a, status, body) -> list[str]:
        if status != 200:
            return [f"want 200, got {status}"]
        ids = [int(x) for x in (first(query, "universeIds") or "").split(",") if x.strip()]
        served = {it["id"]: it for it in body.get("data", [])}
        out = [f"id {i} served but not requested" for i in set(served) - set(ids)]
        for uid in ids:
            h = self.hit(feed, uid, a)
            got = served.get(uid)
            if h is None:
                if got is not None:
                    out.append(f"id {uid} served, but has no observation within the limit")
                continue
            r, item = h
            if got is None:
                out.append(f"id {uid} missing (observed {r['requested_at']})")
            elif strip_mock(got) != item or got.get("_mock", {}).get("observed_at") != r["requested_at"]:
                out.append(f"id {uid}: item differs from the one observed at {r['requested_at']}")
        return out

    def _whole(self, feed, query, a, status, body) -> list[str]:
        param = {"steam_players": "appid", "steam_appdetails": "appids", "steam_store_new": "start",
                 "roblox_search": "searchQuery"}.get(feed)
        key = None if param is None else first(query, param) or ("0" if param == "start" else "")
        if feed in ("steam_players", "steam_appdetails"):
            key = int(key)
        h = self.hit(feed, key, a)
        if h is None:
            if feed == "roblox_search":
                return _want(status == 200 and body == {"searchResults": []}, "want an empty search result")
            return _want(status == 404, f"want 404 unknown-id for {key}, got {status}")
        r, _ = h
        if status != r["status"]:
            return [f"want {r['status']} (observed {r['requested_at']}), got {status}"]
        if not isinstance(body, dict) or strip_mock(body) != r["body"] \
                or body.get("_mock", {}).get("observed_at") != r["requested_at"]:
            return [f"{key}: body differs from the one observed at {r['requested_at']}"]
        return []

    def _sorts(self, query, a, status, body) -> list[str]:
        country, token = first(query, "country"), first(query, "sortsPageToken")
        visible = [c for c in self.cycles.get(country or "", []) if c.closed_at is not None and c.closed_at <= a]
        if token is None:
            if not visible or a - visible[-1].pages[-1]["_t"] > config.FRESHNESS["roblox_sorts"]:
                return _want(status == 503, f"want 503 no-data for {country}, got {status}")
            cycle, n = visible[-1], 0
        else:
            ms, _, page = token.partition("-p")
            match = [c for c in visible[-config.SORT_CYCLES_KEPT:]
                     if str(int(c.pages[0]["_t"].timestamp() * 1000)) == ms]
            if not match or not page.isdigit() or int(page) >= len(match[0].pages):
                return _want(status == 400, f"want 400 unknown-token, got {status}")
            cycle, n = match[0], int(page)
        if status != 200:
            return [f"want 200, got {status}"]
        r = cycle.pages[n]
        last = n + 1 == len(cycle.pages)
        out = []
        drop = ("_mock", "nextSortsPageToken")
        if {k: v for k, v in body.items() if k not in drop} != {k: v for k, v in r["body"].items() if k not in drop} \
                or body.get("_mock", {}).get("observed_at") != r["requested_at"]:
            out.append(f"{country} page {n}: differs from the page observed at {r['requested_at']}")
        if (body.get("nextSortsPageToken") == "") != last:
            out.append(f"{country} page {n}: next token {'missing' if not last else 'on the last page'}")
        return out


def _want(ok: bool, message: str) -> list[str]:
    return [] if ok else [message]
