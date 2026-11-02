"""One cycle of each collector job. A job's loop (see cli.run_job) calls its cycle every Job.interval_s.

Discovery cycles (roblox-sorts, steam-lists, roblox-search) archive their responses and update the
ID list; snapshot cycles (roblox-games, roblox-votes, steam-players) poll the IDs from that list.
"""

import logging
import threading
from dataclasses import dataclass

from . import config, extract, idlist
from .clock import Clock
from .config import Settings
from .http import Fetcher
from .idlist import IdStore
from .records import is_ok

log = logging.getLogger(__name__)


@dataclass
class Ctx:
    settings: Settings
    fetcher: Fetcher
    ids: IdStore
    clock: Clock
    stop: threading.Event


def batched(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ------------------------------------------------------------------ discovery


def roblox_sorts_cycle(ctx: Ctx) -> int:
    """All pages of the explore sorts for each country; returns the number of unique games found."""
    found: dict[int, None] = {}
    for country in ctx.settings.roblox_countries:
        token = None
        for _ in range(ctx.settings.max_sort_pages):
            if ctx.stop.is_set():
                return len(found)
            params = {"sessionId": ctx.settings.session_id, "device": "computer", "country": country}
            if token:
                params["sortsPageToken"] = token
            rec = ctx.fetcher.get("roblox_sorts", config.ROBLOX_SORTS_URL, params)
            if not is_ok(rec):
                break
            found.update(dict.fromkeys(extract.sort_universe_ids(rec["body"])))
            token = extract.next_sorts_token(rec["body"])
            if not token:
                break
    if found:
        now = ctx.clock.now()
        ctx.ids.update(lambda s: idlist.prune(idlist.merge(s, "roblox", found, now), now,
                                              ctx.settings.id_max_age_days))
    return len(found)


def steam_lists_cycle(ctx: Ctx) -> int:
    """Top-100 chart + popularnew pages -> Steam IDs (+ clone candidates); appdetails for IDs not fetched yet."""
    s = ctx.settings
    rec = ctx.fetcher.get("steam_charts", config.STEAM_CHARTS_URL)
    chart = extract.chart_appids(rec["body"]) if is_ok(rec) else []

    rows: list[dict] = []
    start = 0
    for _ in range(s.max_store_pages):
        if ctx.stop.is_set():
            break
        rec = ctx.fetcher.get("steam_store_new", config.STEAM_STORE_SEARCH_URL, {
            "filter": "popularnew", "start": start, "count": 100, "infinite": 1, "cc": "us", "l": "english"})
        if not is_ok(rec):
            break
        rows += extract.store_rows(rec["body"].get("results_html") or "")
        start += 100
        if start >= (rec["body"].get("total_count") or 0):
            break

    now = ctx.clock.now()
    cands = extract.clone_candidates(rows, now.date(), s.candidate_max_age_days)
    info = {gid: {"candidate": False} for gid in chart}
    info.update({c["appid"]: {"candidate": True, "released": c["released"],
                              **({"title": c["title"]} if "title" in c else {})} for c in cands})
    if info:
        ctx.ids.update(lambda st: idlist.prune(idlist.merge(st, "steam", info, now, info), now,
                                               s.id_max_age_days))

    fetched = ctx.ids.read()["steam_details"]
    for appid in [gid for gid in info if str(gid) not in fetched]:
        if ctx.stop.is_set():
            break
        rec = ctx.fetcher.get("steam_appdetails", config.STEAM_APPDETAILS_URL,
                              {"appids": appid, "cc": "us", "l": "english"})
        if is_ok(rec):
            stamp = rec["requested_at"]
            ctx.ids.update(lambda st: st["steam_details"].__setitem__(str(appid), stamp))
    return len(info)


def roblox_search_cycle(ctx: Ctx) -> int:
    """Search Roblox for the title of every Steam clone candidate; returns the number of queries."""
    n = 0
    for _, entry in idlist.candidates(ctx.ids.read()):
        if ctx.stop.is_set() or "title" not in entry:
            continue
        ctx.fetcher.get("roblox_search", config.ROBLOX_SEARCH_URL, {
            "searchQuery": entry["title"], "sessionId": ctx.settings.session_id, "pageType": "all"})
        n += 1
    return n


# ------------------------------------------------------------------ snapshots


def _roblox_batches(ctx: Ctx, feed: str, url: str) -> int:
    ids = idlist.select(ctx.ids.read(), "roblox", ctx.settings.max_roblox_ids)
    if not ids:
        log.info("%s: ID list is empty, waiting for discovery (roblox-sorts)", feed)
    for batch in batched(ids, ctx.settings.roblox_batch):
        if ctx.stop.is_set():
            break
        ctx.fetcher.get(feed, url, {"universeIds": ",".join(map(str, batch))})
    return len(ids)


def roblox_games_cycle(ctx: Ctx) -> int:
    return _roblox_batches(ctx, "roblox_games", config.ROBLOX_GAMES_URL)


def roblox_votes_cycle(ctx: Ctx) -> int:
    return _roblox_batches(ctx, "roblox_votes", config.ROBLOX_VOTES_URL)


def steam_players_cycle(ctx: Ctx) -> int:
    ids = idlist.select(ctx.ids.read(), "steam", ctx.settings.max_steam_ids)
    if not ids:
        log.info("steam_players: ID list is empty, waiting for discovery (steam-lists)")
    for appid in ids:
        if ctx.stop.is_set():
            break
        ctx.fetcher.get("steam_players", config.STEAM_PLAYERS_URL, {"appid": appid})
    return len(ids)


CYCLES = {
    "roblox-sorts": roblox_sorts_cycle,
    "steam-lists": steam_lists_cycle,
    "roblox-search": roblox_search_cycle,
    "roblox-games": roblox_games_cycle,
    "roblox-votes": roblox_votes_cycle,
    "steam-players": steam_players_cycle,
}
