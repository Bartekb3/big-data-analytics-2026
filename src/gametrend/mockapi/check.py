"""Poll a running mock API like the ingestion producers would and report, per source: the largest gap
between requests (wall time), statuses, new vs repeated observations, the newest and the oldest observation
relative to the replay time of its response (the newest must never be later), and the replay-time span of
error responses. With an archive (`--data-dir`) every response is also compared with what the archive says
it must be (verify.Oracle), and the received observations are counted against the recorded ones.

Polling plan (architecture/mockapi/README.md, Polling plan): each stream source sends the next request of its
round every max(pace, min(cycle / N, 14 s)) seconds of replay time, N being the requests in a round, so at the
rate the data was recorded; each source goes round-robin and keeps its position when the ID list is refreshed.
The other sources are polled at their recorded cycle, only to check their answers. Each request starts on
schedule, whatever the previous ones are doing, and failures are not backed off: this measures the mock, not a
producer.
"""

import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import requests

from ..collector import config as collector_config
from .clock import parse_utc
from .config import FEED_PATH

# recorded cycle of each source, in seconds (the collector's job intervals)
CYCLE = {feed: job.interval_s for job in collector_config.JOBS.values() for feed in job.feeds}
CYCLE.update(steam_charts=120, steam_store_new=120, steam_appdetails=300, roblox_search=300)  # rare: polled more often
IDS_EVERY = 60
MAX_SPACING = 14  # seconds between requests of one stream source, below the 15 s the course requires
BATCH = 50
STREAM = ("roblox_games", "roblox_votes", "steam_players")
STORE_PAGES = ("0", "100")
SEARCH_QUERIES = 20  # archived search queries asked (needs the archive)
WORKERS = 32  # requests in flight at once; a slow answer must not delay the next request

_local = threading.local()


def session() -> requests.Session:
    """One HTTP session per thread."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def spacing(feed: str, n: int) -> float:
    """Seconds between two requests of a feed whose round has n requests: the recording rate, but never faster
    than the collector's pacer and, for stream sources, never slower than MAX_SPACING."""
    if feed not in STREAM:
        return CYCLE[feed] / n
    pace = collector_config.PACER_INTERVAL[collector_config.FEED_PACER[feed]]
    return max(pace, min(CYCLE[feed] / n, MAX_SPACING))


def keep_order(current: list, fresh: list) -> list:
    """IDs in polling order after a refresh: known IDs keep their place, new ones join at the end, IDs no
    longer listed leave. Restarting the round at every refresh would starve the IDs at the end of the list."""
    listed = set(fresh)
    kept = [i for i in current if i in listed]
    known = set(kept)
    return kept + [i for i in fresh if i not in known]


class Stats:
    def __init__(self):
        self.times: dict[str, list[float]] = defaultdict(list)
        self.status: dict[str, Counter] = defaultdict(Counter)
        self.seen: set = set()
        self.new: Counter = Counter()
        self.repeat: Counter = Counter()
        self.max_lead: dict[str, float] = {}
        self.max_age: dict[str, float] = {}
        self.errors: dict[str, list] = defaultdict(list)  # feed -> replay times of non-200 answers
        self.compared = 0  # answers compared with the archive
        self.mismatch: Counter = Counter()
        self.examples: list[str] = []
        self.first_a = self.last_a = None  # replay times of the first and the last answer
        self.lock = threading.Lock()  # requests run in parallel

    def answered(self, replay_time: str) -> None:
        a = parse_utc(replay_time)
        self.first_a = min(self.first_a, a) if self.first_a else a
        self.last_a = max(self.last_a, a) if self.last_a else a

    def new_since(self, feed: str, since) -> int:
        """Distinct observations of a feed received that were made at or after `since`."""
        return sum(1 for f, _, t in self.seen if f == feed and parse_utc(t) >= since)

    def observe(self, feed: str, key, observed_at: str, replay_time: str) -> None:
        lead = (parse_utc(observed_at) - parse_utc(replay_time)).total_seconds()
        self.max_lead[feed] = max(self.max_lead.get(feed, lead), lead)
        self.max_age[feed] = max(self.max_age.get(feed, -lead), -lead)
        if (feed, key, observed_at) in self.seen:
            self.repeat[feed] += 1
        else:
            self.seen.add((feed, key, observed_at))
            self.new[feed] += 1

    def problems(self, feed: str, found: list[str], replay_time: str) -> None:
        self.compared += 1
        self.mismatch[feed] += len(found)
        self.examples += [f"{replay_time} {feed}: {p}" for p in found][:max(0, 10 - len(self.examples))]

    def gaps(self, feed: str) -> list[float]:
        t = sorted(self.times[feed])
        return [b - a for a, b in zip(t, t[1:])]


def run_check(base_url: str, duration: float, timeout: float = 10, data_dir: str | None = None) -> int:
    clock = session().get(f"{base_url}/_mock/clock", timeout=timeout).json()
    speed = clock["speed"]
    print(f"mock at {base_url}: replay time {clock['replay_time']}, speed {speed}, errors {clock.get('errors')}, "
          f"injection {clock.get('inject')}, archive {clock['archive_start']} .. {clock['archive_end']}", flush=True)
    oracle = None
    if data_dir:
        from .verify import Oracle

        a = parse_utc(clock["replay_time"])
        oracle = Oracle(Path(data_dir).expanduser() / "raw", a, a + timedelta(seconds=duration * speed + 60))
        print(f"comparing every answer with the archive in {data_dir}", flush=True)
    ctx = {"oracle": oracle, "replay_errors": clock.get("errors", "replay") == "replay"}
    stats = Stats()
    t0 = time.monotonic()
    order = {"roblox": [], "steam": []}  # IDs in polling order
    tasks: dict[str, list] = {}  # feed -> requests of one round
    cursor: Counter = Counter()  # feed -> requests sent (position in the round)
    due: dict[str, float] = {}  # feed -> wall time of its next request
    ended = threading.Event()

    def refresh(ids: dict) -> None:
        for platform in order:
            order[platform] = keep_order(order[platform], ids.get(platform, []))
        roblox, steam = order["roblox"], order["steam"]
        tasks["roblox_games"] = [roblox[i:i + BATCH] for i in range(0, len(roblox), BATCH)]
        tasks["roblox_votes"] = tasks["roblox_games"]
        tasks["steam_players"] = [[a] for a in steam]
        tasks["roblox_sorts"] = [[c] for c in collector_config.Settings.roblox_countries]
        tasks["steam_charts"] = [[None]]
        tasks["steam_store_new"] = [[p] for p in STORE_PAGES]
        tasks["steam_appdetails"] = [[a] for a in steam]
        if oracle:
            tasks["roblox_search"] = [[q] for q in oracle.keys("roblox_search")[:SEARCH_QUERIES]]
        now = time.monotonic()
        for feed, round_ in tasks.items():
            if round_ and feed not in due:
                due[feed] = now

    def get_ids():
        try:
            r = session().get(f"{base_url}/_mock/ids", timeout=timeout)
        except requests.RequestException:
            return {}  # mock restarting: keep polling, take the IDs again a minute later
        return r.json() if r.status_code == 200 else None

    ids = get_ids()
    if not ids:
        print("no IDs (archive ended, no data yet or mock not reachable)")
        return 1
    print(f"IDs: {len(ids['roblox'])} Roblox, {len(ids['steam'])} Steam", flush=True)
    refresh(ids)
    next_ids = t0 + IDS_EVERY / speed

    def send(feed: str, arg: list) -> None:
        try:
            if request(base_url, feed, arg, stats, timeout, ctx):
                ended.set()
        except Exception as e:  # e.g. an answer that did not come from the mock: count it, keep polling
            with stats.lock:
                stats.status[feed][type(e).__name__] += 1

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while time.monotonic() - t0 < duration and not ended.is_set():
            feed = min(due, key=due.get) if due else None
            if feed is None or next_ids <= due[feed]:
                time.sleep(max(0.0, next_ids - time.monotonic()))
                next_ids += IDS_EVERY / speed
                ids = get_ids()
                if ids is None:
                    ended.set()
                elif ids:
                    refresh(ids)
                continue
            time.sleep(max(0.0, due[feed] - time.monotonic()))
            round_ = tasks[feed]
            if not round_:  # its IDs are gone
                del due[feed]
                continue
            arg = round_[cursor[feed] % len(round_)]
            cursor[feed] += 1
            due[feed] += spacing(feed, len(round_)) / speed
            pool.submit(send, feed, arg)

    recorded = {f: oracle.count(f, stats.first_a, stats.last_a) for f in STREAM} if oracle and stats.first_a else {}
    report(stats, time.monotonic() - t0, ended.is_set(), recorded)
    stream_ok = all(stats.gaps(f) and max(stats.gaps(f)) <= 15 for f in STREAM)
    lead_ok = all(v <= 0 for v in stats.max_lead.values())
    match_ok = not sum(stats.mismatch.values())
    return 0 if stream_ok and lead_ok and match_ok else 2


def params_of(feed: str, arg: list) -> dict:
    if feed in ("roblox_games", "roblox_votes"):
        return {"universeIds": ",".join(map(str, arg))}
    name = {"steam_players": "appid", "steam_appdetails": "appids", "steam_store_new": "start",
            "roblox_search": "searchQuery", "roblox_sorts": "country"}.get(feed)
    return {name: arg[0]} if name else {}


def request(base_url: str, feed: str, arg: list, stats: Stats, timeout: float, ctx: dict | None = None) -> bool:
    """Send one request (all pages for sorts); returns True when the archive has ended."""
    ctx = ctx or {}
    oracle = ctx.get("oracle")
    url = base_url + FEED_PATH[feed]
    params = params_of(feed, arg)
    for _ in range(collector_config.Settings.max_sort_pages):
        with stats.lock:
            stats.times[feed].append(time.monotonic())
        try:
            r = session().get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            with stats.lock:
                stats.status[feed][type(e).__name__] += 1
            return False
        try:
            body = r.json()
        except ValueError:
            body = r.text
        with stats.lock:
            token = record(feed, arg, params, r, body, stats, oracle, ctx.get("replay_errors", True))
        if token is True:
            return True
        if not token:
            return False
        params = {"country": arg[0], "sortsPageToken": token}
    return False


def record(feed: str, arg: list, params: dict, r, body, stats: Stats, oracle, replay_errors: bool):
    """Book one answer; returns True at the end of the archive, the next sorts page token, or None."""
    stats.status[feed][r.status_code] += 1
    reason = r.headers.get("X-Mock-Reason")
    if reason == "end":
        return True
    replay_time = r.headers["X-Mock-Time"]
    stats.answered(replay_time)
    if r.status_code != 200:
        stats.errors[feed].append(replay_time)
    if oracle and reason != "injected":
        query = {k: [str(v)] for k, v in params.items()}
        stats.problems(feed, oracle.problems(feed, query, parse_utc(replay_time), r.status_code, body, reason,
                                             replay_errors), replay_time)
    if feed == "steam_appdetails" and isinstance(body, dict):  # `_mock` sits inside the app's object
        body = body.get(str(arg[0]), {})
    if not isinstance(body, dict) or r.status_code not in (200, 404) or "_mock" not in body and "data" not in body:
        return None
    if feed in ("roblox_games", "roblox_votes"):
        for item in body.get("data", []):
            stats.observe(feed, item["id"], item["_mock"]["observed_at"], replay_time)
        return None
    if "_mock" in body:
        stats.observe(feed, arg[0], body["_mock"]["observed_at"], replay_time)
    return body.get("nextSortsPageToken") if feed == "roblox_sorts" else None


def report(stats: Stats, elapsed: float, ended: bool, recorded: dict | None = None) -> None:
    nan = float("nan")
    print(f"\nran {timedelta(seconds=round(elapsed))}{' (archive ended)' if ended else ''}")
    print(f"{'source':17} {'requests':>8} {'max gap s':>9} {'new obs':>8} {'repeats':>8} {'newest s':>9} "
          f"{'oldest s':>9} {'mismatch':>8}  statuses; errors at replay time")
    for feed in (*STREAM, *(f for f in CYCLE if f not in STREAM)):
        g = stats.gaps(feed)
        err = stats.errors[feed]
        span = f"; {err[0][11:19]}..{err[-1][11:19]}" if err else ""
        print(f"{feed:17} {len(stats.times[feed]):>8} {max(g) if g else nan:>9.1f} {stats.new[feed]:>8} "
              f"{stats.repeat[feed]:>8} {stats.max_lead.get(feed, nan):>9.1f} {-stats.max_age.get(feed, nan):>9.1f} "
              f"{stats.mismatch[feed]:>8}  {dict(stats.status[feed])}{span}")
    for feed, n in (recorded or {}).items():
        got = stats.new_since(feed, stats.first_a)
        print(f"{feed}: received {got} of the {n} observations recorded during the run")
    for line in stats.examples:
        print("  mismatch:", line)
    gaps_ok = all(stats.gaps(f) and max(stats.gaps(f)) <= 15 for f in STREAM)
    print(f"\nstream sources requested at least every 15 s: {'yes' if gaps_ok else 'NO'}")
    print(f"no observation later than the replay time: {'yes' if all(v <= 0 for v in stats.max_lead.values()) else 'NO'}")
    if stats.compared:
        bad = sum(stats.mismatch.values())
        print(f"answers equal to the archive: {'NO' if bad else 'yes'} ({stats.compared} compared, {bad} differences)")
    else:
        print("answers equal to the archive: not checked (no --data-dir)")
