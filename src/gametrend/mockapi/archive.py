"""Reads the collector archive forward in time, one feed at a time (only closed hours, i.e. .gz files).

Several files of one hour (HH.jsonl.gz, HH.1.jsonl.gz, ...) are merged by requested_at. New files
are picked up when the reader runs out of known ones (follow mode).
"""

import gzip
import heapq
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .clock import parse_utc

log = logging.getLogger(__name__)

_HOUR_FILE = re.compile(r"^(\d{2})(?:\.(\d+))?\.jsonl\.gz$")
HOUR = timedelta(hours=1)


def hour_start(path: Path) -> datetime | None:
    """Start of the UTC hour of an archive file (raw/<feed>/date=YYYY-MM-DD/HH[.n].jsonl.gz), or None."""
    m = _HOUR_FILE.match(path.name)
    day = path.parent.name.removeprefix("date=")
    if not m or day == path.parent.name:
        return None
    try:
        return datetime.strptime(f"{day} {m.group(1)}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def hour_groups(feed_dir: Path) -> dict[datetime, list[Path]]:
    """All .gz files of one feed, grouped by hour."""
    groups: dict[datetime, list[Path]] = {}
    for p in feed_dir.glob("date=*/*.jsonl.gz"):
        start = hour_start(p)
        if start is not None:
            groups.setdefault(start, []).append(p)
    return {h: sorted(ps) for h, ps in sorted(groups.items())}


def read_file(path: Path, counts: dict):
    """Records of one file, skipping broken lines (counted in counts["broken_lines"])."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                rec["_t"] = parse_utc(rec["requested_at"])
            except (ValueError, KeyError, TypeError):
                counts["broken_lines"] = counts.get("broken_lines", 0) + 1
                continue
            yield rec


def merge_hour(paths: list[Path], counts: dict):
    """Records of all files of one hour, in requested_at order (each file is already in order)."""
    return heapq.merge(*(read_file(p, counts) for p in paths), key=lambda r: r["_t"])


class FeedReader:
    """Yields the records of one feed up to a given time; each record once, in time order.

    Each record gets a parsed "_t" (its requested_at). Hours that end before `since` are skipped
    (warm-up window at start).
    """

    def __init__(self, feed_dir: Path, since: datetime | None = None, rescan_every: timedelta = timedelta(seconds=60)):
        self.feed_dir = feed_dir
        self.counts: dict = {}
        self._since = since
        self._rescan_every = rescan_every
        self._done: datetime | None = None  # last hour opened (or skipped)
        self._seen: set[Path] = set()
        self._last_scan: datetime | None = None
        self._next_start: datetime | None = None  # first known hour not opened yet
        self._iter = iter(())
        self._next: dict | None = None

    def take_until(self, until: datetime):
        """Records with requested_at <= until that were not returned before."""
        while True:
            if self._next is None:
                self._next = next(self._iter, None)
                if self._next is None and not self._open_next_hour(until):
                    return
                continue
            if self._next["_t"] > until:
                return
            rec, self._next = self._next, None
            yield rec

    def _open_next_hour(self, until: datetime) -> bool:
        if self._done is not None and until < self._done + HOUR:
            return False  # the next hour cannot hold records <= until
        known = self._next_start is not None and until >= self._next_start
        if not known and self._last_scan is not None and until - self._last_scan < self._rescan_every:
            return False  # follow mode: the directory was listed a moment ago and had nothing new
        self._last_scan = until
        for start, paths in hour_groups(self.feed_dir).items():
            if self._done is not None and start <= self._done:
                late = [p for p in paths if p not in self._seen]
                if late:  # e.g. HH.1 written after a collector restart, copied after we replayed that hour
                    self._seen.update(late)
                    log.warning("%s: ignoring file(s) of an hour already replayed: %s", self.feed_dir.name,
                                ", ".join(p.name for p in late))
                continue
            if start > until:
                self._next_start = start
                return False
            self._done = start
            self._seen.update(paths)
            if self._since is not None and start + HOUR <= self._since:
                continue  # before the warm-up window
            self._last_scan = self._next_start = None
            self._iter = merge_hour(paths, self.counts)
            return True
        return False


def archive_bounds(raw_dir: Path, feeds) -> tuple[datetime | None, datetime | None]:
    """(first requested_at in the archive, end of its last archived hour) over the given feeds."""
    first, end = None, None
    for feed in feeds:
        groups = hour_groups(raw_dir / feed)
        if not groups:
            continue
        hours = list(groups)
        for rec in merge_hour(groups[hours[0]], {}):
            first = rec["_t"] if first is None or rec["_t"] < first else first
            break
        last_end = hours[-1] + HOUR
        end = last_end if end is None or last_end > end else end
    return first, end
