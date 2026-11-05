"""Append-only raw archive: <raw_dir>/<feed>/date=YYYY-MM-DD/HH.jsonl, gzipped when the hour is over.

The current hour is a plain .jsonl file (one record per line, flushed after each write), so a crash
loses at most a partial last line. Closed hours are compressed to HH.jsonl.gz (lossless gzip,
level 6). Exactly one writer per feed is allowed (one collector process per job). Each feed has its
own lock, so compressing one feed's hour never delays writes of the other feeds.
"""

import gzip
import json
import logging
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

GZIP_LEVEL = 6  # standard gzip default: much faster than Python's 9, files only a few percent bigger


def hour_path(raw_dir: Path, feed: str, ts: datetime) -> Path:
    """Plain file for the UTC hour of ts."""
    return raw_dir / feed / f"date={ts:%Y-%m-%d}" / f"{ts:%H}.jsonl"


def gz_target(plain: Path) -> Path:
    """Free .gz name for a plain hour file (HH.jsonl.gz, or HH.1.jsonl.gz, ... if it already exists)."""
    target = plain.with_name(plain.name + ".gz")
    n = 1
    while target.exists():
        target = plain.with_name(f"{plain.stem}.{n}.jsonl.gz")
        n += 1
    return target


def compress(plain: Path) -> Path:
    """Gzip a closed hour file and remove the plain file."""
    target = gz_target(plain)
    tmp = target.with_name(target.name + ".tmp")
    with open(plain, "rb") as src, gzip.open(tmp, "wb", compresslevel=GZIP_LEVEL) as dst:
        shutil.copyfileobj(src, dst)
    os.replace(tmp, target)
    plain.unlink()
    return target


def stale_files(raw_dir: Path, feeds, current: dict[str, Path]) -> list[Path]:
    """Plain hour files of these feeds that are not currently open for writing."""
    out = []
    for feed in feeds:
        open_now = current.get(feed)
        for p in sorted((raw_dir / feed).glob("date=*/*.jsonl")):
            if p != open_now:
                out.append(p)
    return out


class Archive:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._open: dict[str, tuple[Path, object]] = {}

    def _lock(self, feed: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(feed, threading.Lock())

    def write(self, feed: str, record: dict, ts: datetime) -> None:
        """Append one record to the hour file of ts (the call's requested_at)."""
        path = hour_path(self.raw_dir, feed, ts)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock(feed):
            current = self._open.get(feed)
            if current and current[0] != path:
                self._close(feed)
            if feed not in self._open:
                path.parent.mkdir(parents=True, exist_ok=True)
                # backslashreplace: a lone surrogate (valid in JSON, not in UTF-8) is written as its JSON escape
                self._open[feed] = (path, open(path, "a", encoding="utf-8", errors="backslashreplace"))
            fh = self._open[feed][1]
            fh.write(line)
            fh.flush()

    def compress_stale(self, feeds) -> None:
        """Gzip leftover plain files of these feeds (e.g. after a crash or a restart in a later hour)."""
        for feed in feeds:
            with self._lock(feed):
                current = {feed: self._open[feed][0]} if feed in self._open else {}
                for p in stale_files(self.raw_dir, [feed], current):
                    if p.stat().st_size == 0:
                        p.unlink()
                        continue
                    log.info("compressing %s", p)
                    compress(p)

    def close(self) -> None:
        """Close all open files. They stay plain; the next start compresses them once their hour is over."""
        for feed in list(self._open):
            with self._lock(feed):
                if feed in self._open:
                    self._open.pop(feed)[1].close()

    def _close(self, feed: str) -> None:
        path, fh = self._open.pop(feed)
        fh.close()
        compress(path)
