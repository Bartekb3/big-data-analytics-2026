"""The ID list: which games the snapshot jobs poll. Written by discovery jobs, read by snapshot jobs.

Stored as one JSON file, replaced atomically, so collectors in separate processes can share it:

    {"roblox": {"<id>": {"first_seen": iso, "last_seen": iso}},
     "steam":  {"<id>": {"first_seen": iso, "last_seen": iso, "title": ..., "released": ..., "candidate": true}},
     "steam_details": {"<id>": iso}}    # when appdetails was fetched
"""

import json
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path

from .records import utc_iso

PLATFORMS = ("roblox", "steam")


def empty_state() -> dict:
    return {"roblox": {}, "steam": {}, "steam_details": {}}


def merge(state: dict, platform: str, ids, now: datetime, info: dict[int, dict] | None = None) -> dict:
    """Mark ids as seen at `now`; `info` adds or updates per-id fields (e.g. Steam title)."""
    games = state.setdefault(platform, {})
    stamp = utc_iso(now)
    for gid in ids:
        entry = games.setdefault(str(gid), {"first_seen": stamp})
        entry["last_seen"] = stamp
        if info and gid in info:
            entry.update(info[gid])
    return state


def prune(state: dict, now: datetime, max_age_days: int) -> dict:
    """Drop games whose last_seen is older than max_age_days."""
    cutoff = utc_iso(now - timedelta(days=max_age_days))
    for platform in PLATFORMS:
        games = state.get(platform, {})
        for gid in [g for g, e in games.items() if e["last_seen"] < cutoff]:
            del games[gid]
    return state


def select(state: dict, platform: str, limit: int) -> list[int]:
    """Up to `limit` ids, most recently seen first (ties by id), for polling."""
    games = state.get(platform, {})
    ordered = sorted(games.items(), key=lambda kv: (kv[1]["last_seen"], -int(kv[0])), reverse=True)
    return [int(gid) for gid, _ in ordered[:limit]]


def candidates(state: dict) -> list[tuple[int, dict]]:
    """Steam clone candidates with their info, by id."""
    return sorted((int(g), e) for g, e in state.get("steam", {}).items() if e.get("candidate"))


def load(path: Path) -> dict:
    if not path.exists():
        return empty_state()
    state = json.loads(path.read_text(encoding="utf-8"))
    for key, value in empty_state().items():
        state.setdefault(key, value)
    return state


def save(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


class IdStore:
    """File-backed ID list; every update is load -> change -> atomic save under a process-local lock."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()

    def read(self) -> dict:
        return load(self.path)

    def update(self, change) -> dict:
        """Apply change(state) and save; returns the new state."""
        with self._lock:
            state = load(self.path)
            change(state)
            save(self.path, state)
            return state
