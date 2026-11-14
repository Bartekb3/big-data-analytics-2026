"""Producer memory (pure, in memory): repeated observations and metadata changes.

- Repeat check: the last `ts` per observation key; an unchanged `ts` sends no event and is not counted
  (architecture/mockapi/README.md, Time). On the real APIs `ts` is always new, so it never fires.
- Metadata: a `meta` event is sent when a game is first seen or its metadata fields change. The fields are
  kept in memory, so after a restart every game's metadata is sent once more (harmless: `games` writes are
  guarded by the observation time).
"""

import hashlib
import json
import threading


class RepeatFilter:
    def __init__(self):
        self._last: dict[tuple, str] = {}
        self._lock = threading.Lock()

    def is_new(self, key: tuple, ts: str) -> bool:
        """True (and remembered) if `ts` differs from the last one seen for `key`."""
        with self._lock:
            if self._last.get(key) == ts:
                return False
            self._last[key] = ts
            return True

    def seen(self, key: tuple, ts: str) -> bool:
        """True if `ts` is the last one remembered for `key` (nothing is remembered)."""
        with self._lock:
            return self._last.get(key) == ts

    def remember(self, key: tuple, ts: str) -> None:
        with self._lock:
            self._last[key] = ts


def meta_fingerprint(event: dict) -> str:
    content = {k: v for k, v in event.items() if k not in ("ts", "kind")}
    return hashlib.sha1(json.dumps(content, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


class MetaTracker:
    def __init__(self):
        self._seen: dict[tuple, str] = {}
        self._lock = threading.Lock()

    def changed(self, platform: str, event: dict) -> bool:
        """True (and remembered) if this game's metadata is new or differs from the last one sent."""
        key = (platform, event.get("id"))
        fp = meta_fingerprint(event)
        with self._lock:
            if self._seen.get(key) == fp:
                return False
            self._seen[key] = fp
            return True

    def forget(self, platform: str, game_id: int) -> None:
        """Send the next metadata of this game again (used when its event could not be delivered)."""
        with self._lock:
            self._seen.pop((platform, game_id), None)
