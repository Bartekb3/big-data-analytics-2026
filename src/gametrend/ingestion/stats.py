"""Producer counters per feed, logged every minute (later written to `ingest_stats`).

sent      records delivered to Kafka
dropped   records dropped by validation (or bodies that could not be read)
errors    API errors (429, 5xx, timeouts, network errors)
failed    records Kafka did not accept (reported by the delivery callback)
requests  HTTP requests made
"""

import threading
from collections import Counter, defaultdict

FIELDS = ("requests", "sent", "dropped", "errors", "failed")


class Stats:
    def __init__(self):
        self._counts: dict[str, Counter] = defaultdict(Counter)
        self._lock = threading.Lock()

    def add(self, feed: str, name: str, n: int = 1) -> None:
        if n:
            with self._lock:
                self._counts[feed][name] += n

    def take(self) -> dict[str, dict[str, int]]:
        """Counts since the last call (and reset)."""
        with self._lock:
            out = {feed: {f: c[f] for f in FIELDS} for feed, c in sorted(self._counts.items())}
            self._counts.clear()
        return out


def format_counts(counts: dict[str, dict[str, int]]) -> str:
    if not counts:
        return "no activity"
    return "; ".join(f"{feed}: " + " ".join(f"{k}={v}" for k, v in c.items()) for feed, c in counts.items())
