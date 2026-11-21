"""Polling plan (pure): request spacing and round-robin rounds over the ID list.

Rules: architecture/mockapi/README.md, Polling plan (applies to the mock and to the real APIs).
"""

import math
import threading


def spacing(pace: float, cycle: float, n: int, max_spacing: float) -> float:
    """Seconds between two requests of a stream source whose round has n requests: the recording rate
    (cycle / n), never faster than the API pace, never slower than max_spacing (the 15 s rule)."""
    if n <= 0:
        return pace
    return max(pace, min(cycle / n, max_spacing))


def keep_order(current: list, fresh) -> list:
    """IDs in polling order after a refresh: known IDs keep their place, new IDs join at the end (in the
    order given), IDs no longer listed leave."""
    fresh_list = list(dict.fromkeys(fresh))
    fresh_set = set(fresh_list)
    kept = [i for i in current if i in fresh_set]
    known = set(kept)
    return kept + [i for i in fresh_list if i not in known]


class Round:
    """Round-robin over an ID list in batches; keeps its position when the list is refreshed."""

    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self._ids: list = []
        self._next_id = None  # the ID the next batch starts with
        self._lock = threading.Lock()

    def refresh(self, fresh) -> None:
        with self._lock:
            pos = self._position()
            old = self._ids
            self._ids = keep_order(old, fresh)
            listed = set(self._ids)
            # continue at the first ID at or after the old position that is still listed
            self._next_id = None
            for i in old[pos:]:
                if i in listed:
                    self._next_id = i
                    break

    def _position(self) -> int:
        if self._next_id is None:
            return 0
        try:
            return self._ids.index(self._next_id)
        except ValueError:
            return 0

    def __len__(self) -> int:
        return len(self._ids)

    @property
    def requests_per_round(self) -> int:
        return math.ceil(len(self._ids) / self.batch_size) if self._ids else 0

    def next_batch(self) -> list:
        """The next batch of IDs (empty if there are none); wraps around after the last ID."""
        with self._lock:
            if not self._ids:
                return []
            pos = self._position()
            batch = self._ids[pos:pos + self.batch_size]
            nxt = pos + len(batch)
            self._next_id = self._ids[nxt] if nxt < len(self._ids) else self._ids[0]
            return batch
