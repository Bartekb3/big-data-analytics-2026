"""Injected clock: the only place that reads the wall clock (AGENTS.md rule 8)."""

import threading
import time
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Current UTC time (timezone-aware)."""

    def monotonic(self) -> float:
        """Seconds from an arbitrary origin, for measuring intervals."""

    def sleep(self, seconds: float) -> None:
        """Wait; may return early when the collector is stopping."""


class RealClock:
    def __init__(self, stop: threading.Event):
        self._stop = stop

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._stop.wait(seconds)
