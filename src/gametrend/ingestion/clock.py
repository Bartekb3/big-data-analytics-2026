"""Injected clock: the only place in ingestion that reads the wall clock (AGENTS.md rule 8)."""

import time
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Current UTC time (timezone-aware); the event `ts` on the real APIs."""

    def monotonic(self) -> float:
        """Seconds for measuring intervals (pacing, schedules)."""

    def sleep(self, seconds: float) -> None:
        """Wait (a fake clock just advances)."""


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)
