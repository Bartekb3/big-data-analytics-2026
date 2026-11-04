"""Replay clock: maps wall-clock time onto archive time. The only place in mockapi that reads the wall clock."""

from datetime import datetime, timezone


def parse_utc(text: str) -> datetime:
    """ISO-8601 string (with Z or an offset) -> timezone-aware UTC datetime."""
    ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def wall_now() -> datetime:
    return datetime.now(timezone.utc)


class ReplayClock:
    """A = a0 + (wall - start) * speed. `start` is a fixed instant, so a restart continues at the same A."""

    def __init__(self, a0: datetime, start: datetime, speed: float = 1.0, wall=wall_now):
        if speed <= 0:
            raise ValueError("speed must be positive")
        self.a0 = a0
        self.start = start
        self.speed = speed
        self._wall = wall

    def now(self) -> datetime:
        return self.a0 + (self._wall() - self.start) * self.speed
