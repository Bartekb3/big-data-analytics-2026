import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gametrend.ingestion.http import Result

RESPONSES = Path(__file__).resolve().parents[2] / "api_test" / "responses"
T0 = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)


def load(name: str):
    return json.loads((RESPONSES / f"{name}_response.json").read_text(encoding="utf-8"))


def mocked(obj: dict, observed_at: str) -> dict:
    """Like the mock API: a copy with `_mock.observed_at` added."""
    return {**copy.deepcopy(obj), "_mock": {"observed_at": observed_at}}


class FakeClock:
    def __init__(self, start: datetime = T0):
        self.t = 0.0
        self.start = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.start + timedelta(seconds=self.t)

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if seconds > 0:
            self.t += seconds


class FakeSink:
    def __init__(self, deliver: bool = True):
        self.sent: list[tuple[str, dict]] = []
        self.deliver = deliver

    def send(self, topic, event, delivered):
        self.sent.append((topic, event))
        delivered(self.deliver)

    def poll(self):
        pass

    def flush(self, timeout):
        return 0

    def kinds(self):
        return [e["kind"] for _, e in self.sent]


class FakeClient:
    """Answers from a list of scripted Results per feed (the last one repeats), records the requests."""

    def __init__(self, clock=None):
        self.clock = clock or FakeClock()
        self.answers: dict[str, list[Result]] = {}
        self.calls: list[tuple[str, dict | None]] = []

    def script(self, feed: str, *results: Result) -> None:
        self.answers[feed] = list(results)

    def get(self, feed, params=None):
        self.calls.append((feed, params))
        queue = self.answers[feed]
        res = queue.pop(0) if len(queue) > 1 else queue[0]
        return Result(res.status, res.body, res.headers, res.error, self.clock.now())

    def get_url(self, url, params=None):
        return self.get("_mock_ids" if url.endswith("/_mock/ids") else url, params)


def ok(body) -> Result:
    return Result(200, body)


@pytest.fixture
def clock():
    return FakeClock()
