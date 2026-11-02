"""Test doubles and fixture loading for collector tests (no network)."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

RESPONSES = Path(__file__).resolve().parents[2] / "api_test" / "responses"


def load_fixture(name: str):
    return json.loads((RESPONSES / f"{name}_response.json").read_text(encoding="utf-8"))


class FakeClock:
    """Deterministic clock: sleep() advances both wall and monotonic time."""

    def __init__(self, start=datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)):
        self.t = 0.0
        self.start = start
        self.sleeps: list[float] = []

    def now(self):
        return self.start + timedelta(seconds=self.t)

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        if seconds > 0:
            self.sleeps.append(seconds)
            self.t += seconds

    def advance(self, seconds):
        self.t += seconds


class FakeResponse:
    def __init__(self, url, status=200, body=None, headers=None, text=None):
        self.url = url
        self.status_code = status
        self.headers = headers or {}
        self.content = text.encode() if text is not None else (json.dumps(body).encode() if body is not None else b"")


class FakeSession:
    """Answers GETs from a list of responders: callables (url, params) -> FakeResponse or an exception."""

    def __init__(self, responder):
        self.responder = responder
        self.calls: list[tuple[str, dict | None]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        result = self.responder(url, params)
        if isinstance(result, Exception):
            raise result
        return result
