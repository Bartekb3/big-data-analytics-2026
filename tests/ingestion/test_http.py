import requests

from gametrend.ingestion import config
from gametrend.ingestion.http import HttpClient, RateLimiter, Result, backoff_delay, classify

from .conftest import FakeClock


def test_rate_limiter_spaces_request_starts():
    clock = FakeClock()
    lim = RateLimiter(3.0, clock)
    for _ in range(3):
        lim.acquire()
    assert clock.sleeps == [0.0, 3.0, 3.0]
    clock.t += 10  # idle for a while: no waiting
    lim.acquire()
    assert clock.sleeps[-1] == 0.0


def test_backoff_doubles_with_jitter_and_is_capped():
    assert backoff_delay(0, 1, 10) == 0
    assert backoff_delay(1, 1, 10, rand=lambda: 1.0) == 1
    assert backoff_delay(3, 1, 10, rand=lambda: 1.0) == 4
    assert backoff_delay(3, 1, 10, rand=lambda: 0.0) == 2  # jitter: between half and all
    assert backoff_delay(10, 1, 10, rand=lambda: 1.0) == 10  # mock cap
    assert backoff_delay(10, 1, 120, rand=lambda: 1.0) == 120  # real cap


def test_classify_answers():
    assert classify(Result(200, {"data": []})) == "ok"
    assert classify(Result(200, None)) == "bad_body"  # text body
    assert classify(Result(200, [1])) == "bad_body"  # JSON that is not an object
    assert classify(Result(404, {"response": {"result": 42}})) == "not_found"
    assert classify(Result(404, {}, {"X-Mock-Reason": "unknown-id"})) == "not_found"
    assert classify(Result(503, {}, {"X-Mock-Reason": "end"})) == "end"
    assert classify(Result(503, {}, {"X-Mock-Reason": "no-data"})) == "error"
    assert classify(Result(400, {}, {"X-Mock-Reason": "unknown-token"})) == "stale_token"
    assert classify(Result(400, {}, {"X-Mock-Reason": "bad-request"})) == "error"
    assert classify(Result(429, {})) == "error"
    assert classify(Result(None, error="ReadTimeout")) == "error"


class FakeResponse:
    def __init__(self, status, payload=None, text=False, headers=None):
        self.status_code = status
        self._payload = payload
        self._text = text
        self.headers = headers or {}

    def json(self):
        if self._text:
            raise ValueError("not JSON")
        return self._payload


class FakeSession:
    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def make_client(answer, mode="mock"):
    session = FakeSession(answer)
    settings = config.Settings(mode=mode, mock_url="http://mock:8080/")
    return HttpClient(settings, FakeClock(), session_factory=lambda: session), session


def test_client_uses_mock_base_url_and_paths_of_the_real_api():
    client, session = make_client(FakeResponse(200, {"data": []}, headers={"X-Mock-Time": "t"}))
    res = client.get("roblox_games", {"universeIds": "1,2"})
    assert session.calls == [("http://mock:8080/v1/games", {"universeIds": "1,2"}, config.HTTP_TIMEOUT_S)]
    assert res.status == 200 and res.body == {"data": []} and res.outcome == "ok"
    assert res.headers["X-Mock-Time"] == "t"
    assert res.requested_at is not None


def test_client_real_mode_uses_real_hosts():
    client, session = make_client(FakeResponse(200, {}), mode="real")
    client.get("steam_players", {"appid": 1})
    assert session.calls[0][0] == "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"


def test_client_text_body_and_network_error():
    client, _ = make_client(FakeResponse(200, text=True))
    assert client.get("roblox_votes").outcome == "bad_body"
    client, _ = make_client(requests.ConnectionError("refused"))
    res = client.get("roblox_votes")
    assert res.status is None and "ConnectionError" in res.error and res.outcome == "error"


def test_settings_speed_scales_intervals():
    s = config.Settings(speed=10)
    assert s.scaled(60) == 6
    assert s.backoff_cap_s == 1.0
    client = HttpClient(s, FakeClock(), session_factory=lambda: None)
    assert client.limiters["roblox_games"].interval == 0.3
