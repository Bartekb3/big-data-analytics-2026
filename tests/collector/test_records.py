from datetime import datetime, timedelta, timezone

from gametrend.collector.records import is_ok, is_retryable, make_record, parse_body, utc_iso

T = datetime(2026, 11, 2, 13, 44, 43, 275000, tzinfo=timezone.utc)


def test_utc_iso_has_millis_and_z():
    assert utc_iso(T) == "2026-11-02T13:44:43.275Z"


def test_parse_body_json_and_text():
    assert parse_body(b'{"a": 1}') == ("body", {"a": 1})
    assert parse_body(b"<html>busy</html>") == ("body_text", "<html>busy</html>")


def test_record_keeps_original_times_status_and_body():
    rec = make_record("roblox_votes", "https://x/votes?universeIds=1", T, T + timedelta(milliseconds=213),
                      status=200, headers={"X-RateLimit-Limit": "200, 200;w=60", "Content-Type": "json"},
                      content=b'{"data": [{"id": 1, "upVotes": 5, "downVotes": 0}]}')
    assert rec == {
        "feed": "roblox_votes",
        "requested_at": "2026-11-02T13:44:43.275Z",
        "received_at": "2026-11-02T13:44:43.488Z",
        "url": "https://x/votes?universeIds=1",
        "elapsed_ms": 213,
        "status": 200,
        "rate_limit": {"x-ratelimit-limit": "200, 200;w=60"},
        "body": {"data": [{"id": 1, "upVotes": 5, "downVotes": 0}]},
    }
    assert is_ok(rec) and not is_retryable(rec)


def test_network_error_has_no_status_and_is_retryable():
    rec = make_record("steam_players", "https://x", T, T + timedelta(seconds=30), error="ReadTimeout: boom")
    assert "status" not in rec and "body" not in rec
    assert rec["error"] == "ReadTimeout: boom"
    assert is_retryable(rec) and not is_ok(rec)


def test_retry_rules():
    assert is_retryable({"status": 429})
    assert is_retryable({"status": 503})
    assert not is_retryable({"status": 404})  # Steam: app has no player stats
    assert not is_ok({"status": 200, "body_text": "not json"})
