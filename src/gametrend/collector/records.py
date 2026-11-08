"""The raw record: one JSON object per HTTP call, stored unchanged (format in architecture/collector/README.md)."""

import json
from datetime import datetime


def utc_iso(ts: datetime) -> str:
    """ISO-8601 UTC string with millisecond precision and a Z suffix."""
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_body(content: bytes) -> tuple[str, object]:
    """Return ("body", parsed JSON) or ("body_text", decoded text) when the body is not JSON."""
    try:
        return "body", json.loads(content)
    except (ValueError, UnicodeDecodeError):
        return "body_text", content.decode("utf-8", errors="replace")


def rate_limit_headers(headers) -> dict[str, str]:
    """Keep only the headers that describe rate limits."""
    return {k.lower(): v for k, v in headers.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"}


def make_record(
    feed: str,
    url: str,
    requested_at: datetime,
    received_at: datetime,
    status: int | None = None,
    headers=None,
    content: bytes | None = None,
    error: str | None = None,
) -> dict:
    """Build the raw record of one call. A failed call (no HTTP response) has no status and an error."""
    rec = {
        "feed": feed,
        "requested_at": utc_iso(requested_at),
        "received_at": utc_iso(received_at),
        "url": url,
        "elapsed_ms": round((received_at - requested_at).total_seconds() * 1000),
    }
    if status is not None:
        rec["status"] = status
    if headers:
        limits = rate_limit_headers(headers)
        if limits:
            rec["rate_limit"] = limits
    if error is not None:
        rec["error"] = error
    if content:
        key, body = parse_body(content)
        rec[key] = body
    return rec


def is_ok(rec: dict) -> bool:
    return rec.get("status") == 200 and "body" in rec


def is_retryable(rec: dict) -> bool:
    """Network errors, 429 and 5xx are retried; other statuses (e.g. Steam 404 = no stats) are final."""
    status = rec.get("status")
    return status is None or status == 429 or status >= 500
