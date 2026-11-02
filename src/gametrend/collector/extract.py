"""Pure helpers that read IDs (and little else) out of API response bodies.

The collector only needs IDs to decide what to poll next; full parsing into events belongs to the
producers (see the ingestion area).
"""

import html
import re
from datetime import date, datetime


def sort_universe_ids(body: dict) -> list[int]:
    """Unique universeIds of all games in all sorts of one get-sorts page, in order of appearance."""
    seen: dict[int, None] = {}
    for sort in body.get("sorts") or []:
        for game in sort.get("games") or []:
            uid = game.get("universeId")
            if isinstance(uid, int):
                seen.setdefault(uid, None)
    return list(seen)


def next_sorts_token(body: dict) -> str | None:
    return body.get("nextSortsPageToken") or None


def chart_appids(body: dict) -> list[int]:
    """AppIDs of the GetMostPlayedGames top-100 chart."""
    ranks = (body.get("response") or {}).get("ranks") or []
    return [r["appid"] for r in ranks if isinstance(r.get("appid"), int)]


_APPID = re.compile(r'data-ds-appid="(\d+)"')
_TITLE = re.compile(r'<span class="title">(.*?)</span>', re.S)
_RELEASED = re.compile(r'search_released[^>]*>\s*(.*?)\s*</div>', re.S)


def store_rows(results_html: str) -> list[dict]:
    """Rows of a Steam store search page: appid, title and the raw release date text (keys absent if missing)."""
    rows = []
    for chunk in results_html.split("<a href=")[1:]:
        m = _APPID.search(chunk)
        if not m:
            continue
        row = {"appid": int(m.group(1))}
        if t := _TITLE.search(chunk):
            row["title"] = html.unescape(t.group(1).strip())
        if r := _RELEASED.search(chunk):
            if r.group(1).strip():
                row["released_raw"] = r.group(1).strip()
        rows.append(row)
    return rows


_STEAM_DATE_FORMATS = ("%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y")


def parse_steam_date(text: str | None) -> date | None:
    """Steam store date text ("Feb 26, 2025") -> date; None for "Coming soon", "Q1 2026", etc."""
    if not text:
        return None
    for fmt in _STEAM_DATE_FORMATS:
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            pass
    return None


def clone_candidates(rows: list[dict], today: date, max_age_days: int) -> list[dict]:
    """Store rows released in the last max_age_days (inclusive), with a parsed `released` date."""
    out = []
    for row in rows:
        released = parse_steam_date(row.get("released_raw"))
        if released is not None and 0 <= (today - released).days <= max_age_days:
            out.append({**row, "released": released.isoformat()})
    return out
