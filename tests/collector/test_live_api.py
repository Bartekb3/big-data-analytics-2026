"""Live API smoke tests: one call per endpoint, checking the fields the system relies on.

Never run by default (they need the network). Run manually: uv run pytest -m live tests/collector
"""

import time
import uuid

import pytest
import requests

from gametrend.collector import config, extract

pytestmark = pytest.mark.live
SID = str(uuid.uuid4())


@pytest.fixture(scope="module")
def http():
    s = requests.Session()
    s.headers["User-Agent"] = config.USER_AGENT
    yield s
    s.close()


def get(http, url, params=None):
    r = http.get(url, params=params, timeout=30)
    time.sleep(1)
    return r


@pytest.fixture(scope="module")
def roblox_ids(http):
    r = get(http, config.ROBLOX_SORTS_URL, {"sessionId": SID, "device": "computer", "country": "us"})
    assert r.status_code == 200
    ids = extract.sort_universe_ids(r.json())
    assert len(ids) >= 50 and extract.next_sorts_token(r.json())
    return ids[:50]


def test_roblox_games_batch_of_50(http, roblox_ids):
    r = get(http, config.ROBLOX_GAMES_URL, {"universeIds": ",".join(map(str, roblox_ids))})
    assert r.status_code == 200
    data = r.json()["data"]
    assert len(data) > 0
    for field in ("id", "name", "playing", "visits", "favoritedCount", "created", "updated", "genre_l1"):
        assert field in data[0], field


def test_roblox_votes_batch_of_50(http, roblox_ids):
    r = get(http, config.ROBLOX_VOTES_URL, {"universeIds": ",".join(map(str, roblox_ids))})
    assert r.status_code == 200
    assert {"id", "upVotes", "downVotes"} <= set(r.json()["data"][0])


def test_roblox_search(http):
    r = get(http, config.ROBLOX_SEARCH_URL, {"searchQuery": "garden", "sessionId": SID, "pageType": "all"})
    assert r.status_code == 200
    hits = [c for g in r.json()["searchResults"] for c in g.get("contents") or []]
    assert hits and "universeId" in hits[0]


def test_steam_chart_and_players(http):
    r = get(http, config.STEAM_CHARTS_URL)
    assert r.status_code == 200
    appids = extract.chart_appids(r.json())
    assert len(appids) == 100
    r = get(http, config.STEAM_PLAYERS_URL, {"appid": appids[0]})
    assert r.status_code == 200 and isinstance(r.json()["response"]["player_count"], int)


def test_steam_store_popularnew_and_appdetails(http):
    r = get(http, config.STEAM_STORE_SEARCH_URL,
            {"filter": "popularnew", "start": 0, "count": 100, "infinite": 1, "cc": "us", "l": "english"})
    assert r.status_code == 200
    rows = extract.store_rows(r.json()["results_html"])
    assert len(rows) >= 50 and "title" in rows[0]
    appid = rows[0]["appid"]
    r = get(http, config.STEAM_APPDETAILS_URL, {"appids": appid, "cc": "us", "l": "english"})
    data = r.json()[str(appid)]["data"]
    assert {"name", "release_date", "genres", "short_description"} <= set(data)
