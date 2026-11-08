from datetime import date

from gametrend.collector import extract

from tests.collector.fakes import load_fixture


def test_sort_universe_ids_from_fixture():
    body = load_fixture("roblox_sorts")
    ids = extract.sort_universe_ids(body)
    assert len(ids) == 314 and len(set(ids)) == 314
    assert all(isinstance(i, int) for i in ids)
    assert extract.next_sorts_token(body)


def test_sort_universe_ids_skips_sorts_without_games():
    body = {"sorts": [{"sortId": "filters_v5"}, {"sortId": "x", "games": [{"universeId": 1}, {"universeId": 1}, {}]}]}
    assert extract.sort_universe_ids(body) == [1]
    assert extract.next_sorts_token({"nextSortsPageToken": ""}) is None


def test_chart_appids_from_fixture():
    ids = extract.chart_appids(load_fixture("steam_most_played"))
    assert len(ids) == 100 and ids[0] == 730


def test_store_rows_from_fixture():
    rows = extract.store_rows(load_fixture("steam_store_search")["results_html"])
    assert len(rows) == 100
    assert rows[0] == {"appid": 730, "title": "Counter-Strike 2", "released_raw": "Aug 21, 2012"}


def test_store_rows_unescape_and_missing_date():
    html = ('<a href="x" data-ds-appid="5"><span class="title">Tom &amp; Jerry</span>'
            '<div class="search_released responsive_secondrow">  </div></a>')
    assert extract.store_rows("start" + html) == [{"appid": 5, "title": "Tom & Jerry"}]


def test_parse_steam_date():
    assert extract.parse_steam_date("Feb 26, 2025") == date(2025, 2, 26)
    assert extract.parse_steam_date("26 Feb, 2025") == date(2025, 2, 26)
    assert extract.parse_steam_date("Coming soon") is None
    assert extract.parse_steam_date("Q1 2026") is None
    assert extract.parse_steam_date(None) is None


def test_clone_candidates_keeps_recent_releases_only():
    rows = [{"appid": 1, "title": "New", "released_raw": "Oct 28, 2026"},
            {"appid": 2, "title": "Old", "released_raw": "Aug 21, 2012"},
            {"appid": 3, "title": "Edge", "released_raw": "Aug 4, 2026"},   # exactly 90 days before
            {"appid": 4, "title": "Soon", "released_raw": "Coming soon"},
            {"appid": 5, "title": "Future", "released_raw": "Dec 1, 2026"}]
    out = extract.clone_candidates(rows, date(2026, 11, 2), 90)
    assert [(c["appid"], c["released"]) for c in out] == [(1, "2026-10-28"), (3, "2026-08-04")]
