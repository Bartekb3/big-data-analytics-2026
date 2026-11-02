from gametrend.collector import config, feeds, idlist
from gametrend.collector.archive import Archive
from gametrend.collector.config import Settings
from gametrend.collector.http import Fetcher
from gametrend.collector.idlist import IdStore

from tests.collector.fakes import FakeResponse, FakeSession, load_fixture


def make_ctx(tmp_path, clock, stop, responder, **settings):
    s = Settings(data_dir=tmp_path, session_id="sid", **settings)
    session = FakeSession(responder)
    archive = Archive(s.raw_dir)
    fetcher = Fetcher(session, clock, archive, s.pacer_interval, max_retries=1, rand=lambda: 0.0)
    return feeds.Ctx(s, fetcher, IdStore(s.ids_path), clock, stop), session


def test_batched():
    assert feeds.batched([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


def test_roblox_sorts_follows_pages_per_country_and_updates_ids(tmp_path, clock, stop):
    pages = {None: {"sorts": [{"games": [{"universeId": 1}, {"universeId": 2}]}], "nextSortsPageToken": "p2"},
             "p2": {"sorts": [{"games": [{"universeId": 2}, {"universeId": 3}]}], "nextSortsPageToken": None}}
    ctx, session = make_ctx(tmp_path, clock, stop,
                            lambda url, p: FakeResponse(url, 200, pages[p.get("sortsPageToken")]),
                            roblox_countries=("us", "pl"))
    assert feeds.roblox_sorts_cycle(ctx) == 3
    assert [(p["country"], p.get("sortsPageToken")) for _, p in session.calls] == \
        [("us", None), ("us", "p2"), ("pl", None), ("pl", "p2")]
    assert sorted(idlist.select(ctx.ids.read(), "roblox", 10)) == [1, 2, 3]
    assert clock.sleeps == [1.0, 1.0, 1.0]  # explore pacer: 1 request/s


def test_roblox_sorts_stops_country_on_error(tmp_path, clock, stop):
    ctx, session = make_ctx(tmp_path, clock, stop, lambda url, p: FakeResponse(url, 400, {"errors": []}),
                            roblox_countries=("us",))
    assert feeds.roblox_sorts_cycle(ctx) == 0
    assert len(session.calls) == 1
    assert ctx.ids.read() == idlist.empty_state()


def test_roblox_games_polls_known_ids_in_batches_of_50(tmp_path, clock, stop):
    ctx, session = make_ctx(tmp_path, clock, stop, lambda url, p: FakeResponse(url, 200, {"data": []}))
    ctx.ids.update(lambda s: idlist.merge(s, "roblox", range(1, 121), clock.now()))
    assert feeds.roblox_games_cycle(ctx) == 120
    assert [len(p["universeIds"].split(",")) for _, p in session.calls] == [50, 50, 20]
    assert all(url == config.ROBLOX_GAMES_URL for url, _ in session.calls)
    assert clock.sleeps == [3.0, 3.0]  # hidden-limit pacing


def test_snapshot_jobs_do_nothing_without_ids(tmp_path, clock, stop):
    ctx, session = make_ctx(tmp_path, clock, stop, lambda url, p: FakeResponse(url, 200, {}))
    assert feeds.roblox_votes_cycle(ctx) == 0
    assert feeds.steam_players_cycle(ctx) == 0
    assert session.calls == []


def test_stop_interrupts_a_cycle(tmp_path, clock, stop):
    ctx, session = make_ctx(tmp_path, clock, stop, lambda url, p: (stop.set(), FakeResponse(url, 200, {}))[1])
    ctx.ids.update(lambda s: idlist.merge(s, "steam", [1, 2, 3], clock.now()))
    feeds.steam_players_cycle(ctx)
    assert len(session.calls) == 1


def test_steam_lists_collects_chart_and_candidates_and_fetches_details_once(tmp_path, clock, stop):
    store = {"success": 1, "total_count": 2, "results_html":
             '<a href="x" data-ds-appid="900"><span class="title">Brand New</span>'
             '<div class="search_released">Oct 28, 2026</div></a>'
             '<a href="x" data-ds-appid="730"><span class="title">Counter-Strike 2</span>'
             '<div class="search_released">Aug 21, 2012</div></a>'}

    def responder(url, p):
        if url == config.STEAM_CHARTS_URL:
            return FakeResponse(url, 200, load_fixture("steam_most_played"))
        if url == config.STEAM_STORE_SEARCH_URL:
            return FakeResponse(url, 200, store)
        return FakeResponse(url, 200, {str(p["appids"]): {"success": True}})

    ctx, session = make_ctx(tmp_path, clock, stop, responder)
    assert feeds.steam_lists_cycle(ctx) == 101  # top 100 + 1 new release
    state = ctx.ids.read()
    assert state["steam"]["900"]["candidate"] is True and state["steam"]["900"]["released"] == "2026-10-28"
    assert state["steam"]["730"]["candidate"] is False  # old game: chart only, not a candidate
    assert len(state["steam_details"]) == 101

    session.calls.clear()
    feeds.steam_lists_cycle(ctx)
    assert not [c for c in session.calls if c[0] == config.STEAM_APPDETAILS_URL]  # details only once


def test_roblox_search_queries_candidate_titles(tmp_path, clock, stop):
    ctx, session = make_ctx(tmp_path, clock, stop, lambda url, p: FakeResponse(url, 200, {"searchResults": []}))
    ctx.ids.update(lambda s: idlist.merge(s, "steam", [1, 2, 3], clock.now(), {
        1: {"candidate": True, "title": "Dressmaker"}, 2: {"candidate": False, "title": "Old"}, 3: {"candidate": True}}))
    assert feeds.roblox_search_cycle(ctx) == 1
    assert session.calls[0][1]["searchQuery"] == "Dressmaker"
