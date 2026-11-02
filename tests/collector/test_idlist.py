from datetime import datetime, timedelta, timezone

from gametrend.collector import idlist

T0 = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def test_merge_keeps_first_seen_and_updates_last_seen():
    s = idlist.empty_state()
    idlist.merge(s, "roblox", [1, 2], T0)
    idlist.merge(s, "roblox", [2], T0 + timedelta(hours=1))
    assert s["roblox"]["2"] == {"first_seen": "2026-11-02T12:00:00.000Z", "last_seen": "2026-11-02T13:00:00.000Z"}
    assert s["roblox"]["1"]["last_seen"] == "2026-11-02T12:00:00.000Z"


def test_merge_adds_info():
    s = idlist.merge(idlist.empty_state(), "steam", [7], T0, {7: {"title": "X", "candidate": True}})
    assert s["steam"]["7"]["title"] == "X" and idlist.candidates(s) == [(7, s["steam"]["7"])]


def test_prune_drops_games_not_seen_for_max_age():
    s = idlist.empty_state()
    idlist.merge(s, "roblox", [1], T0 - timedelta(days=8))
    idlist.merge(s, "roblox", [2], T0 - timedelta(days=6))
    idlist.prune(s, T0, 7)
    assert list(s["roblox"]) == ["2"]


def test_select_most_recent_first_and_limited():
    s = idlist.empty_state()
    idlist.merge(s, "roblox", [1, 2], T0)
    idlist.merge(s, "roblox", [3], T0 + timedelta(minutes=15))
    assert idlist.select(s, "roblox", 2) == [3, 1]
    assert idlist.select(s, "steam", 10) == []


def test_store_round_trip_and_atomic_save(tmp_path):
    store = idlist.IdStore(tmp_path / "state" / "ids.json")
    assert store.read() == idlist.empty_state()
    store.update(lambda st: idlist.merge(st, "roblox", [5], T0))
    assert idlist.select(store.read(), "roblox", 10) == [5]
    assert [p.name for p in (tmp_path / "state").iterdir()] == ["ids.json"]
