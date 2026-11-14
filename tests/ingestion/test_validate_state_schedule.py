from gametrend.ingestion.schedule import Round, keep_order, spacing
from gametrend.ingestion.state import MetaTracker, RepeatFilter
from gametrend.ingestion.validate import is_valid

TS = "2026-11-06T12:00:00.000Z"


# ------------------------------------------------------------------ validation


def test_valid_events_of_every_kind():
    assert is_valid("roblox", {"kind": "game", "ts": TS, "id": 1, "playing": 0, "visits": 5, "favorites": 2,
                               "updated": TS})
    assert is_valid("roblox", {"kind": "votes", "ts": TS, "id": 1, "up": 1, "down": 0})
    assert is_valid("roblox", {"kind": "sort", "ts": TS, "id": 1, "country": "us", "sort": "top-trending",
                               "rank": 1, "playing": 9})
    assert is_valid("roblox", {"kind": "meta", "ts": TS, "id": 1, "name": "A"})
    assert is_valid("steam", {"kind": "players", "ts": TS, "id": 1, "players": 0})
    assert is_valid("steam", {"kind": "meta", "ts": TS, "id": 1, "name": "A", "genres": ["Action"],
                              "released": "2025-02-26"})


def test_invalid_events():
    base = {"kind": "votes", "ts": TS, "id": 1, "up": 1, "down": 0}
    assert not is_valid("roblox", {**base, "down": None})
    assert not is_valid("roblox", {k: v for k, v in base.items() if k != "down"})
    assert not is_valid("roblox", {**base, "up": "1"})
    assert not is_valid("roblox", {**base, "up": True})  # bool is not an integer here
    assert not is_valid("roblox", {**base, "extra": 1})  # only the contract's fields
    assert not is_valid("roblox", {**base, "ts": "2026-11-06 12:00"})
    assert not is_valid("roblox", {**base, "id": "1"})
    assert not is_valid("steam", base)  # votes is not a Steam kind
    assert not is_valid("steam", {"kind": "meta", "ts": TS, "id": 1, "name": "A", "released": "Feb 2025"})
    assert not is_valid("roblox", {"kind": "meta", "ts": TS, "id": 1})  # name is required


# ------------------------------------------------------------------ repeats and metadata


def test_repeat_filter():
    r = RepeatFilter()
    assert r.is_new(("roblox_games", 1), TS)
    assert not r.is_new(("roblox_games", 1), TS)
    assert r.is_new(("roblox_votes", 1), TS)  # another endpoint, another key
    assert r.is_new(("roblox_games", 1), "2026-11-06T12:01:00.000Z")
    assert not r.seen(("roblox_sorts", "us"), TS)
    r.remember(("roblox_sorts", "us"), TS)
    assert r.seen(("roblox_sorts", "us"), TS)


def test_meta_tracker_sends_first_and_changed_only():
    m = MetaTracker()
    meta = {"kind": "meta", "ts": TS, "id": 1, "name": "A"}
    assert m.changed("roblox", meta)
    assert not m.changed("roblox", {**meta, "ts": "2026-11-07T00:00:00.000Z"})  # only the time differs
    assert m.changed("roblox", {**meta, "name": "B"})
    assert m.changed("steam", meta)  # another platform, another game
    m.forget("steam", 1)
    assert m.changed("steam", meta)


# ------------------------------------------------------------------ polling plan


def test_spacing_follows_the_polling_plan():
    assert spacing(3, 60, 24, 14) == 3  # Roblox games, 1155 IDs
    assert spacing(1, 300, 24, 14) == 12.5  # Roblox votes, 1155 IDs
    assert abs(spacing(1, 300, 178, 14) - 300 / 178) < 1e-9  # Steam players
    assert spacing(1, 300, 5, 14) == 14  # few IDs: never slower than the 15 s rule
    assert spacing(3, 60, 1000, 14) == 3  # never faster than the API pace
    assert spacing(3, 60, 0, 14) == 3


def test_keep_order():
    assert keep_order([3, 1, 2], [1, 2, 4, 3, 5]) == [3, 1, 2, 4, 5]
    assert keep_order([3, 1, 2], [2, 3]) == [3, 2]
    assert keep_order([], [5, 5, 4]) == [5, 4]


def test_round_batches_wrap_around():
    r = Round(2)
    r.refresh([1, 2, 3, 4, 5])
    assert r.requests_per_round == 3
    assert [r.next_batch() for _ in range(4)] == [[1, 2], [3, 4], [5], [1, 2]]


def test_round_keeps_its_position_when_refreshed():
    r = Round(2)
    r.refresh([1, 2, 3, 4, 5, 6])
    assert r.next_batch() == [1, 2]
    r.refresh([1, 2, 3, 4, 5, 6, 7])  # a new ID joins at the end, the round goes on
    assert r.next_batch() == [3, 4]
    r.refresh([1, 2, 6, 7])  # 5 left; the next listed ID after the position is 6
    assert r.next_batch() == [6, 7]
    assert r.next_batch() == [1, 2]


def test_round_empty():
    r = Round(50)
    assert r.next_batch() == [] and r.requests_per_round == 0


# ------------------------------------------------------------------ event time vs the mock's replay time


def test_after_mock_time():
    from gametrend.ingestion.validate import after_mock_time

    assert after_mock_time("2026-11-06T12:00:00.001Z", "2026-11-06T12:00:00.000Z")
    assert not after_mock_time("2026-11-06T12:00:00.000Z", "2026-11-06T12:00:00.000Z")  # equal is allowed
    assert not after_mock_time("2026-11-06T11:59:59.999Z", "2026-11-06T12:00:00.000Z")
    assert after_mock_time("2026-11-06T13:00:00.000Z", "2026-11-06T13:30:00+01:00")  # offsets compared in UTC


def test_after_mock_time_without_readable_header_keeps_the_event():
    from gametrend.ingestion.validate import after_mock_time

    assert not after_mock_time("2026-11-06T12:00:00.000Z", None)
    assert not after_mock_time("2026-11-06T12:00:00.000Z", "")
    assert not after_mock_time("2026-11-06T12:00:00.000Z", "garbage")
