from gametrend.mockapi.check import MAX_SPACING, Stats, keep_order, period, spacing


def test_stream_requests_stay_at_most_max_spacing_apart_with_few_ids():
    assert period("steam_players", 500) == 240  # many IDs: the feed cadence
    assert period("steam_players", 10) == 10 * MAX_SPACING  # few IDs: each polled more often
    assert period("roblox_sorts", 3) == 300  # batch-like source keeps its cadence
    assert spacing("roblox_games", 24) == 50 / 24  # 1155 Roblox IDs: a batch every 2.1 s
    assert spacing("roblox_votes", 24) == MAX_SPACING
    assert spacing("roblox_sorts", 3) == 100


def test_refresh_keeps_the_polling_order():
    assert keep_order([5, 1, 9], [1, 2, 5, 7]) == [5, 1, 2, 7]  # 9 left, 2 and 7 join at the end
    assert keep_order([], [3, 1]) == [3, 1]


def test_stats_count_repeats_lead_and_age():
    s = Stats()
    s.observe("roblox_games", 1, "2026-11-02T14:00:00.000Z", "2026-11-02T14:00:05.000Z")
    s.observe("roblox_games", 1, "2026-11-02T14:00:00.000Z", "2026-11-02T14:00:09.000Z")
    assert (s.new["roblox_games"], s.repeat["roblox_games"], s.max_lead["roblox_games"]) == (1, 1, -5.0)
    assert s.max_age["roblox_games"] == 9.0
