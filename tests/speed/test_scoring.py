from datetime import timezone

import pytest

from gametrend.common.metrics import Observation
from gametrend.speed.parse import MetaEvent, VotesEvent
from gametrend.speed.scoring import (doc_to_observation, history_by_game, reduce_metas, reduce_players,
                                     reduce_votes, score_game, score_players)

from .helpers import doc, pe, t


def chosen_of(*events):
    reduced = reduce_players(events)
    assert len(reduced) == 1
    return next(iter(reduced.values()))


# ------------------------------------------------------------------ reduction


def test_many_observations_in_one_bucket_give_one():
    # Roblox game events every minute: ~5 per bucket
    evs = [pe(t(f"12:0{m}"), players=10 + m, visits=100 + m) for m in range(5)] + [pe(t("12:05"), 20, 200)]
    chosen = chosen_of(*evs)
    assert [o.observed_at for o, _ in chosen] == [t("12:04"), t("12:05")]
    assert chosen[0][1].players == 14


def test_exact_tie_decided_by_favorites_not_arrival_order():
    a = pe(t("12:01"), 10, 100, favorites=1)
    b = pe(t("12:01"), 10, 100, favorites=2)
    assert chosen_of(a, b)[0][1].favorites == 2
    assert chosen_of(b, a)[0][1].favorites == 2


def test_games_are_kept_apart():
    reduced = reduce_players([pe(t("12:01"), gid=1), pe(t("12:01"), gid=2), pe(t("12:01"), gid=1, platform="steam")])
    assert set(reduced) == {("roblox", 1), ("roblox", 2), ("steam", 1)}


def test_reduce_votes_latest_then_values():
    v = [VotesEvent("roblox", 1, t("12:01"), 5, 1), VotesEvent("roblox", 1, t("12:03"), 6, 1),
         VotesEvent("roblox", 1, t("12:03"), 6, 2), VotesEvent("roblox", 1, t("12:06"), 7, 2)]
    assert reduce_votes(v) == [v[2], v[3]]
    assert reduce_votes(list(reversed(v))) == [v[2], v[3]]


def test_reduce_metas_latest_per_game_tie_by_content():
    a = MetaEvent("roblox", 1, t("12:00"), {"name": "A"})
    b = MetaEvent("roblox", 1, t("12:00"), {"name": "B"})
    c = MetaEvent("roblox", 1, t("11:00"), {"name": "Z"})
    assert reduce_metas([a, b, c]) == [b] == reduce_metas([c, b, a])


# ------------------------------------------------------------------ new_visits chain


def test_chain_without_predecessor():
    chosen = chosen_of(pe(t("12:01"), 10, 100), pe(t("12:06"), 10, 130), pe(t("12:11"), 10, 150))
    assert [u.new_visits for u in score_game(chosen, [])] == [None, 30, 20]


def test_chain_with_predecessor_from_mongo():
    chosen = chosen_of(pe(t("12:01"), 10, 100), pe(t("12:06"), 10, 130))
    history = [Observation(t("11:51"), 10, 80), Observation(t("11:56"), 10, 90)]
    assert [u.new_visits for u in score_game(chosen, history)] == [10, 30]


def test_predecessor_is_strictly_before_the_first_bucket():
    # MongoDB already has an older version of the first bucket: it is not its own predecessor
    chosen = chosen_of(pe(t("12:04"), 10, 110))
    history = [Observation(t("11:56"), 10, 90), Observation(t("12:01"), 10, 100)]
    assert score_game(chosen, history)[0].new_visits == 20


def test_reset_and_gap_give_missing():
    chosen = chosen_of(pe(t("12:01"), 10, 100), pe(t("12:06"), 10, 50))
    assert [u.new_visits for u in score_game(chosen, [Observation(t("11:56"), 10, 90)])] == [10, None]
    gap = chosen_of(pe(t("12:01"), 10, 100))
    assert score_game(gap, [Observation(t("11:00"), 10, 90)])[0].new_visits is None  # 61 min
    edge = chosen_of(pe(t("12:01"), 10, 100))
    assert score_game(edge, [Observation(t("11:01"), 10, 90)])[0].new_visits == 10  # exactly 1 h


def test_steam_has_no_new_visits():
    chosen = chosen_of(pe(t("12:01"), 5, platform="steam"), pe(t("12:06"), 6, platform="steam"))
    assert [u.new_visits for u in score_game(chosen, [])] == [None, None]


# ------------------------------------------------------------------ growth


def test_growth_from_history_and_from_the_micro_batch():
    chosen = chosen_of(pe(t("13:01"), 150, 100), pe(t("13:06"), 300, 110))
    history = [Observation(t("12:01"), 100, 50)]
    updates = score_game(chosen, history)
    assert updates[0].growths == {"growth_1h": 50.0}
    assert updates[1].growths == {"growth_1h": 200.0}  # reference 12:01 is 5 min from the 12:06 target


def test_newer_version_of_a_bucket_in_the_batch_wins_over_mongo():
    # the reference bucket 12:00 is in MongoDB (12:01) and in the micro-batch (12:04, newer)
    chosen = chosen_of(pe(t("12:04"), 200, 100), pe(t("13:04"), 400, 200))
    history = [Observation(t("12:01"), 100, 90)]
    updates = score_game(chosen, history)
    assert updates[1].growths["growth_1h"] == 100.0  # against 200, not 100


def test_reference_zero_or_missing_players_gives_no_growth():
    chosen = chosen_of(pe(t("13:01"), 10, 100))
    assert score_game(chosen, [Observation(t("12:01"), 0, 50)])[0].growths == {}
    assert score_game(chosen, [Observation(t("12:01"), None, 50)])[0].growths == {}


def test_current_zero_is_minus_100():
    chosen = chosen_of(pe(t("13:01"), 0, 100))
    assert score_game(chosen, [Observation(t("12:01"), 10, 50)])[0].growths == {"growth_1h": -100.0}


# ------------------------------------------------------------------ MongoDB documents


def test_doc_to_observation():
    assert doc_to_observation(doc(t("12:01"), players=3, visits=7)) == Observation(t("12:01"), 3, 7)
    assert doc_to_observation({"platform": "roblox", "id": 1, "ts": t("12:00"), "up": 1}) is None  # votes only
    with pytest.raises(ValueError):
        doc_to_observation({"observed_at": t("12:01").replace(tzinfo=None)})


def test_score_players_uses_each_games_own_history():
    reduced = reduce_players([pe(t("13:01"), 20, gid=1), pe(t("13:01"), 20, gid=2)])
    history = history_by_game([doc(t("12:01"), players=10, gid=1), doc(t("12:01"), players=40, gid=2)])
    updates = score_players(reduced, history)
    assert [(u.event.id, u.growths["growth_1h"]) for u in updates] == [(1, 100.0), (2, -50.0)]
    assert all(u.bucket.tzinfo == timezone.utc for u in updates)
