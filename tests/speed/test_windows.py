from datetime import timedelta

import pytest
from bson.int64 import Int64

from gametrend.common.metrics import Observation, bucket_start
from gametrend.speed.windows import game_ranges, growth_range, history_filter, merge_ranges, predecessor_range

from .helpers import t

H = timedelta(hours=1)


def test_predecessor_range_is_one_hour_and_five_minutes_before_the_first_bucket():
    first = Observation(t("12:03"), players=1, visits=1)
    assert predecessor_range(first) == (t("10:55"), t("12:00"))


def test_growth_range_bounds():
    first = Observation(t("12:03"), players=1)
    last = Observation(t("12:27"), players=1)
    # first observed_at - h - 15 min tolerance - 5 min ts lag .. last observed_at - h + 15 min
    assert growth_range(first, last, H) == (t("10:43"), t("11:42"))
    assert growth_range(first, last, 24 * H) == (t("11:43", day=5), t("12:42", day=5))


def test_references_at_both_tolerance_edges_lie_in_the_range():
    first = last = Observation(t("12:04:59"), players=1)
    start, end = growth_range(first, last, H)
    for ref_observed_at in (t("10:49:59"), t("11:19:59")):  # observed_at - 1 h -+ 15 min
        assert start <= bucket_start(ref_observed_at) <= end


def test_game_ranges_steam_has_no_predecessor():
    roblox = game_ranges([Observation(t("12:03"), players=1, visits=5)])
    steam = game_ranges([Observation(t("12:03"), players=1)])
    assert len(roblox) == 4 and len(steam) == 3
    assert game_ranges([]) == []


def test_merge_normal_operation_gives_few_ranges():
    # many games covering the same ~24 minutes: their ranges collapse into one per horizon (+ predecessor)
    ranges = []
    for minute in range(0, 4):
        first = Observation(t(f"12:0{minute}"), players=1, visits=1)
        last = Observation(t(f"12:2{minute}"), players=1, visits=1)
        ranges += game_ranges([first, last])
    merged = merge_ranges(ranges)
    # 1 h range and predecessor range overlap -> 3 ranges (24 h, 3 h, 1 h + predecessor)
    assert len(merged) == 3
    assert merged[-1] == (t("10:40"), t("12:00"))


def test_merge_one_late_event_adds_a_narrow_range_only():
    normal = game_ranges([Observation(t("12:00"), players=1, visits=1), Observation(t("12:20"), players=1, visits=1)])
    late = game_ranges([Observation(t("12:00", day=4), players=1, visits=1)])
    merged = merge_ranges(normal + late)
    assert merged[0][0] == t("11:40", day=3)  # its 24 h reference range
    total = sum((e - s for s, e in merged), timedelta())
    assert total < 10 * H  # not two days of history


def test_merge_disjoint_and_touching():
    a = (t("10:00"), t("10:10"))
    b = (t("10:10"), t("10:20"))  # touches a: closed ranges merge
    c = (t("11:00"), t("11:05"))
    d = (t("10:05"), t("10:07"))  # inside a
    assert merge_ranges([c, b, d, a]) == [(t("10:00"), t("10:20")), c]
    assert merge_ranges([]) == []
    with pytest.raises(ValueError):
        merge_ranges([(t("10:10"), t("10:00"))])


def test_history_filter():
    flt = history_filter("roblox", [3, 1, 3], [(t("10:00"), t("10:10")), (t("10:05"), t("10:20"))])
    assert flt == {"platform": "roblox", "id": {"$in": [Int64(1), Int64(3)]},
                   "$or": [{"ts": {"$gte": t("10:00"), "$lte": t("10:20")}}]}
    assert all(isinstance(i, Int64) for i in flt["id"]["$in"])
    assert history_filter("roblox", [], [(t("10:00"), t("10:10"))]) is None
    assert history_filter("roblox", [1], []) is None
