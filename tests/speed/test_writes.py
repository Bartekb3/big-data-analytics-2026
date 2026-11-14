import pytest
from bson.int64 import Int64
from pymongo import UpdateOne

from gametrend.speed.parse import MetaEvent, VotesEvent
from gametrend.speed.scoring import PlayerUpdate
from gametrend.speed.writes import (META_FIELDS, VOTES_FIELDS, WriteFailed, guarded_upsert, meta_op, player_op,
                                    summarize, votes_op)

from .helpers import pe, t


def parts(op: UpdateOne):
    return op._filter, op._doc, op._upsert


def test_player_op_sets_present_and_unsets_absent_fields_of_its_group():
    e = pe(t("12:01"), players=10, visits=2**40, favorites=3)
    flt, doc, upsert = parts(player_op(PlayerUpdate(e, t("12:00"), new_visits=None, growths={"growth_1h": 5.0})))
    assert upsert
    assert flt == {"platform": "roblox", "id": 1, "ts": t("12:00"), "observed_at": {"$not": {"$gte": t("12:01")}}}
    assert doc["$set"] == {"observed_at": t("12:01"), "players": 10, "visits": 2**40, "favorites": 3,
                           "updated": t("00:00"), "growth_1h": 5.0}
    assert doc["$unset"] == {"new_visits": "", "growth_3h": "", "growth_24h": ""}
    # the vote fields belong to another group: never touched by a game event
    assert not set(VOTES_FIELDS) & (set(doc["$set"]) | set(doc["$unset"]))


def test_bson_types():
    e = pe(t("12:01"), players=10, visits=5, favorites=3)
    flt, doc, _ = parts(player_op(PlayerUpdate(e, t("12:00"), new_visits=2)))
    assert isinstance(flt["id"], Int64)
    assert isinstance(doc["$set"]["visits"], Int64) and isinstance(doc["$set"]["favorites"], Int64)
    assert type(doc["$set"]["players"]) is int and type(doc["$set"]["new_visits"]) is int
    _, vdoc, _ = parts(votes_op(VotesEvent("roblox", 1, t("12:01"), 5, 1)))
    assert isinstance(vdoc["$set"]["up"], Int64) and isinstance(vdoc["$set"]["down"], Int64)


def test_steam_player_op_has_only_steam_fields():
    e = pe(t("12:01"), players=0, platform="steam", gid=730)
    _, doc, _ = parts(player_op(PlayerUpdate(e, t("12:00"))))
    assert doc["$set"] == {"observed_at": t("12:01"), "players": 0}  # zero is kept, not unset
    assert doc["$unset"] == {"growth_1h": "", "growth_3h": "", "growth_24h": ""}


def test_votes_op_uses_its_own_guard_and_bucket():
    flt, doc, _ = parts(votes_op(VotesEvent("roblox", 1, t("12:04:59"), 5, 1)))
    assert flt["ts"] == t("12:00")
    assert flt["votes_observed_at"] == {"$not": {"$gte": t("12:04:59")}}
    assert "observed_at" not in flt
    assert doc == {"$set": {"votes_observed_at": t("12:04:59"), "up": 5, "down": 1}}  # no $unset: group is full


def test_meta_op_unsets_missing_optional_fields():
    flt, doc, _ = parts(meta_op(MetaEvent("roblox", 7, t("12:01"), {"name": "A", "genre_l1": "RPG"})))
    assert flt == {"platform": "roblox", "id": 7, "observed_at": {"$not": {"$gte": t("12:01")}}}
    assert "ts" not in flt
    assert doc["$set"] == {"observed_at": t("12:01"), "name": "A", "genre_l1": "RPG"}
    assert set(doc["$unset"]) == set(META_FIELDS["roblox"]) - {"name", "genre_l1"}


def test_guarded_upsert_rejects_fields_outside_the_group():
    with pytest.raises(ValueError):
        guarded_upsert({"id": 1}, "observed_at", t("12:00"), {"up": 1}, ("players",))


def test_summarize_success_and_rejected():
    assert summarize({}) == {"matched": 0, "modified": 0, "upserted": 0, "rejected": 0}
    ok = {"nMatched": 3, "nModified": 2, "nUpserted": 4, "writeErrors": [], "writeConcernErrors": []}
    assert summarize(ok) == {"matched": 3, "modified": 2, "upserted": 4, "rejected": 0}
    dup = {"nMatched": 1, "nModified": 1, "nUpserted": 0,
           "writeErrors": [{"index": 0, "code": 11000, "errmsg": "E11000"}, {"index": 2, "code": 11000}]}
    assert summarize(dup)["rejected"] == 2


def test_summarize_raises_on_other_errors():
    mixed = {"writeErrors": [{"index": 0, "code": 11000}, {"index": 1, "code": 121, "errmsg": "validation"}]}
    with pytest.raises(WriteFailed, match="121"):
        summarize(mixed)
    with pytest.raises(WriteFailed):
        summarize({"writeErrors": [], "writeConcernErrors": [{"code": 64, "errmsg": "timeout"}]})
