import itertools

from gametrend.speed.microbatch import format_report, history_filters, process_batch
from gametrend.speed.scoring import reduce_players
from gametrend.speed.writes import GAME_METRICS, GAMES

from .helpers import RT, ST, FakeStore, doc, game_json, pe, steam_json, t, votes_json


def fake_timer():
    ticks = itertools.count()
    return lambda: next(ticks) * 0.01  # 10 ms per call


def test_one_query_per_platform():
    reduced = reduce_players([pe(t("12:01"), gid=1), pe(t("12:01"), gid=2), pe(t("12:01"), platform="steam")])
    filters = history_filters(reduced)
    assert [f["platform"] for f in filters] == ["roblox", "steam"]
    assert [int(i) for i in filters[0]["id"]["$in"]] == [1, 2]


def test_process_batch_end_to_end_with_a_fake_store():
    records = [
        (RT, game_json(1, t("13:01"), playing=150, visits=1000)),
        (RT, game_json(1, t("13:03"), playing=160, visits=1010)),  # same bucket, newer: wins
        (RT, game_json(1, t("13:06"), playing=170, visits=1030)),
        (RT, votes_json(1, t("13:02"), 10, 1)),
        (RT, '{"kind": "meta", "ts": "2026-11-06T13:00:00.000Z", "id": 1, "name": "A"}'),
        (RT, '{"kind": "sort", "ts": "2026-11-06T13:00:00.000Z", "id": 1, "country": "us", "sort": "s", '
             '"rank": 1, "playing": 9}'),
        (ST, steam_json(730, t("13:01"), 50)),
        (RT, "broken"),
    ]
    store = FakeStore(docs=[doc(t("12:03"), players=100, visits=900), doc(t("12:58"), players=140, visits=990),
                            doc(t("12:01"), players=25, platform="steam", gid=730)])
    report = process_batch(4, records, store, timer=fake_timer())

    ops = store.written[GAME_METRICS]
    sets = [op._doc["$set"] for op in ops]
    roblox = [s for s in sets if "visits" in s]
    assert [(s["players"], s.get("new_visits"), s.get("growth_1h")) for s in roblox] == [(160, 20, 60.0),
                                                                                         (170, 20, 70.0)]
    steam = [s for s in sets if "visits" not in s and "players" in s]
    assert steam == [{"observed_at": t("13:01"), "players": 50, "growth_1h": 100.0}]
    assert any("up" in s for s in sets)
    assert len(store.written[GAMES]) == 1

    assert report.counts == {"game": 3, "votes": 1, "meta": 1, "sort": 1, "players": 1, "invalid": 1}
    assert report.games == 2 and report.history_docs == 3
    assert len(store.filters) == 2


def test_empty_batch():
    store = FakeStore()
    report = process_batch(0, [], store, timer=fake_timer())
    assert store.filters == [] and store.written == {GAME_METRICS: [], GAMES: []}
    assert report.games == 0


def test_report_line():
    store = FakeStore(write_result={"matched": 1, "modified": 1, "upserted": 2, "rejected": 3})
    report = process_batch(9, [(ST, steam_json(730, t("13:01"), 50))], store, timer=fake_timer())
    line = format_report(report)
    assert line.startswith("batch 9: events game=0 players=1 votes=0 meta=0 sort=0 invalid=0; games 1;")
    assert "rejected (older or already written)=3" in line
    for part in ("read ", "compute ", "write ", "total "):
        assert part in line
    assert report.read_ms > 0 and report.write_ms > 0 and report.total_ms >= report.read_ms + report.write_ms
