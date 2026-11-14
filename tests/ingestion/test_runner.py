import io
import json

from gametrend.ingestion import config
from gametrend.ingestion.http import Result
from gametrend.ingestion.runner import Producers
from gametrend.ingestion.sink import PrintSink

from .conftest import FakeClient, FakeClock, FakeSink, load, mocked, ok

OBS1 = "2026-11-06T12:00:01.000Z"
OBS2 = "2026-11-06T12:01:40.000Z"


def make(mode="mock", deliver=True):
    clock = FakeClock()
    client = FakeClient(clock)
    sink = FakeSink(deliver)
    p = Producers(config.Settings(mode=mode, countries=("us",)), client, sink, clock)
    return p, client, sink


def games_body(observed_at, n=2):
    return {"data": [mocked(it, observed_at) for it in load("roblox_games")["data"][:n]]}


# ------------------------------------------------------------------ stream sources


def test_games_answer_sends_game_and_meta_then_only_new_observations():
    p, client, sink = make()
    client.script("roblox_games", ok(games_body(OBS1)), ok(games_body(OBS1)), ok(games_body(OBS2)))
    p.poll_batch("roblox_games", [1, 2])
    assert sink.kinds() == ["game", "meta", "game", "meta"]
    assert {t for t, _ in sink.sent} == {"roblox.snapshots"}
    p.poll_batch("roblox_games", [1, 2])  # the same observations again: nothing, not counted
    assert len(sink.sent) == 4
    p.poll_batch("roblox_games", [1, 2])  # new observations, unchanged metadata: game events only
    assert sink.kinds()[4:] == ["game", "game"]
    assert client.calls[0] == ("roblox_games", {"universeIds": "1,2"})
    counts = p.stats.take()["roblox_games"]
    assert counts == {"requests": 3, "sent": 6, "dropped": 0, "errors": 0, "failed": 0}


def test_real_mode_uses_request_time_as_ts():
    p, client, sink = make(mode="real")
    client.script("roblox_votes", ok(load("roblox_votes")))
    p.poll_batch("roblox_votes", [1])
    assert {e["ts"] for _, e in sink.sent} == {"2026-11-06T12:00:00.000Z"}


def test_invalid_records_are_dropped_and_counted():
    p, client, sink = make()
    item = mocked({"id": 9, "upVotes": None, "downVotes": 3}, OBS1)
    client.script("roblox_votes", ok({"data": [item, None]}))
    p.poll_batch("roblox_votes", [9])
    assert sink.sent == []
    assert p.stats.take()["roblox_votes"]["dropped"] == 2


def test_errors_back_off_and_success_resets():
    p, client, sink = make()
    client.script("steam_players", Result(429, {}), Result(None, error="ReadTimeout"),
                  ok(mocked({"response": {"player_count": 5, "result": 1}}, OBS1)))
    p.poll_batch("steam_players", [440])
    first_penalty = p._penalty_until["steam_players"]
    assert 0.5 <= first_penalty <= 1.0
    p.poll_batch("steam_players", [440])
    assert p._attempts["steam_players"] == 2
    p.poll_batch("steam_players", [440])
    assert p._attempts["steam_players"] == 0
    assert sink.sent == [("steam.players", {"kind": "players", "ts": OBS1, "id": 440, "players": 5})]
    assert client.calls[0] == ("steam_players", {"appid": 440})
    assert p.stats.take()["steam_players"]["errors"] == 2


def test_steam_404_gives_no_event_and_no_error():
    p, client, sink = make()
    client.script("steam_players", Result(404, {"response": {"result": 42}}))
    p.poll_batch("steam_players", [1])
    assert sink.sent == [] and p.stats.take()["steam_players"]["errors"] == 0


def test_end_of_archive_stops_the_producers():
    p, client, _ = make()
    client.script("roblox_games", Result(503, {}, {"X-Mock-Reason": "end"}))
    p.poll_batch("roblox_games", [1])
    assert p.stop.is_set()


def test_failed_delivery_is_counted_and_metadata_is_sent_again():
    p, client, sink = make(deliver=False)
    client.script("roblox_games", ok(games_body(OBS1, 1)), ok(games_body(OBS2, 1)))
    p.poll_batch("roblox_games", [1])
    p.poll_batch("roblox_games", [1])
    assert sink.kinds() == ["game", "meta", "game", "meta"]
    assert p.stats.take()["roblox_games"]["failed"] == 4


# ------------------------------------------------------------------ IDs and appdetails


def test_refresh_ids_fills_rounds_and_details_queue():
    p, client, _ = make()
    client.script("_mock_ids", ok({"roblox": [1, 2, 3], "steam": [440, 730]}))
    assert p.refresh_ids()
    assert p.rounds["roblox_games"].next_batch() == [1, 2, 3]
    assert p.rounds["roblox_votes"].next_batch() == [1, 2, 3]
    assert p.rounds["steam_players"].next_batch() == [440]
    assert p.details_pending == {440, 730}


def test_details_once_per_game_and_retried_after_404():
    p, client, sink = make()
    details = load("steam_appdetails")
    app = {"3241660": mocked(details["3241660"], OBS1)}
    p.details_pending = {3241660, 99}
    client.script("steam_appdetails", Result(404, {}, {"X-Mock-Reason": "unknown-id"}), ok(app))
    p.fetch_details()  # 99 first (sorted): 404, stays pending; then 3241660: sent
    assert p.details_pending == {99} and p.details_done == {3241660}
    assert sink.sent[0][0] == "steam.players" and sink.sent[0][1]["kind"] == "meta"
    client.script("_mock_ids", ok({"roblox": [], "steam": [3241660, 99]}))
    p.refresh_ids()
    assert p.details_pending == {99}  # a game with details is not asked again


# ------------------------------------------------------------------ sorts


def sorts_page(observed_at, token):
    body = mocked(load("roblox_sorts"), observed_at)
    body["nextSortsPageToken"] = token
    return body


def test_sorts_cycle_follows_tokens_and_skips_a_repeated_cycle():
    p, client, sink = make()
    client.script("roblox_sorts", ok(sorts_page(OBS1, "c-p1")), ok(sorts_page(OBS1, "")),
                  ok(sorts_page(OBS1, "c-p1")))
    p.sorts_cycle("us")
    assert [c[1].get("sortsPageToken") for c in client.calls] == [None, "c-p1"]
    assert client.calls[0][1]["country"] == "us"
    n = len(sink.sent)
    assert n > 0 and set(sink.kinds()) == {"sort"}
    p.sorts_cycle("us")  # the same cycle: only the first page is asked, nothing sent
    assert len(client.calls) == 3 and len(sink.sent) == n


def test_sorts_stale_token_restarts_from_the_first_page():
    p, client, sink = make()
    client.script("roblox_sorts", ok(sorts_page(OBS1, "old-p1")), Result(400, {}, {"X-Mock-Reason": "unknown-token"}),
                  ok(sorts_page(OBS2, "")))
    p.sorts_cycle("us")
    assert [c[1].get("sortsPageToken") for c in client.calls] == [None, "old-p1", None]
    assert {e["ts"] for _, e in sink.sent} == {OBS2}


def test_sorts_errors_retry_then_give_up():
    p, client, sink = make()
    client.script("roblox_sorts", Result(500, {}))
    p.sorts_cycle("us")
    assert len(client.calls) == 5 and sink.sent == []
    assert p.stats.take()["roblox_sorts"]["errors"] == 5


# ------------------------------------------------------------------ loop and sink


def test_stream_loop_spaces_requests_and_stops(monkeypatch):
    p, client, _ = make()
    p.rounds["roblox_games"].refresh(list(range(120)))  # 3 batches of 50
    submitted = []

    class Pool:
        def submit(self, fn, *args):
            submitted.append((p.clock.monotonic(), args[1], args[2]))
            if len(submitted) == 4:
                p.stop.set()

    p.stream_loop("roblox_games", Pool())
    times = [t for t, _, _ in submitted]
    assert [b[0] for _, _, b in submitted] == [0, 50, 100, 0]
    assert times == [0, 14, 28, 42]  # 60 s / 3 requests = 20 s, capped at 14 s


def test_print_sink_writes_topic_and_json():
    out = io.StringIO()
    flags = []
    PrintSink(out).send("steam.players", {"kind": "players", "id": 1}, flags.append)
    topic, payload = out.getvalue().rstrip("\n").split("\t")
    assert topic == "steam.players" and json.loads(payload) == {"kind": "players", "id": 1} and flags == [True]
