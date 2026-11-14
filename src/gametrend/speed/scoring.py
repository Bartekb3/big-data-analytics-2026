"""Per micro-batch reduction and scoring (architecture/speed/README.md, Buckets and Scoring).

Pure functions: events of one micro-batch plus the history read from MongoDB in, updates out.
Metric semantics come from gametrend.common.metrics, so speed and batch compute them identically.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ..common.metrics import Observation, bucket_start, growths, last_with_visits, latest_per_bucket, visit_deltas
from .parse import MetaEvent, PlayerEvent, VotesEvent

GameKey = tuple[str, int]


@dataclass(frozen=True)
class PlayerUpdate:
    """The player data of one bucket of one game, with its derived fields (absent ones missing)."""

    event: PlayerEvent
    bucket: datetime
    new_visits: int | None = None
    growths: dict = field(default_factory=dict)


def _none_lowest(value):
    return (0, 0) if value is None else (1, value)


def _full_rank(e: PlayerEvent):
    return (e.observed_at, _none_lowest(e.players), _none_lowest(e.visits), _none_lowest(e.favorites),
            _none_lowest(e.updated))


def to_observation(e: PlayerEvent) -> Observation:
    return Observation(e.observed_at, players=e.players, visits=e.visits)


def doc_to_observation(doc: dict) -> Observation | None:
    """A `game_metrics` document -> Observation; None for a document without player data (votes only)."""
    observed_at = doc.get("observed_at")
    if observed_at is None:
        return None
    if isinstance(observed_at, datetime) and observed_at.tzinfo is None:
        raise ValueError("naive observed_at from MongoDB: open the client with tz_aware=True")
    players = doc.get("players")
    visits = doc.get("visits")
    return Observation(observed_at.astimezone(timezone.utc),
                       players=None if players is None else int(players),
                       visits=None if visits is None else int(visits))


def reduce_players(events) -> dict[GameKey, list[tuple[Observation, PlayerEvent]]]:
    """One player event per game and 5-minute bucket (common `latest_per_bucket` rule), in time order.

    An exact tie on the contract's rule (observed_at, players, visits) is decided by favorites, then
    updated, so the result never depends on arrival order.
    """
    by_game: dict[GameKey, list[PlayerEvent]] = defaultdict(list)
    for e in events:
        by_game[(e.platform, e.id)].append(e)
    out = {}
    for key, evs in by_game.items():
        evs = sorted(evs, key=_full_rank, reverse=True)  # latest_per_bucket keeps the first of equal rank
        obs = [to_observation(e) for e in evs]
        origin = {id(o): e for o, e in zip(obs, evs)}
        out[key] = [(o, origin[id(o)]) for o in latest_per_bucket(obs)]
    return out


def reduce_votes(events) -> list[VotesEvent]:
    """One votes event per game and bucket: the latest observed_at; a tie goes to higher up, then down."""
    best: dict[tuple, VotesEvent] = {}
    for e in events:
        key = (e.platform, e.id, bucket_start(e.observed_at))
        cur = best.get(key)
        if cur is None or (e.observed_at, e.up, e.down) > (cur.observed_at, cur.up, cur.down):
            best[key] = e
    return [best[k] for k in sorted(best)]


def _meta_rank(e: MetaEvent):
    return e.observed_at, json.dumps(e.fields, sort_keys=True, default=str)


def reduce_metas(events) -> list[MetaEvent]:
    """One meta event per game: the latest observed_at; a tie goes to the larger serialized content."""
    best: dict[GameKey, MetaEvent] = {}
    for e in events:
        key = (e.platform, e.id)
        cur = best.get(key)
        if cur is None or _meta_rank(e) > _meta_rank(cur):
            best[key] = e
    return [best[k] for k in sorted(best)]


def score_game(chosen: list[tuple[Observation, PlayerEvent]], history) -> list[PlayerUpdate]:
    """`new_visits` and growth for the micro-batch buckets of one game, computed as one chain.

    `chosen`: output of `reduce_players` for the game. `history`: Observations of the game read from
    MongoDB (any order; may include versions of the micro-batch's own buckets).
    """
    if not chosen:
        return []
    history = latest_per_bucket(history)
    buckets = [o for o, _ in chosen]
    first = buckets[0].bucket
    previous = last_with_visits([h for h in history if h.bucket < first])
    deltas = visit_deltas(buckets, previous)
    merged = latest_per_bucket(history + buckets)  # a newer version of a bucket wins
    gs = growths(buckets, merged)
    return [PlayerUpdate(e, o.bucket, new_visits=d, growths=g) for (o, e), d, g in zip(chosen, deltas, gs)]


def history_by_game(docs) -> dict[GameKey, list[Observation]]:
    out: dict[GameKey, list[Observation]] = defaultdict(list)
    for doc in docs:
        obs = doc_to_observation(doc)
        if obs is not None:
            out[(doc["platform"], int(doc["id"]))].append(obs)
    return out


def score_players(reduced: dict[GameKey, list], history: dict[GameKey, list[Observation]]) -> list[PlayerUpdate]:
    updates = []
    for key in sorted(reduced):
        updates.extend(score_game(reduced[key], history.get(key, [])))
    return updates
