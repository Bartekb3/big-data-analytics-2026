"""One micro-batch: Kafka records -> history read -> scoring -> guarded writes, plus its log line.

MongoDB is reached only through `store` (read_history, write), so this runs in tests without MongoDB or Spark.
The timer only measures durations for the log; no processing logic depends on it.
"""

import time
from collections import Counter
from dataclasses import dataclass, field

from .parse import parse_records
from .scoring import history_by_game, reduce_metas, reduce_players, reduce_votes, score_players
from .windows import game_ranges, history_filter
from .writes import GAME_METRICS, GAMES, meta_op, player_op, votes_op

_KINDS = ("game", "players", "votes", "meta", "sort", "invalid")


@dataclass
class BatchReport:
    batch_id: int
    counts: Counter
    games: int = 0
    history_docs: int = 0
    read_ms: float = 0.0
    write_ms: float = 0.0
    total_ms: float = 0.0
    writes: dict = field(default_factory=dict)  # collection -> summarize() counts

    @property
    def compute_ms(self) -> float:
        return max(0.0, self.total_ms - self.read_ms - self.write_ms)


def history_filters(reduced) -> list[dict]:
    """One history query per platform for the games of the micro-batch."""
    by_platform: dict[str, tuple[set, list]] = {}
    for (platform, gid), chosen in reduced.items():
        ids, ranges = by_platform.setdefault(platform, (set(), []))
        ids.add(gid)
        ranges.extend(game_ranges([o for o, _ in chosen]))
    filters = [history_filter(p, ids, ranges) for p, (ids, ranges) in sorted(by_platform.items())]
    return [f for f in filters if f is not None]


def process_batch(batch_id: int, records, store, timer=time.monotonic) -> BatchReport:
    t0 = timer()
    parsed = parse_records(records)
    report = BatchReport(batch_id, parsed.counts)

    reduced = reduce_players(parsed.players)
    report.games = len(reduced)

    t_read = timer()
    docs = []
    for flt in history_filters(reduced):
        docs.extend(store.read_history(flt))
    report.read_ms = (timer() - t_read) * 1000
    report.history_docs = len(docs)

    updates = score_players(reduced, history_by_game(docs))
    metric_ops = [player_op(u) for u in updates] + [votes_op(v) for v in reduce_votes(parsed.votes)]
    game_ops = [meta_op(m) for m in reduce_metas(parsed.metas)]

    t_write = timer()
    report.writes[GAME_METRICS] = store.write(GAME_METRICS, metric_ops)
    report.writes[GAMES] = store.write(GAMES, game_ops)
    report.write_ms = (timer() - t_write) * 1000
    report.total_ms = (timer() - t0) * 1000
    return report


def _writes_text(name: str, w: dict) -> str:
    return (f"{name} matched={w.get('matched', 0)} modified={w.get('modified', 0)} upserted={w.get('upserted', 0)} "
            f"rejected (older or already written)={w.get('rejected', 0)}")


def format_report(r: BatchReport) -> str:
    kinds = " ".join(f"{k}={r.counts.get(k, 0)}" for k in _KINDS)
    writes = "; ".join(_writes_text(name, w) for name, w in r.writes.items())
    return (f"batch {r.batch_id}: events {kinds}; games {r.games}; history {r.history_docs} docs; "
            f"read {r.read_ms:.0f} ms, compute {r.compute_ms:.0f} ms, write {r.write_ms:.0f} ms, "
            f"total {r.total_ms:.0f} ms; {writes}")
