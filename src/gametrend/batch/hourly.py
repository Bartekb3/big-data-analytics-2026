"""Hourly aggregates of one game (game_metrics_hourly). Pure logic, no Spark.

Fields and semantics: architecture/contracts/mongodb-collections.md#game_metrics_hourly; the metric
functions come from common, called as described in architecture/common/README.md.

The Spark side (aggregate.py) passes the events of one game for the hours being computed plus the hour
before them (only the predecessor for `new_visits`), with exact copies already removed. Same key with
different values is resolved here, by the deterministic rules of the metric definitions.
"""

from datetime import datetime

from ..common.metrics import Observation, hour_start, hourly_new_visits, latest_per_bucket, visit_deltas
from ..common.normalize import to_python
from .hours import from_micros, to_micros

# Columns aggregate.py hands over per game; timestamps as epoch microseconds (no time-zone conversion)
INPUT_COLUMNS = ("platform", "id", "kind", "ts_us", "playing", "players", "visits", "updated_us", "up", "down")
INPUT_KINDS = ("game", "players", "votes")  # `sort` and `meta` carry no hourly field

# One row per (game, hour); timestamps as epoch microseconds, absent values null
OUTPUT_SCHEMA = ("platform string, id bigint, hour_us bigint, players bigint, peak_players bigint, "
                 "new_visits bigint, updated_us bigint, up bigint, down bigint")
OUTPUT_COLUMNS = tuple(part.split()[0] for part in OUTPUT_SCHEMA.split(", "))
METRIC_FIELDS = ("players", "peak_players", "new_visits", "updated", "up", "down")

_MAX_EXACT = 2**53  # Spark hands nullable bigint to pandas as float64: exact only up to here


def to_int(value) -> int | None:
    """pandas/numpy cell -> int or None. A float must be integral and exact, else ValueError (fail loudly)."""
    value = to_python(value)
    if value is None or isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer() and abs(value) <= _MAX_EXACT:
        return int(value)
    raise ValueError(f"expected an integer, got {value!r}")


def frame_events(frame) -> list[dict]:
    """pandas frame (INPUT_COLUMNS) -> events with plain Python values; absent values are None."""
    events = []
    for rec in frame.to_dict("records"):
        updated = to_int(rec.get("updated_us"))
        events.append({
            "kind": rec["kind"],
            "ts": from_micros(to_int(rec["ts_us"])),
            "playing": to_int(rec.get("playing")),
            "players": to_int(rec.get("players")),
            "visits": to_int(rec.get("visits")),
            "updated": None if updated is None else from_micros(updated),
            "up": to_int(rec.get("up")),
            "down": to_int(rec.get("down")),
        })
    return events


def _observation(event: dict) -> Observation | None:
    if event["kind"] == "game":
        return Observation(event["ts"], event["playing"], event["visits"])
    if event["kind"] == "players":
        return Observation(event["ts"], event["players"])
    return None


def _latest(events, key):
    """The event with the latest ts; equal ts decided by the values (never by input order)."""
    return max(events, key=key, default=None)


def aggregate_game(events: list[dict], hours: list[datetime]) -> list[dict]:
    """Hourly values of one game for each hour in `hours` (ascending, consecutive).

    `events` may start up to one hour before `hours[0]`: that hour supplies the predecessor of the
    first `new_visits` delta; its own values are not returned. Returns one dict per hour that has at
    least one metric field: {"hour_ts", and the present fields of METRIC_FIELDS}.
    """
    wanted = set(hours)
    observations = [o for o in map(_observation, events) if o is not None]
    buckets = latest_per_bucket(observations)
    deltas = visit_deltas(buckets)  # one chain over the whole window equals per-hour chaining

    per_hour: dict[datetime, dict] = {h: {"buckets": [], "deltas": [], "peak": None, "game": [], "votes": []}
                                      for h in wanted}
    for obs in observations:  # every observation of the hour, not only the last one of each bucket
        slot = per_hour.get(hour_start(obs.observed_at))
        if slot is not None and obs.players is not None:
            slot["peak"] = obs.players if slot["peak"] is None else max(slot["peak"], obs.players)
    for obs, delta in zip(buckets, deltas):
        slot = per_hour.get(hour_start(obs.observed_at))
        if slot is not None:
            slot["buckets"].append(obs)
            slot["deltas"].append(delta)
    for e in events:
        slot = per_hour.get(hour_start(e["ts"]))
        if slot is not None and e["kind"] in ("game", "votes"):
            slot[e["kind"]].append(e)

    out = []
    for h in sorted(wanted):
        slot = per_hour[h]
        values = {}
        with_players = [o for o in slot["buckets"] if o.players is not None]
        if with_players:
            values["players"] = with_players[-1].players  # buckets are in time order
        if slot["peak"] is not None:
            values["peak_players"] = slot["peak"]
        new_visits = hourly_new_visits(slot["deltas"])
        if new_visits is not None:
            values["new_visits"] = new_visits
        last_game = _latest([e for e in slot["game"] if e["updated"] is not None],
                            key=lambda e: (e["ts"], e["updated"]))
        if last_game is not None:
            values["updated"] = last_game["updated"]
        last_votes = _latest([e for e in slot["votes"] if e["up"] is not None and e["down"] is not None],
                             key=lambda e: (e["ts"], e["up"], e["down"]))
        if last_votes is not None:
            values["up"], values["down"] = last_votes["up"], last_votes["down"]
        if values:
            out.append({"hour_ts": h, **values})
    return out


def aggregate_frame(frame, hours: list[datetime]):
    """The `applyInPandas` function: events of one game -> one row per hour (OUTPUT_COLUMNS)."""
    import pandas as pd

    if len(frame) == 0:
        return pd.DataFrame({c: pd.Series([], dtype="Int64" if c != "platform" else object) for c in OUTPUT_COLUMNS})
    platform, gid = frame["platform"].iloc[0], to_int(frame["id"].iloc[0])
    rows = []
    for doc in aggregate_game(frame_events(frame), hours):
        rows.append({
            "platform": platform, "id": gid, "hour_us": to_micros(doc["hour_ts"]),
            "players": doc.get("players"), "peak_players": doc.get("peak_players"),
            "new_visits": doc.get("new_visits"),
            "updated_us": to_micros(doc["updated"]) if "updated" in doc else None,
            "up": doc.get("up"), "down": doc.get("down"),
        })
    data = {c: pd.Series([r[c] for r in rows], dtype=object if c == "platform" else "Int64") for c in OUTPUT_COLUMNS}
    return pd.DataFrame(data, columns=list(OUTPUT_COLUMNS))


def to_document(row: dict) -> dict:
    """One output row (OUTPUT_COLUMNS, plain values) -> game_metrics_hourly fields; absent stays absent.

    BSON types (Long) are applied by the MongoDB writer."""
    doc = {"platform": row["platform"], "id": int(row["id"]), "hour_ts": from_micros(row["hour_us"])}
    for name in ("players", "peak_players", "new_visits", "up", "down"):
        if row.get(name) is not None:
            doc[name] = int(row[name])
    if row.get("updated_us") is not None:
        doc["updated"] = from_micros(row["updated_us"])
    return doc


def group_by_hour(docs: list[dict]) -> dict[datetime, list[dict]]:
    out: dict[datetime, list[dict]] = {}
    for doc in docs:
        out.setdefault(doc["hour_ts"], []).append(doc)
    return out
