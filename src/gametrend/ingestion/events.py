"""Pure parsing: API response body -> observations with candidate events.

Field mapping: architecture/ingestion/README.md (API sources and cadence); event format:
architecture/contracts/kafka-topics.md. Events are not validated here (see gametrend.common.kafka_contract); a missing value is
an absent field, never 0 or "" (contracts/conventions.md).

`request_ts` is the event time on the real APIs (request time from the injected clock). In mock mode it is
None and the time is the unit's `_mock.observed_at` (architecture/mockapi/README.md, Time).
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Observation:
    """One unit of observed data (a game item, a Steam answer, a sorts page) and the events built from it."""
    key: tuple  # repeat-check key, e.g. ("roblox_games", id)
    ts: str
    events: list[dict] = field(default_factory=list)


@dataclass
class Parsed:
    observations: list[Observation] = field(default_factory=list)
    dropped: int = 0  # units that could not be read at all (no id, no time, not an object)


def iso_utc(value) -> str | None:
    """ISO-8601 string with any offset -> UTC with millisecond precision and Z; None if not parseable."""
    if not isinstance(value, str) or not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


_STEAM_DATE_FORMATS = ("%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y")


def steam_release_date(release_date) -> str | None:
    """Steam `release_date` object -> YYYY-MM-DD; None when unreleased or not a full date ("To be announced")."""
    if not isinstance(release_date, dict) or release_date.get("coming_soon"):
        return None
    text = release_date.get("date")
    if not isinstance(text, str):
        return None
    for fmt in _STEAM_DATE_FORMATS:
        try:
            return datetime.strptime(text.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


def unit_ts(obj: dict, request_ts: str | None) -> str | None:
    if request_ts is not None:
        return request_ts
    mock = obj.get("_mock")
    return iso_utc(mock.get("observed_at")) if isinstance(mock, dict) else None


def _put(event: dict, name: str, value) -> None:
    """Optional field: set only when the API has a value (missing is not zero, not "")."""
    if value is not None and value != "" and value != []:
        event[name] = value


def _required(event: dict, item: dict, api_name: str, name: str | None = None) -> None:
    """Required field: copied when present; a missing one makes validation drop the event."""
    if api_name in item:
        event[name or api_name] = item[api_name]


def _int_id(value) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _items(body) -> list | None:
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        return None
    return body["data"]


# ------------------------------------------------------------------ Roblox


def parse_roblox_games(body, request_ts: str | None) -> Parsed:
    """`/v1/games` -> per game: a `game` event and a `meta` candidate (sent only when it changed)."""
    out = Parsed()
    items = _items(body)
    if items is None:
        out.dropped = 1
        return out
    for item in items:
        gid = _int_id(item.get("id")) if isinstance(item, dict) else None
        ts = unit_ts(item, request_ts) if gid is not None else None
        if ts is None:
            out.dropped += 1
            continue
        game = {"kind": "game", "ts": ts, "id": gid}
        _required(game, item, "playing")
        _required(game, item, "visits")
        _required(game, item, "favoritedCount", "favorites")
        if "updated" in item:
            game["updated"] = iso_utc(item["updated"]) or item["updated"]  # unparseable -> validation drops it
        meta = {"kind": "meta", "ts": ts, "id": gid}
        _required(meta, item, "name")
        for name in ("genre_l1", "genre_l2", "description"):
            _put(meta, name, item.get(name))
        _put(meta, "created", iso_utc(item.get("created")))
        out.observations.append(Observation(("roblox_games", gid), ts, [game, meta]))
    return out


def parse_roblox_votes(body, request_ts: str | None) -> Parsed:
    out = Parsed()
    items = _items(body)
    if items is None:
        out.dropped = 1
        return out
    for item in items:
        gid = _int_id(item.get("id")) if isinstance(item, dict) else None
        ts = unit_ts(item, request_ts) if gid is not None else None
        if ts is None:
            out.dropped += 1
            continue
        votes = {"kind": "votes", "ts": ts, "id": gid}
        _required(votes, item, "upVotes", "up")
        _required(votes, item, "downVotes", "down")
        out.observations.append(Observation(("roblox_votes", gid), ts, [votes]))
    return out


def parse_roblox_sorts_page(body, country: str, request_ts: str | None) -> Parsed:
    """One page of explore sorts -> one `sort` event per game per sort of contentType `Games`.

    rank = position of the game in its sort's `games[]`, starting at 1. A game in several sorts gives one
    event per sort.
    """
    out = Parsed()
    if not isinstance(body, dict) or not isinstance(body.get("sorts"), list):
        out.dropped = 1
        return out
    ts = unit_ts(body, request_ts)
    if ts is None:
        out.dropped = 1
        return out
    events = []
    for sort in body["sorts"]:
        if not isinstance(sort, dict) or sort.get("contentType") != "Games":
            continue
        games = sort.get("games") if isinstance(sort.get("games"), list) else []
        for rank, game in enumerate(games, start=1):
            gid = _int_id(game.get("universeId")) if isinstance(game, dict) else None
            if gid is None:
                out.dropped += 1
                continue
            event = {"kind": "sort", "ts": ts, "id": gid, "country": country, "rank": rank}
            _required(event, sort, "sortId", "sort")
            _required(event, game, "playerCount", "playing")
            events.append(event)
    out.observations.append(Observation(("roblox_sorts", country), ts, events))
    return out


# ------------------------------------------------------------------ Steam


def parse_steam_players(body, appid: int, request_ts: str | None) -> Parsed:
    """`GetNumberOfCurrentPlayers` -> a `players` event; `result: 42` (no stats) -> no event, not a drop."""
    out = Parsed()
    resp = body.get("response") if isinstance(body, dict) else None
    ts = unit_ts(body, request_ts) if isinstance(resp, dict) else None
    if ts is None:
        out.dropped = 1
        return out
    if resp.get("result") == 42:
        return out  # the game has no player stats: missing is not zero
    event = {"kind": "players", "ts": ts, "id": appid}
    _required(event, resp, "player_count", "players")
    out.observations.append(Observation(("steam_players", appid), ts, [event]))
    return out


def parse_steam_appdetails(body, appid: int, request_ts: str | None) -> Parsed:
    """`appdetails` -> a Steam `meta` candidate; `success: false` (no details) -> no event, not a drop."""
    out = Parsed()
    app = body.get(str(appid)) if isinstance(body, dict) else None
    if not isinstance(app, dict):
        out.dropped = 1
        return out
    if not app.get("success"):
        return out
    data = app.get("data")
    ts = unit_ts(app, request_ts) if isinstance(data, dict) else None
    if ts is None:
        out.dropped = 1
        return out
    meta = {"kind": "meta", "ts": ts, "id": appid}
    _required(meta, data, "name")
    genres = data.get("genres")
    if isinstance(genres, list):
        _put(meta, "genres", [g["description"] for g in genres
                              if isinstance(g, dict) and isinstance(g.get("description"), str)])
    _put(meta, "released", steam_release_date(data.get("release_date")))
    _put(meta, "short_description", data.get("short_description"))
    out.observations.append(Observation(("steam_appdetails", appid), ts, [meta]))
    return out
