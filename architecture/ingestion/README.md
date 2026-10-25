# Ingestion

## Purpose

Poll the Roblox and Steam APIs for the games in the tracked set and publish validated events to Kafka. Not responsible for deciding which games are tracked (that is [batch](../batch/README.md)) or for any metric computation.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| producers service (Roblox job group) | planned | |
| producers service (Steam job group) | planned | |
| shared HTTP client (rate limiter, back-off) | planned | |
| `ingest_stats` counters | planned | |

## Inputs

- Roblox and Steam public APIs (external), see [API sources and cadence](#api-sources-and-cadence).
- [`tracked_games`](../contracts/mongodb-collections.md#tracked_games) - which IDs to poll and at which `poll_minutes`.

## Outputs

- [`roblox.snapshots`](../contracts/kafka-topics.md#robloxsnapshots) - `game`, `votes`, `sort`, `meta` events.
- [`steam.players`](../contracts/kafka-topics.md#steamplayers) - `players`, `meta` events.
- [`ingest_stats`](../contracts/mongodb-collections.md#ingest_stats) - counters of sent, dropped and failed records.

## Design / rules

- One `producers` service with a Roblox and a Steam job group.
- Shared rate limiter, about 1 request/s per host.
- Exponential back-off with jitter on HTTP 429 and 5xx.
- At most 50 IDs per Roblox batch call.

### API sources and cadence

| event | API endpoint (API field -> event field) | cadence |
|---|---|---|
| Roblox `game` | `games.roblox.com/v1/games` (`playing`, `visits`, `favoritedCount` -> `favorites`, `updated`) | every `poll_minutes` of the game (from `tracked_games`) |
| Roblox `votes` | `games.roblox.com/v1/games/votes` (`upVotes` -> `up`, `downVotes` -> `down`) | every 15 min, same 50-ID loop as `game` |
| Roblox `sort` | `explore-api/v1/get-sorts` (`playerCount` -> `playing`), 3 countries | every 15 min |
| Roblox `meta` | `games.roblox.com/v1/games` (`name`, `genre_l1`, `genre_l2`, `created`, `description`) | only when the metadata changes |
| Steam `players` | `ISteamUserStats/GetNumberOfCurrentPlayers` (`player_count` -> `players`) | every `poll_minutes` of the game (from `tracked_games`) |
| Steam `meta` | `store.steampowered.com/api/appdetails` (`name`, `genres[].description` -> `genres`, `release_date.date` -> `released`, `short_description`) | when a game is first seen or its metadata changes |

Steam `release_date.date` comes as e.g. `"Feb 26, 2025"` and must be converted to `YYYY-MM-DD`.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/ingestion/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests).

Parsing and validation are tested against `api_test/responses/*.json`, which holds a recorded response of every endpoint.

## Open questions

- How "metadata changed" is detected for `meta` events (hash of fields kept in memory vs read from `games`). Owner A.
- How `sort` games outside the tracked set are handled (they are the input for the next tracked-set rebuild). Owner A with C.
- Retention of `ingest_stats`. Owner A.
- Cadence of Roblox `votes`: "every 15 min, same loop as `game`" conflicts with `game` polling every 5 min for games with `poll_minutes` 5 (see [tracked set rules](../batch/README.md#tracked-set-rules)). Is `votes` a separate 15-minute loop? Owner A.
