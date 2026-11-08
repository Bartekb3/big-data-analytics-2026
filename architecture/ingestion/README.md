# Ingestion

## Purpose

Poll the Roblox and Steam APIs (or the [mock API](../mockapi/README.md) that replays them) for the games in the tracked set and publish validated events to Kafka. Not responsible for deciding which games are tracked (that is [batch](../batch/README.md)) or for any metric computation.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| producers service (Roblox job group) | planned | |
| producers service (Steam job group) | planned | |
| shared HTTP client (rate limiter, back-off) | planned | |
| mock API mode (see [Mock and real APIs](#mock-and-real-apis)) | planned | |
| `ingest_stats` counters | planned | |

## Inputs

- Roblox and Steam public APIs (external), see [API sources and cadence](#api-sources-and-cadence), or the [mock API](../mockapi/README.md#using-the-mock-from-a-producer), which serves them from the recorded archive. The mock is the default during development.
- [`tracked_games`](../contracts/mongodb-collections.md#tracked_games) - which IDs to poll and at which `poll_minutes`. Until batch builds it: the mock's [`/_mock/ids`](../mockapi/README.md#_mockids).

## Outputs

- [`roblox.snapshots`](../contracts/kafka-topics.md#robloxsnapshots) - `game`, `votes`, `sort`, `meta` events.
- [`steam.players`](../contracts/kafka-topics.md#steamplayers) - `players`, `meta` events.
- [`ingest_stats`](../contracts/mongodb-collections.md#ingest_stats) - counters of sent, dropped and failed records.

## Design / rules

- One `producers` service with a Roblox and a Steam job group.
- Shared rate limiter per API group, with the measured intervals from [collector § Pacing](../collector/README.md#pacing-and-back-off) (not per URL host: the mock is one host). It stays on with the mock.
- Exponential back-off with jitter on HTTP 429 and 5xx, capped as in [Mock and real APIs](#mock-and-real-apis).
- Every stream source (Roblox games, Roblox votes, Steam players) gets a request at least every 15 s: see the [polling plan](../mockapi/README.md#polling-plan-meets-the-course-rule-a-request-to-every-stream-source-at-least-every-15-s), which applies to both modes.
- Start each request on schedule, without waiting for the previous answer of that source; HTTP timeout 10 s.
- A repeated observation sends no event and is not counted in `ingest_stats`.
- At most 50 IDs per Roblox batch call.

### Mock and real APIs

One code path. Only these differ:

| what | mock | real APIs |
|---|---|---|
| base URLs (one setting per API group) | all the mock's URL | the real hosts |
| event `ts` | `_mock.observed_at` of the unit | request time, from an injected clock |
| IDs | `/_mock/ids` until `tracked_games` exists | `tracked_games` |
| back-off cap | 10 s (the mock's errors end with its clock, so waiting longer only breaks the 15 s rule) | 120 s |

Everything else is the same: parsing, validation, rate limiter, polling plan, repeat check (it never fires on the real APIs, because `ts` is always new). The Kafka record timestamp is left unset in both modes.

### API sources and cadence

| event | API endpoint (API field -> event field) | cadence |
|---|---|---|
| Roblox `game` | `games.roblox.com/v1/games` (`playing`, `visits`, `favoritedCount` -> `favorites`, `updated`) | [polling plan](../mockapi/README.md#polling-plan-meets-the-course-rule-a-request-to-every-stream-source-at-least-every-15-s); `poll_minutes` of the game (from `tracked_games`) is the slowest allowed |
| Roblox `votes` | `games.roblox.com/v1/games/votes` (`upVotes` -> `up`, `downVotes` -> `down`) | polling plan, its own 50-ID loop |
| Roblox `sort` | `explore-api/v1/get-sorts` (`playerCount` -> `playing`), 3 countries | every 15 min |
| Roblox `meta` | `games.roblox.com/v1/games` (`name`, `genre_l1`, `genre_l2`, `created`, `description`) | only when the metadata changes |
| Steam `players` | `ISteamUserStats/GetNumberOfCurrentPlayers` (`player_count` -> `players`) | polling plan; `poll_minutes` of the game is the slowest allowed |
| Steam `meta` | `store.steampowered.com/api/appdetails` (`name`, `genres[].description` -> `genres`, `release_date.date` -> `released`, `short_description`) | when a game is first seen or its metadata changes |

Steam `release_date.date` comes as e.g. `"Feb 26, 2025"` and must be converted to `YYYY-MM-DD`.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/ingestion/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests).

Parsing and validation are tested against `api_test/responses/*.json`, which holds a recorded response of every endpoint, and against the same responses from the mock (with `_mock`).

The mock does not react to load, so it cannot show that the limiter or the back-off work. Required before calling this area done:

- limiter and back-off: unit tests with a fake clock;
- metadata change detection: unit tests (the mock has one recorded `appdetails` per game);
- a short run against the real APIs (tests marked `live`), to confirm the same code stays within the real limits;
- a run against the mock with its reference client's criteria: [Checking a producer](../mockapi/README.md#checking-a-producer).

## Open questions

- How "metadata changed" is detected for `meta` events (hash of fields kept in memory vs read from `games`). Owner A.
- How `sort` games outside the tracked set are handled (they are the input for the next tracked-set rebuild). Owner A with C.
- Retention of `ingest_stats`. Owner A.
