# Collector

## Purpose

Gather real data from the Roblox and Steam APIs and archive every HTTP response **unchanged, with its original timestamps**, so that replay producers can later stream it into Kafka at a much higher rate than the APIs allow (the live sources are rate-limited; the project must be tested under big-data stream conditions).

A **supporting tool, not part of the Lambda pipeline**: it never writes to Kafka, MongoDB or Parquet. Not responsible for parsing responses into events (that is [ingestion](../ingestion/README.md) and the replay producers), nor for artificial timestamps (only replay producers re-stamp time).

## Owner

Package E, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| discovery jobs (`roblox-sorts`, `steam-lists`, `roblox-search`) | in-progress | |
| snapshot jobs (`roblox-games`, `roblox-votes`, `steam-players`) | in-progress | |
| raw archive + ID list | in-progress | |
| `stats` (archive summary) | in-progress | |

## Inputs

Roblox and Steam public APIs (external, no API keys):

| feed | endpoint | docs |
|---|---|---|
| `roblox_sorts` | `apis.roblox.com/explore-api/v1/get-sorts` | undocumented website endpoint |
| `roblox_games` | `games.roblox.com/v1/games` | [Swagger](https://games.roblox.com/docs/index.html), [legacy APIs](https://create.roblox.com/docs/cloud/legacy) |
| `roblox_votes` | `games.roblox.com/v1/games/votes` | as above |
| `roblox_search` | `apis.roblox.com/search-api/omni-search` | undocumented website endpoint |
| `steam_charts` | `api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1` | [ISteamChartsService](https://partner.steamgames.com/doc/webapi/ISteamChartsService) |
| `steam_players` | `api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1` | [ISteamUserStats](https://partner.steamgames.com/doc/webapi/ISteamUserStats#GetNumberOfCurrentPlayers) |
| `steam_store_new` | `store.steampowered.com/search/results/?filter=popularnew` | undocumented website endpoint (HTML inside JSON) |
| `steam_appdetails` | `store.steampowered.com/api/appdetails` | unofficial: [Storefront API](https://wiki.teamfortress.com/wiki/User:RJackson/StorefrontAPI) |

## Outputs

Files under the data directory (`COLLECTOR_DATA_DIR`, see `.env.example`; kept outside the repo):

- `raw/<feed>/date=YYYY-MM-DD/HH.jsonl` - one file per feed and UTC hour, one record per line, flushed after every line. The first write of the next hour closes it and compresses it to `HH.jsonl.gz` (lossless gzip, level 6); feeds that write rarely keep their last file plain until their next write. Files left plain by a stop or crash are compressed at the next start (`HH.<n>.jsonl.gz` if that hour already has a `.gz`). Each feed has its own lock, so compressing one feed never delays the others.
- `state/ids.json` - the ID list (below).

The raw record format will become a contract (`contracts/raw-archive.md`) once a replay producer reads it.

### Raw record

One JSON object per line, one line per HTTP **attempt** (429s, 5xx and network errors are archived too):

| field | type | meaning |
|---|---|---|
| `feed` | string | feed name (table above) |
| `requested_at` | ISO-8601 UTC string, ms | when the request was sent; the observation time of the data |
| `received_at` | ISO-8601 UTC string, ms | when the response was received |
| `url` | string | full URL including query parameters |
| `elapsed_ms` | int | `received_at - requested_at` |
| `status` | int | HTTP status; absent when no response arrived |
| `rate_limit` | object | `x-ratelimit-*` / `retry-after` headers, when sent |
| `error` | string | exception text when no response arrived |
| `body` | JSON | response body, when it is JSON |
| `body_text` | string | response body, when it is not JSON |

Times are never modified after the call. Steam `404` with `result: 42` means "no player stats for this app" and is kept as-is.

`body` is the parsed response stored as a JSON object: every key, key order and value is kept, but not the formatting (whitespace, number spelling such as `1.50` -> `1.5`, `\u` escapes). No field is removed, renamed or converted; interpreting the content is left to the producers.

### ID list (`state/ids.json`)

`{"roblox": {id: {first_seen, last_seen}}, "steam": {id: {first_seen, last_seen, candidate, title?, released?}}, "steam_details": {id: fetched_at}}`. Discovery jobs add IDs and refresh `last_seen`; IDs not seen for 7 days are dropped. Snapshot jobs poll the most recently seen IDs (caps: 3,000 Roblox, 500 Steam). Written atomically, so jobs in separate processes can share it.

## Design / rules

### Jobs

Discovery (which games) and snapshots (numbers for known games) are independent jobs that only share the ID list. Each job runs in its own thread with its own cadence; a cycle that found nothing to do (e.g. empty ID list) retries after 2 min.

| job | feeds | cadence | what one cycle does |
|---|---|---|---|
| `roblox-sorts` | `roblox_sorts` | 15 min | all pages (max 10) of the explore sorts for countries `us`, `br`, `pl`; adds every listed game to the ID list |
| `steam-lists` | `steam_charts`, `steam_store_new`, `steam_appdetails` | 6 h | top-100 chart + `popularnew` pages (max 5); chart games and clone candidates (released in the last 90 days) go to the ID list; `appdetails` once per new app |
| `roblox-search` | `roblox_search` | 24 h | one search per Steam clone candidate title |
| `roblox-games` | `roblox_games` | 60 s (or back-to-back if a cycle is longer) | all Roblox IDs in batches of 50 |
| `roblox-votes` | `roblox_votes` | 5 min | all Roblox IDs in batches of 50 |
| `steam-players` | `steam_players` | 5 min | one call per Steam ID |

Never run the same job in two processes at once (one writer per feed).

### Pacing and back-off

Minimum time between request starts, per pacer (feeds sharing a host share a pacer where needed):

| pacer | feeds | interval | why |
|---|---|---|---|
| `roblox_explore` | `roblox_sorts` | 1 s | header: 120 req/min |
| `roblox_search` | `roblox_search` | 1 s | header: 4 req/s |
| `roblox_games` | `roblox_games` | 3 s | header says 300 req/min, but a hidden limit returns 429 after ~550 IDs in a burst; 1 call / 3 s ran clean |
| `roblox_votes` | `roblox_votes` | 1 s | header: 200 req/min |
| `steam_api` | `steam_charts`, `steam_players` | 1 s | no header; 4 req/s worked; 100k/day documented for keyed use |
| `steam_store` | `steam_store_new`, `steam_appdetails` | 2 s | ~200 appdetails / 5 min known |

Network errors, 429 and 5xx are retried up to 6 times with exponential back-off and jitter (5 s doubling, max 120 s, so ~2.5-4.5 min before giving up); other statuses are final.

`roblox_games` and `roblox_votes` have separate hidden budgets (a votes sweep does not shorten the games burst of ~11 calls, and votes work while games is blocked), so they keep separate pacers. 7 min of continuous games every 3 s plus votes sweeps gave no 429. Short 429 streaks still happen sporadically for reasons on the Roblox side (one streak of 4 in the sample runs, cleared after ~50 s); retries cover them, and `stats` (`http_429`) plus `gave up` warnings show how often in the long run. Measurements: 2026-11-02, one IP, ~10 min of probing; re-check with `stats` on real runs.

### Logging and stopping

- INFO: start, one line per finished cycle with call counts per status, compression of leftover files at start.
- WARNING: every retried failure (`HTTP 429 on attempt 1, backing off 7s`) and every response that does not end as a clean 200 with JSON: final 4xx (`HTTP 404 (final) <url>`), 200 without JSON, `gave up after 7 attempts, last: ...`, `stopping, not retrying ...`.
- ERROR: an unexpected exception in a cycle (with traceback); the job continues with its next cycle.
- Ctrl+C (SIGINT), `kill` (SIGTERM) and closing the terminal window (SIGHUP) stop cleanly: no new request or retry is sent, calls in flight finish (at most the 30 s timeout), files are closed and stay plain until the next start. `kill -9` or a power loss lose at most a partial last line, which readers must skip.

### Expected volume

Smoke run 2026-11-02 (one cycle of every job): 1,161 Roblox IDs (3 countries), 178 Steam IDs, 88 clone candidates; no 429s. One `roblox-games` cycle = 24 calls in 69 s, 1.76 MB plain / 0.48 MB gzip (ratio 3.6; sorts 5.9, votes 3.9, Steam players 13).

Extrapolated: about 29 records/s (Roblox games ~17/s, sort rows ~8/s, votes ~4/s, Steam ~0.6/s), i.e. a message well under every 15 s; about 2.5 GB/day plain, ~0.6 GB/day gzip, ~95 % of it `roblox_games`. Replace with `stats` from a long run.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/collector/config.py` | endpoints, feeds, pacers, jobs, settings |
| `src/gametrend/collector/clock.py` | injected clock (only place reading the wall clock) |
| `src/gametrend/collector/records.py` | raw record building, retry rules |
| `src/gametrend/collector/archive.py` | hourly files, rotation and compression |
| `src/gametrend/collector/http.py` | pacer, back-off, `Fetcher` (archives and logs every attempt, honours the stop flag) |
| `src/gametrend/collector/extract.py` | IDs from sorts, chart and store pages; Steam dates; clone candidates |
| `src/gametrend/collector/idlist.py` | ID list merge / prune / select, atomic file store |
| `src/gametrend/collector/feeds.py` | one cycle per job |
| `src/gametrend/collector/stats.py` | archive summary per feed and day |
| `src/gametrend/collector/cli.py` | `run` and `stats` commands, job threads, shutdown |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests).

Unit tests (`tests/collector/`, no network) cover record building, retry rules, pacing and back-off with a fake clock, warnings for non-200 outcomes, no requests or retries after stop, hourly rotation, lossless compression, per-feed locking and crash recovery of the archive, ID extraction from the recorded fixtures in `api_test/responses/`, the ID list, every job cycle against a fake HTTP session, and the archive summary. `test_live_api.py` (marker `live`, run manually) calls each endpoint once and checks the fields the system relies on.

## Open questions

- Where the collector runs long-term (laptop with `caffeinate` vs a small cloud VM). Owner E.
- Whether to also record a few Roblox search hits' details (`/v1/games` for hits) for matching. Owner E with C.
- Raw size per day and the gzip ratio on real data (measure with `stats`). Owner E.
