# Mock API

## Purpose

Serve the [collector](../collector/README.md) raw archive over HTTP, in the shape of the real Roblox and Steam APIs and without rate limits, so the [ingestion](../ingestion/README.md) producers can poll it like live APIs and the pipeline runs on a real data stream. Every value served is a real recorded value: no record is generated, interpolated or noised.

Responses follow one replay clock: a request at replay time `A` sees only data observed at or before `A`, so no source can run ahead of another however often it is polled. Not responsible for parsing, validating or cleaning data (that is ingestion) and does not write to Kafka, MongoDB or Parquet.

## Owner

Package E, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| replay clock + `/_mock/clock` | in-progress | |
| archive reader (follow mode) | in-progress | |
| ID endpoints (games, votes, Steam players, appdetails) | in-progress | |
| sorts endpoint | in-progress | |
| rare list endpoints (charts, `popularnew`, search) | in-progress | |
| `/_mock/ids` | in-progress | |
| error replay and injection | in-progress | |
| reference client and archive comparison (`check`) | in-progress | |

## Inputs

- The collector raw archive (`raw/<feed>/date=YYYY-MM-DD/*.jsonl.gz`, mounted read-only). Its record format is described in [collector § Raw record](../collector/README.md#raw-record) until it moves to `contracts/raw-archive.md` (see Open questions).

## Outputs

- HTTP responses to the ingestion producers (and later batch and matching), in the shape of the real endpoints listed in [collector § Inputs](../collector/README.md#inputs), plus the `_mock` field described in [Using the mock from a producer](#using-the-mock-from-a-producer).

## Using the mock from a producer

This section is what a producer author needs to know about the mock. **The producer is the same code for the mock and for the real APIs**; the few differences are settings, listed in [ingestion § Mock and real APIs](../ingestion/README.md#mock-and-real-apis). Commands to start the mock and the reference client: [RUNNING.md](../RUNNING.md#running-each-area). Which event fields come from which API field: [ingestion § API sources](../ingestion/README.md#api-sources-and-cadence) and [kafka-topics](../contracts/kafka-topics.md).

### In one paragraph

The mock is one HTTP server that answers the same paths and query parameters as the real APIs, with the real response bodies the collector recorded. It has a clock, the **replay time** `A`, which runs like a normal clock but starts in the past (e.g. at the start of the recording). At any moment it answers with what the real API answered at `A` (more precisely: the latest recording made at or before `A`). Asking more often than the data was recorded does not give newer data, only the same answer again; asking at the recording rate gives each recording once.

### Endpoints

One base URL (e.g. `http://mockapi:8080`) replaces all API hosts (`games.roblox.com`, `apis.roblox.com`, `api.steampowered.com`, `store.steampowered.com`); paths and parameters stay as in the real API.

| endpoint | path | parameters | answer | for producers |
|---|---|---|---|---|
| Roblox games | `/v1/games` | `universeIds=1,2,3` (send up to 50, as for the real API) | `{"data": [item, ...]}` | stream source |
| Roblox votes | `/v1/games/votes` | `universeIds=1,2,3` (up to 50) | `{"data": [item, ...]}` | stream source |
| Steam players | `/ISteamUserStats/GetNumberOfCurrentPlayers/v1/` | `appid=440` | `{"response": {...}}` | stream source |
| Roblox sorts | `/explore-api/v1/get-sorts` | `country` (`us`, `br`, `pl`), `sortsPageToken` (from the previous page) | one page of sorts | batch-like (new data every 15 min) |
| Steam appdetails | `/api/appdetails` | `appids=440` (one ID) | `{"440": {..., "_mock": {...}}}` | once per new Steam game (`meta`) |
| Steam charts, store `popularnew`, Roblox search | `/ISteamChartsService/GetMostPlayedGames/v1/`, `/search/results/` (`start`), `/search-api/omni-search` (`searchQuery`) | as the real API | as the real API | not for producers (batch, matching) |
| IDs to poll | `/_mock/ids` | - | `{"roblox": [ids], "steam": [ids]}`: games with a recent observation | refresh every minute |
| clock | `/_mock/clock` | - | `replay_time`, `speed`, `archive_start`, `archive_end`, `ended`, `errors`, `inject` | diagnostics |

As on the real APIs, `/v1/games` rejects more than 50 IDs and both Roblox endpoints reject an empty ID list (`400 bad-request`), and a blank `sortsPageToken` means the first page. Other parameters (`sessionId`, `cc`, `l`, ...) are accepted and ignored. A Roblox batch answer leaves out the IDs that have no recent observation; ask for any grouping of IDs, the mock regroups.

### Time: use `_mock.observed_at` as `ts`

- Every unit of data carries `"_mock": {"observed_at": "2026-11-02T16:20:56.247Z"}`: on each item of `data[]` (games, votes), on each sorts page, on each whole Steam players answer, inside the app's object of an appdetails answer. It is the moment the collector observed that value. **Use it as the event `ts`, unchanged**; never use your own clock ([conventions § Time](../contracts/conventions.md#time)).
- Every response has the header `X-Mock-Time` = the replay time `A` of that answer. Always `observed_at <= A`.
- Dates are the original recording dates, so `ts` lies in the past. **Do not set the Kafka record timestamp to `ts`**: leave it unset. Event time travels only in `ts`, and the old dates do not expire because of the [topic settings](../contracts/kafka-topics.md#topic-settings).
- **Safeguard against repeats.** Keep the last `observed_at` per `(endpoint, id)` and send events only when it changes (for sorts: per country, by the `observed_at` of the first page). At the recording rate repeats are rare; see below. On the real APIs every answer is new, so this check never fires there. A repeat is not an error and not a dropped record: do not count it in `ingest_stats`.
- Everything else in the item is the real API item, unchanged; parse it exactly as the real API. Only `nextSortsPageToken` is the mock's own value; pass it back as received.

### Polling plan (meets the course rule "a request to every stream source at least every 15 s")

Poll at the rate the data was recorded. Each recording is then delivered once, and no faster clock is needed.

1. Every 60 s: `GET /_mock/ids`. Until the `tracked_games` set exists this is the list of IDs to poll. Right after the start of a recording it can be short or empty (the collector's first round is not recorded yet); keep asking every minute.
2. For each stream source make a **round** of requests: Roblox games and votes, the IDs in batches of 50; Steam players, one request per appid. Let `N` be the number of requests in a round.
3. Send the next request of the round every `max(pace, min(cycle / N, 14 s))` seconds; after the last one start again from the first. `pace` and `cycle` are settings, with these starting values (the collector's [pacers](../collector/README.md#pacing-and-back-off) and [job cadences](../collector/README.md#jobs)):

   | source | `pace` | `cycle` | with 1155 Roblox and 178 Steam IDs |
   |---|---|---|---|
   | Roblox games | 3 s | 60 s | every 3 s (N = 24) |
   | Roblox votes | 1 s | 300 s | every 12.5 s (N = 24) |
   | Steam players | 1 s | 300 s | every 1.7 s (N = 178) |

4. **When the ID list is refreshed, keep the order and the position in the round**: known IDs keep their place, new IDs join at the end, IDs no longer listed leave. Restarting the round at every refresh starves the IDs at the end of the list.
5. Sorts: every 15 min per country, request without `sortsPageToken`, then follow `nextSortsPageToken` until it is `""`.
6. Steam appdetails: for each new appid, until it gets a `200` (`success: false` counts: the game has no details). After a `404` or `503` ask again at the next ID refresh: on the mock an ID can be listed before its details were recorded, and on the real API the request can fail.
7. Start each request on schedule, without waiting for the previous answer of that source. After a failed request the next one goes out as in [ingestion § Mock and real APIs](../ingestion/README.md#mock-and-real-apis).

**When repeats still appear.** The recording is not perfectly regular, so a constant rate cannot match it exactly:

| situation | what happens | what to do |
|---|---|---|
| the collector paused or restarted (a `429` streak, a new run) | a part of the IDs repeat once or skip one recording until the rates line up again | nothing: the safeguard above drops the repeats |
| a source stopped being recorded while the archive goes on | repeats until the [freshness limit](#freshness-limits), then `503 no-data` | nothing |
| too few IDs for the 15 s rule at the recording rate: votes below 22 batches (~1050 IDs), Steam below 22 IDs, games below 5 batches (~200 IDs) | the 14 s limit makes the client ask more often than recorded | accept the repeats, or run the mock faster: `REPLAY_SPEED` = (`cycle` / `N`) / 14 |

### Answers and what to do

Every answer the mock makes itself carries `X-Mock-Reason`; archived answers do not.

| status | `X-Mock-Reason` | meaning | what the producer does |
|---|---|---|---|
| 200 | - | data | parse, drop repeats, send events |
| 404 with body `{"response": {"result": 42}}` | - | recorded Steam answer: the game has no player stats | no `players` event (missing is not zero), as on the real API |
| 404 | `unknown-id` | no recent observation of this ID | no event, like any other 404 |
| 429, 5xx | - | a recorded error of the real API (the mock repeats it while the recording had it, at most 2 min) | the normal error path: count it, back off |
| 504 | `network-error` | a recorded network error | the normal error path |
| 429, 500, 504 after a delay | `injected` | test error (only when injection is on) | the normal error path; the delay ends in a client timeout |
| 503 | `no-data` | the recording has a gap here | the normal error path, no events |
| 400 | `unknown-token` | sorts token too old (two newer cycles appeared) | start again from the first page |
| 400 | `bad-request` | malformed parameters | fix the producer |
| 503 | `end` | the archive is over | the normal error path works too (requests keep failing); stopping the producer is optional |

No branch on `X-Mock-Reason` is needed, except that a producer may stop on `end`. Roblox `429` answers carry `x-ratelimit-*` headers and no `Retry-After`, on the mock as on the real API.

Incomplete items (missing fields, `null`) and `200` bodies that are not a JSON object (text, `null`) are served as recorded: validate as for the real API.

### Checking a producer

The mock comes with a reference client, `check` ([RUNNING.md](../RUNNING.md#running-each-area)), that polls with the plan above (without back-off) and reports per source: the largest gap between requests, statuses, new and repeated observations, the newest and the oldest observation relative to `A`, and (with `--data-dir`) whether every answer equals what the archive says it must be and how many of the observations recorded during the run it received. Exit code 0 means: every stream source was requested at least every 15 s, no observation was later than `A`, and no answer differed from the archive. A producer can be checked the same way: no event may have `ts` later than the `X-Mock-Time` of the answer it came from.

## Design / rules

### Guarantee

For every response at replay time `A` and every record of feed `f` in it: `A - limit_f <= observed_at <= A` (limits in [Freshness limits](#freshness-limits)). Records are released by the clock, never by the number of requests: polling faster returns the same observation again (ID endpoints) or the same cycle (lists), never a later one.

### Replay clock

- `A = REPLAY_A0 + (wall_now - REPLAY_START) * REPLAY_SPEED`. All three are configuration; `REPLAY_START` is a fixed wall-clock instant, not the process start, so a restart continues at the same `A`.
- Defaults: `REPLAY_A0` = start of the archive, `REPLAY_START` = the moment the mock starts, `REPLAY_SPEED` = 1. A speed above 1 moves all sources together, so they stay aligned; the archive then lasts proportionally shorter.
- Original archive dates are kept: `A` and every served time are real observation times (no offset).
- To resume after a break, restart with `REPLAY_A0` = the last `A` (logged by the mock when it stops) and a new `REPLAY_START`.
- `X-Mock-Time` is `A` with millisecond precision; archived times have millisecond precision too, so `observed_at <= X-Mock-Time` holds exactly.

### How an answer is chosen

| type | endpoints | response at `A` |
|---|---|---|
| per ID | `roblox_games`, `roblox_votes`, `steam_players`, `steam_appdetails` | for each requested ID, the latest archived observation with `observed_at <= A` |
| sorts | `roblox_sorts` | pages of the latest complete cycle of the requested country |
| rare lists | `steam_charts`, `steam_store_new`, `roblox_search` | the latest full archived response with `observed_at <= A` |

**Per ID.** Successful archived responses are split into single games (`data[]` items by `id`; Steam players by the `appid` query parameter; appdetails by its `appids` parameter). A request for any set of IDs gets the latest item of each, rebuilt into the real response shape (`{"data": [...]}`). IDs without an observation within the freshness limit are left out of batch responses; a single-ID request for such an ID gets `404 unknown-id`. A recorded Steam `404` with `result: 42` is the latest observation of that appid and is returned unchanged.

**Sorts.** In the archive a page requested without `sortsPageToken` starts a new cycle of its country; the following pages belong to it. A cycle is complete at its last page (empty `nextSortsPageToken` or the 10th page) or, if that is missing, when the next cycle of the country starts. It is served once `A` passes that moment. Page tokens are issued by the mock (`<cycle start in ms>-p<n>`) and work for the latest two cycles, so a client reads all pages of one cycle even if a newer cycle becomes visible meanwhile.

**Rare lists.** `popularnew` pages are selected by `start` from the latest archived run (`404 unknown-id` for a page that was not recorded). Search answers only queries identical to an archived one (the collector searched the titles of the Steam clone candidates); any other query gets `{"searchResults": []}`.

### Freshness limits

| feed | limit |
|---|---|
| `roblox_games` | 10 min |
| `roblox_votes`, `steam_players` | 15 min |
| `roblox_sorts` | 45 min |
| `steam_charts`, `steam_store_new`, `roblox_search` | 2 days |
| `steam_appdetails` | none (fetched once per game) |

If a whole feed has no observation within its limit (a collection gap), every request to it gets `503 no-data`; old data is never served as current.

### Errors

Clients must handle errors, so errors are served by default (`MOCKAPI_ERRORS=replay`; `off` serves the latest usable data instead).

- **Error window** per feed: from a failed archived attempt to the next usable one of the same feed, but at most 120 s after the last failure (the collector's back-off cap, so a failure with no later attempt does not block the feed for hours), every request to that feed gets the latest archived failure: its status, body and rate-limit headers. An archived network error is served as `504 network-error`. This matches the recording: during a back-off the collector's pacer blocked the whole feed.
- **Injection** (`MOCKAPI_INJECT=<p>`, default 0): each request fails with probability `p`: `429` with `Retry-After: 5`, `500`, or `504` after a 35 s delay (a client normally times out first). For tests and the demo.
- Precedence: end of archive, injection, error window, `no-data`, then the per-endpoint answer.

### End of archive

When `A` passes the end of the last archived hour, every request gets `503 end`; clients stop. If a new hour appears later (follow mode), the mock answers again within a minute of replay time. Switching to the real APIs later is a base-URL change in the clients.

### Archive reading

- Only closed hours (`.gz`) are read. Several files of one hour (`HH.jsonl.gz`, `HH.1.jsonl.gz`, ...) are merged by `requested_at`. Broken lines are skipped and counted; a file that cannot be read to its end gives what it has and is logged and counted.
- Follow mode: new files are picked up every minute (of replay time), so the archive can keep growing while the mock runs. A file of an hour that was already replayed is ignored with a warning.
- Memory holds only the latest observation per ID, the last two sorts cycles per country and the latest rare responses. At start the mock reads back each feed's freshness limit (all of appdetails).
- A response depends only on `A` and the archive, so restarts are deterministic; no state files.

### `/_mock/ids`

IDs with an observation within the freshness limit at `A`: Roblox from games and votes, Steam from players. Mock-only; it lets the producers run before the `tracked_games` set exists (see Open questions).

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/mockapi/config.py` | endpoint paths (from the collector URLs), freshness limits, error-window tail, settings |
| `src/gametrend/mockapi/clock.py` | replay clock, UTC parsing |
| `src/gametrend/mockapi/archive.py` | archive reader: hour files, `HH.n` merge, follow mode, archive bounds |
| `src/gametrend/mockapi/engine.py` | answers at replay time: per-feed state, error windows, injection, all endpoints |
| `src/gametrend/mockapi/server.py` | HTTP server (standard library), JSON encoding, injected delays |
| `src/gametrend/mockapi/check.py` | reference client: polling plan, gaps, leads, statuses, report |
| `src/gametrend/mockapi/verify.py` | oracle for `check --data-dir`: the expected answer computed directly from the archive |
| `src/gametrend/mockapi/cli.py` | `serve` and `check` commands |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). Unit tests (`tests/mockapi/`, fake clock, small archives written by the tests, no network):

- replay clock, file names, `HH.n` merge, broken lines, warm-up, follow mode, late files, archive bounds;
- per-ID answers as of `A`, items unchanged apart from `_mock`, random polling at any rate never returns `observed_at > A`, freshness limits and `no-data`, Steam `404` (result 42) and `unknown-id`, appdetails without a limit;
- error windows (status, body, headers, network error, 120 s tail), errors off, non-JSON bodies, injection;
- sorts cycles, tokens across cycles, unknown tokens, stale cycles; rare lists; `/_mock/ids`; end of archive; unknown paths; `X-Mock-Time`; restart gives identical answers;
- the engine and the independent oracle agree on every answer of a random request sequence over a 40-minute archive with error windows, Steam 404s, sorts and the end; the oracle flags older, future, missing and wrongly failed answers;
- `end` left when a new hour arrives, overlapping requests, recorded `200` bodies that are not a JSON object, unreadable files, lone surrogates, requests the real APIs reject (50 IDs, empty list, blank token);
- the polling plan: spacing at the recording rate, below 15 s with few IDs; polling a regularly recorded archive at the recording rate gives no repeats; the reference client's statistics.

End-to-end check (manual): `serve` on a real archive plus `check --data-dir` against it; see [Checking a producer](#checking-a-producer). Planned integration check with producers and Kafka in `docker compose`: no event has `ts` later than `A`.

## Open questions

- Resolved: "now" for every service (retention cutoff, ML evaluation, health page, daily jobs) is data time, not the replay clock or the wall clock; see [conventions § Time](../contracts/conventions.md#time).
- Resolved: re-runs and producer restarts send exact duplicate events; the Parquet master dataset keeps them and its readers handle them, see [parquet-master § Duplicates](../contracts/parquet-master.md#duplicates).
- Producers take IDs from `/_mock/ids` now and from [`tracked_games`](../contracts/mongodb-collections.md#tracked_games) once batch builds it. Owner A with C.
- Move the raw record format to `contracts/raw-archive.md` (the mock is its first reader). Owner E.
- Where the mock and the pipeline run and how the archive gets there from the collecting computer (proposed: copy closed `.gz` hours, e.g. `rsync` every hour). Owner E.
- Docker image and compose service for the mock (needs Docker; not built yet). Owner E with A ([infra](../infra/README.md)).
- Injection rate in the main run (proposed 0: only recorded errors, injection in tests and the demo). Owner E.
