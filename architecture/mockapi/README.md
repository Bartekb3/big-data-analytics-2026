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

This section is everything a producer author needs. Commands to start the mock and the reference client: [RUNNING.md](../RUNNING.md#running-each-area). Which event fields come from which API field: [ingestion § API sources](../ingestion/README.md#api-sources-and-cadence) and [kafka-topics](../contracts/kafka-topics.md).

### In one paragraph

The mock is one HTTP server that answers the same paths and query parameters as the real APIs, with the real response bodies the collector recorded. It has a clock, the **replay time** `A`, which runs like a normal clock but starts in the past (e.g. at the start of the recording). At any moment it answers with what the real API answered at `A` (more precisely: the latest recording made at or before `A`). Asking more often does not give newer data; it gives the same answer again.

### Endpoints

One base URL (e.g. `http://mockapi:8080`) replaces all API hosts (`games.roblox.com`, `apis.roblox.com`, `api.steampowered.com`, `store.steampowered.com`); paths and parameters stay as in the real API.

| endpoint | path | parameters | answer | for producers |
|---|---|---|---|---|
| Roblox games | `/v1/games` | `universeIds=1,2,3` (send up to 50, as for the real API) | `{"data": [item, ...]}` | stream source |
| Roblox votes | `/v1/games/votes` | `universeIds=1,2,3` (up to 50) | `{"data": [item, ...]}` | stream source |
| Steam players | `/ISteamUserStats/GetNumberOfCurrentPlayers/v1/` | `appid=440` | `{"response": {...}}` | stream source |
| Roblox sorts | `/explore-api/v1/get-sorts` | `country` (`us`, `br`, `pl`), `sortsPageToken` (from the previous page) | one page of sorts | batch-like (new data every 15 min) |
| Steam appdetails | `/api/appdetails` | `appids=440` (one ID) | `{"440": {...}}` | once per new Steam game (`meta`) |
| Steam charts, store `popularnew`, Roblox search | `/ISteamChartsService/GetMostPlayedGames/v1/`, `/search/results/` (`start`), `/search-api/omni-search` (`searchQuery`) | as the real API | as the real API | not for producers (batch, matching) |
| IDs to poll | `/_mock/ids` | - | `{"roblox": [ids], "steam": [ids]}`: games with a recent observation | refresh every minute |
| clock | `/_mock/clock` | - | `replay_time`, `speed`, `archive_start`, `archive_end`, `ended`, `errors`, `inject` | diagnostics |

Other parameters (`sessionId`, `cc`, `l`, ...) are accepted and ignored. A Roblox batch answer leaves out the IDs that have no recent observation; ask for any grouping of IDs, the mock regroups.

### Time: use `_mock.observed_at` as `ts`

- Every unit of data carries `"_mock": {"observed_at": "2026-11-02T16:20:56.247Z"}`: on each item of `data[]` (games, votes), on each sorts page, on each whole Steam answer. It is the moment the collector observed that value. **Use it as the event `ts`, unchanged**; never use your own clock ([conventions § Time](../contracts/conventions.md#time)).
- Every response has the header `X-Mock-Time` = the replay time `A` of that answer. Always `observed_at <= A`.
- Dates are the original recording dates, so `ts` lies in the past (see Open questions: Kafka `LogAppendTime`).
- **Drop repeats.** Keep the last `observed_at` per `(endpoint, id)` (per `country` for sorts) and send events only when it changes. A repeated answer means "nothing new yet", not a new observation.
- Everything else in the item is the real API item, unchanged; parse it exactly as the real API. Only `nextSortsPageToken` is the mock's own value; pass it back as received.

### Polling plan (meets the course rule "a request to every stream source at least every 15 s")

1. Every 60 s: `GET /_mock/ids`. Before the `tracked_games` set exists this is the list of IDs to poll. Right after the start of a recording it can be short or empty (the collector's first round is not recorded yet); keep asking every minute.
2. For each stream source make a **round** of requests: Roblox games and votes, the IDs in batches of 50; Steam players, one request per appid. Let `N` be the number of requests in a round.
3. Send the next request of the round every `min(10 s, C / N)` seconds, where `C` = 50 s for games and 240 s for votes and Steam players; after the last one start again from the first. The `C / N` part catches the recorded observations (the collector recorded games every ~60-75 s and votes and Steam players every 5 min per ID); the 10 s part keeps the 15 s rule when there are few IDs. Example with 1155 Roblox and 36 Steam IDs: games every 2.1 s (N = 24), votes every 10 s, Steam every 6.7 s.
   **When the ID list is refreshed, keep the order and the position in the round**: known IDs keep their place, new IDs join at the end, IDs no longer listed leave. Restarting the round at every refresh starves the IDs at the end of the list (measured: a votes round lasts 240 s, so with a restart every 60 s three quarters of the IDs were never asked).
4. Sorts: every 5 min per country, request without `sortsPageToken`, then follow `nextSortsPageToken` until it is `""`.
5. Steam appdetails: once per appid not seen before.
6. HTTP timeout at most 5 s. Keep the schedule even when a request fails or is slow: the next request goes out at its normal time. The mock has no rate limits, so no client-side rate limiter is needed.
7. Back-off on errors at most 10 s, never longer (see the next table). A longer back-off breaks the 15 s rule and only skips data.

### Answers and what to do

Every answer the mock makes itself carries `X-Mock-Reason`; archived answers do not.

| status | `X-Mock-Reason` | meaning | what the producer does |
|---|---|---|---|
| 200 | - | data | parse, drop repeats, send events |
| 404 with body `{"response": {"result": 42}}` | - | recorded Steam answer: the game has no player stats | send no `players` event (missing is not zero) |
| 404 | `unknown-id` | no recent observation of this ID | send nothing for it this time |
| 429, 5xx | - | a recorded error of the real API (the mock repeats it while the recording had it, at most 2 min) | count it, retry at the normal schedule (back-off at most 10 s) |
| 504 | `network-error` | a recorded network error | as above |
| 429, 500, 504 after a delay | `injected` | test error (only when injection is on) | as above; the delay exceeds the 5 s timeout |
| 503 | `no-data` | the recording has a gap here: no recent observation of this endpoint | send nothing; keep polling at the normal schedule |
| 400 | `unknown-token` | sorts token too old (two newer cycles appeared) | start again from the first page |
| 400 | `bad-request` | malformed parameters | fix the producer |
| 503 | `end` | the archive is over | stop the producer |

Incomplete items (missing fields, `null`) and non-JSON `200` bodies are served as recorded: validate as for the real API.

### Checking a producer

The mock comes with a reference client, `check` ([RUNNING.md](../RUNNING.md#running-each-area)), that polls with the plan above and reports per source: the largest gap between requests, statuses, new and repeated observations, the newest and the oldest observation relative to `A`, and (with `--data-dir`) whether every answer equals what the archive says it must be and how many of the observations recorded during the run it received. Exit code 0 means: every stream source was requested at least every 15 s, no observation was later than `A`, and no answer differed from the archive. A producer can be checked the same way: no event may have `ts` later than the `X-Mock-Time` of the answer it came from.

## Design / rules

### Guarantee

For every response at replay time `A` and every record of feed `f` in it: `A - limit_f <= observed_at <= A` (limits in [Freshness limits](#freshness-limits)). Records are released by the clock, never by the number of requests: polling faster returns the same observation again (ID endpoints) or the same cycle (lists), never a later one.

### Replay clock

- `A = REPLAY_A0 + (wall_now - REPLAY_START) * REPLAY_SPEED`. All three are configuration; `REPLAY_START` is a fixed wall-clock instant, not the process start, so a restart continues at the same `A`.
- Defaults: `REPLAY_A0` = start of the archive, `REPLAY_START` = the moment the mock starts, `REPLAY_SPEED` = 1. Speeds above 1 are for the presentation demo only.
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
- **Injection** (`MOCKAPI_INJECT=<p>`, default 0): each request fails with probability `p`: `429` with `Retry-After: 5`, `500`, or `504` after a 35 s delay (beyond any sane client timeout). For tests and the demo.
- Precedence: end of archive, injection, error window, `no-data`, then the per-endpoint answer.

### End of archive

When `A` passes the end of the last archived hour, every request gets `503 end`; clients stop. Switching to the real APIs later is a base-URL change in the clients.

### Archive reading

- Only closed hours (`.gz`) are read. Several files of one hour (`HH.jsonl.gz`, `HH.1.jsonl.gz`, ...) are merged by `requested_at`. Broken lines are skipped and counted.
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
- the reference client's polling spacing and statistics.

End-to-end check (manual): `serve` on a real archive plus `check --data-dir` against it; see [Checking a producer](#checking-a-producer). Planned integration check with producers and Kafka in `docker compose`: no event has `ts` later than `A`.

## Open questions

- Replay clock as "now" for every service (retention cutoff, ML evaluation, health page, daily jobs): with original dates a wall-clock "now" would expire all replayed data at once. Needs a line in [conventions](../contracts/conventions.md#time). Owner E with all owners.
- Kafka topics must use `message.timestamp.type=LogAppendTime` and producers must not set the record timestamp, otherwise old `ts` values make the 72 h retention delete events before the Parquet sink reads them. Owner A ([infra](../infra/README.md)).
- Ingestion's design has a ~1 request/s per-host rate limiter and uncapped exponential back-off; against the mock (one host, no limits) both would break the 15 s rule. Proposed: no limiter and back-off capped at 10 s when the base URL is the mock. Owner A.
- Ingestion's `votes` cadence (every 15 min) vs the mock polling plan (votes spaced at most 10 s, repeats dropped): the plan only changes how often the API is asked, not how often events are sent. Owner A.
- Re-runs and producer restarts send exact duplicate events; the append-only Parquet dataset needs deduplication by `(platform, id, kind, ts)`. Owner C ([batch](../batch/README.md)).
- The `sort` event has no field for the sort name; 307 of 896 US games in one sample cycle appear in more than one of 26 sorts with different ranks. Needs a field in [kafka-topics](../contracts/kafka-topics.md#robloxsnapshots). Owner A with C.
- Producers take IDs from `/_mock/ids` now and from [`tracked_games`](../contracts/mongodb-collections.md#tracked_games) once batch builds it. Owner A with C.
- Move the raw record format to `contracts/raw-archive.md` (the mock is its first reader). Owner E.
- Where the mock and the pipeline run and how the archive gets there from the collecting computer (proposed: copy closed `.gz` hours, e.g. `rsync` every hour). Owner E.
- Docker image and compose service for the mock (needs Docker; not built yet). Owner E with A ([infra](../infra/README.md)).
- Injection rate in the main run (proposed 0: only recorded errors, injection in tests and the demo). Owner E.
