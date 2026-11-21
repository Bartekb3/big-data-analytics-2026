# Speed layer

## Purpose

Consume Kafka events with Spark Structured Streaming and keep 5-minute game metrics and game metadata in MongoDB up to date, including growth and `new_visits`; trend score and the +3 h projection are planned (see Status). Not responsible for training models ([ml](../ml/README.md)) or for long-term history ([batch](../batch/README.md)).

## Owner

Package B, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| streaming job (Kafka -> `game_metrics`, `games`) | in-progress | verified on the mock |
| conditional upsert writer | in-progress | verified on the mock |
| live scoring: growth, `new_visits` | in-progress | verified on the mock |
| live scoring: trend score, projection | planned | blocked: no [ml](../ml/README.md) models yet; `trend_score` and `projection_3h` stay absent and `model_coeffs` is not read |

## Inputs

- [`roblox.snapshots`, `steam.players`](../contracts/kafka-topics.md) - all event kinds.
- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - history needed for growth and `new_visits` (and later projection features).
- [`model_coeffs`](../contracts/mongodb-collections.md#model_coeffs) - planned with trend score and projection: latest document per model and platform, read at the start of each micro-batch.

## Outputs

- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - 5-minute documents.
- [`games`](../contracts/mongodb-collections.md#games) - metadata from `meta` events.

## Design / rules

### Event roles

- `game` events supply the player history (`players` <- `playing`, `visits`, `favorites`, `updated`).
- `votes` events only enrich the same time bucket (`up`, `down`).
- `sort` events are archived for the daily tracked-set rebuild and never write player counts; speed counts and drops them.
- `meta` events upsert `games`.
- Steam `players` events write `players`.

### Reading Kafka

- One streaming query subscribes to both topics, with one checkpoint (`SPEED_CHECKPOINT_DIR`, see [infra](../infra/README.md#volumes)). Spark only reads `topic` and `value` (as a string); everything else happens in plain Python inside `foreachBatch`, on the micro-batch collected to the driver.
- Records are validated with the same contract check the producers use ([common](../common/README.md)); the platform comes from the topic. An invalid record is counted and dropped and never stops the stream.
- `startingOffsets=earliest`: a start without a checkpoint processes everything still in Kafka, so `growth_24h` has references at once.
- `maxOffsetsPerTrigger` (`SPEED_MAX_OFFSETS_PER_TRIGGER`, default 50 000, counted over all partitions of both topics) bounds the micro-batch collected on the driver. At the [event volume](../infra/README.md#disk-sizing) that is about 24 minutes of data; it takes about 100 MB in Python (plus about 25 MB in the JVM and about 25 MB of history), well within the speed container's memory.

### Buckets and deduplication

- One `game_metrics` document per `(platform, id)` and 5-minute bucket (see [`ts`](../contracts/mongodb-collections.md#game_metrics)).
- Within a bucket, the latest event of each kind wins by its original observation timestamp. Older observations are rejected even if they arrive later.
- `observed_at` guards player data, `votes_observed_at` guards vote data, `games.observed_at` guards metadata. The same rule applies to Steam.
- Before writing, a micro-batch is reduced to at most one event per kind, game and bucket (`games`: per game), so `bulk_write(ordered=False)`, whose order is not defined, never holds two competing writes. Player events follow the common `latest_per_bucket` rule. Ties that rule leaves open never depend on arrival order: player events with equal `observed_at`, `players` and `visits` go to higher `favorites`, then later `updated`; `votes` with equal `observed_at` go to higher `up`, then higher `down`; `meta` with equal `observed_at` go to the larger serialized content.
- Each kind owns a group of fields and writes the whole group: the fields the observation has are `$set`, the group's other fields are `$unset`. A newer observation therefore never keeps a derived value (`new_visits`, `growth_*`) or optional metadata of an older one. Speed only unsets fields it writes itself, so this does not conflict with [conventions § Keys and writes](../contracts/conventions.md#keys-and-writes).

| kind | collection | guard | field group |
|---|---|---|---|
| Roblox `game` | `game_metrics` | `observed_at` | `players`, `visits`, `favorites`, `updated`, `new_visits`, `growth_1h`, `growth_3h`, `growth_24h` |
| Roblox `votes` | `game_metrics` | `votes_observed_at` | `up`, `down` |
| Steam `players` | `game_metrics` | `observed_at` | `players`, `growth_1h`, `growth_3h`, `growth_24h` |
| Roblox `meta` | `games` | `observed_at` | `name`, `genre_l1`, `genre_l2`, `description`, `created` |
| Steam `meta` | `games` | `observed_at` | `name`, `genres`, `description` (from `short_description`), `released` |

- A `votes` event that arrives before the `game` event of its bucket creates the document without `players`; the later `game` event fills it.
- `id`, `visits`, `favorites`, `up` and `down` are written as BSON `Long` (`Int64`) even when the value fits 32 bits, as the [contract](../contracts/mongodb-collections.md) requires; timestamps as BSON `Date`.

### Conditional upsert pattern

Writes go through `foreachBatch` with `pymongo` `bulk_write(..., ordered=False)`; the Spark connector's plain upsert cannot express the timestamp guard.

```python
UpdateOne(
    {"platform": p, "id": i, "ts": bucket,
     "observed_at": {"$not": {"$gte": obs_ts}}},   # matches missing, null or older
    {"$set": {"observed_at": obs_ts, <present fields of the group>},
     "$unset": {<absent fields of the group>: ""}},
    upsert=True)
```

If a newer observation already exists, the filter does not match, the upsert tries to insert a duplicate key and fails with `E11000`. This is the expected "older observation rejected" outcome: count it, do not retry it. Any other write error fails the micro-batch (see [Recovery](#recovery)).

### Scoring

- Growth and `new_visits` follow the [metric definitions](../contracts/mongodb-collections.md#metric-definitions), using the shared code from [common](../common/README.md#how-speed-and-batch-call-the-metric-functions).
- One micro-batch can hold many buckets of one game (dozens in an accelerated replay). Speed reduces them with `latest_per_bucket` and computes them as one chain: the first bucket against MongoDB, every next one against the previous bucket of the micro-batch. Never each bucket separately against MongoDB.
- History is read from `game_metrics` once per micro-batch and platform, by `ts` (the index), with projection `{platform, id, ts, observed_at, players, visits}`. Ranges per game:
  - predecessor for `new_visits` (Roblox only): `ts` in `[first ts - 1 h - 5 min, first ts]`; the predecessor itself is chosen in Python as the latest document with `visits` and `ts` strictly before the first bucket;
  - growth references: `ts` in `[first observed_at - h - 20 min, last observed_at - h + 15 min]` for h = 1 h, 3 h, 24 h. The extra 5 minutes cover `observed_at` lying up to 5 minutes after `ts`.
- The ranges of all games are merged (closed ranges; overlapping and touching ones become one) into one query `{platform, id: {$in: games}, $or: [merged ts ranges]}`. In normal operation that is a few ranges; one late event of one game adds only its own narrow ranges instead of stretching one min..max window over every game. Every range is read for every game of the query, which reads somewhat more than needed; the exact choice is made by the pure functions.
- MongoDB history is merged with the micro-batch buckets through `latest_per_bucket` before `growths`, so a newer version of a bucket wins.
- Trend score (Roblox, planned) uses the latest `expected_players` coefficients and the [cold start rule](../ml/README.md#expected-players-model-roblox-only) of that model.
- `projection_3h` (planned) uses the latest `projection_3h` coefficients and is applied as described in [ml](../ml/README.md#3-h-projection-one-model-per-platform).

### Micro-batch log

One log line per micro-batch, to see whether speed keeps up (in an accelerated replay a micro-batch of 50 000 events must finish within a few tens of seconds): `batch_id`; events per kind (`game`, `players`, `votes`, `meta`, `sort`, `invalid`); number of games; history documents read; milliseconds for the MongoDB read, the computation, the writes and in total; per collection `matched`, `modified`, `upserted` and `rejected (older or already written)`. Durations are measured in the I/O layer only; no processing logic reads a clock.

### Recovery

- A speed-only outage resumes from the speed checkpoint ([infra](../infra/README.md#design--rules)) as long as it is shorter than the [Kafka retention](../contracts/kafka-topics.md#topic-settings).
- A longer outage: `failOnDataLoss=false`, so Spark logs a data-loss warning and continues from the oldest offset still in Kafka instead of stopping for good. The job logs this setting at startup.
- Fail fast: any error other than `E11000` (MongoDB unreachable, timeout, another write error) raises from `foreachBatch`, the query stops and the process exits non-zero. There are no retries inside the job. The container restarts by itself ([infra](../infra/README.md#design--rules)) and Spark repeats the micro-batch from the checkpoint.
- Timestamp-conditional upserts make replays safe; a unique index alone would let an old replay overwrite newer values. In a repeated micro-batch, the documents already written have the same guard timestamp, so they are rejected and the derived fields are computed identically.
- A high `rejected (older or already written)` count in the first micro-batches after a restart or a replay is normal: it counts the speed's own documents written before the failure.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/speed/__main__.py` | entry point, `python -m gametrend.speed` |
| `src/gametrend/speed/config.py` | `Settings`, `settings_from_env` (`KAFKA_BOOTSTRAP`, `MONGO_URI`, `SPEED_CHECKPOINT_DIR`, `SPEED_MAX_OFFSETS_PER_TRIGGER`) |
| `src/gametrend/speed/job.py` | Spark and MongoDB I/O: `start_query` (Kafka source options, `foreachBatch`, checkpoint), `make_handler` (collect + process + log), `MongoStore` (history read, `bulk_write`), `main` |
| `src/gametrend/speed/parse.py` | Kafka records -> `PlayerEvent`, `VotesEvent`, `MetaEvent` with UTC `datetime`s and `games` field names; counts per kind and `invalid` |
| `src/gametrend/speed/windows.py` | `predecessor_range`, `growth_range`, `game_ranges`, `merge_ranges`, `history_filter`, `HISTORY_PROJECTION` |
| `src/gametrend/speed/scoring.py` | `reduce_players`, `reduce_votes`, `reduce_metas`, `score_game` (`new_visits` chain and growth), `history_by_game`, `score_players` |
| `src/gametrend/speed/writes.py` | field groups, `guarded_upsert` (`$set`/`$unset`, `Int64`), `player_op`, `votes_op`, `meta_op`, `summarize` (E11000 counted, other errors raise `WriteFailed`) |
| `src/gametrend/speed/microbatch.py` | `process_batch` (one micro-batch against an injected store), `history_filters`, `BatchReport`, `format_report` |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). `tests/speed/`, no network, no Docker; MongoDB is replaced by a fake store.

- parsing: every kind of both platforms, `meta` with missing optional fields, `sort` dropped, broken JSON, missing or mistyped fields, impossible dates, unknown topic; every event ingestion builds from the recorded payloads in `api_test/responses/` parses without an invalid one;
- ranges: predecessor and growth bounds, references at both tolerance edges, merging in normal operation, one late event, disjoint, nested and touching ranges;
- scoring: several observations per bucket, ties independent of order, chains with and without a predecessor, predecessor strictly before the first bucket, reset, the 1 h gap boundary, Steam without visits, growth from history and from the micro-batch, a newer bucket version winning over MongoDB, zero and missing references, current zero (-100), each game against its own history;
- writes: `$set` / `$unset` per group, vote and player groups kept apart, Steam fields only, `Int64` types, the votes guard and bucket, `games` with missing optional fields, E11000 counted, other write errors and write concern errors raising;
- micro-batch: one history query per platform, a whole micro-batch end to end, an empty micro-batch, the log line;
- `spark` marker: the `foreachBatch` handler on a local Spark DataFrame.

## Open questions

- Trend score needs `genre_l1` and game age (from `created`) from [`games`](../contracts/mongodb-collections.md#games), but `games` is neither in Inputs nor listed as read by speed in the contract. Decide how speed gets them and update Inputs and the contract. Belongs to the planned trend score module. Owner B with D.
- The [dashboard](../dashboard/README.md) ranks Roblox games by `trend_score`, which stays absent until the ml models exist. Fallback ranking until then, e.g. by `growth_1h`? Decision: owner D.
