# Batch layer

## Purpose

Archive all events in the Parquet master dataset, compute authoritative hourly aggregates, apply retention to 5-minute metrics and rebuild the tracked set daily. Not responsible for clone matching ([matching](../matching/README.md)) or model training ([ml](../ml/README.md)), although it schedules them.

## Owner

Package C, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| Kafka -> Parquet sink | in-progress | |
| hourly aggregates | in-progress | |
| retention sweep | in-progress | |
| batch cycle (loop service) | in-progress | sink, aggregates, retention; training not scheduled yet |
| tracked set rebuild (daily) | planned | |
| daily jobs (matching, saturation) | planned | |

## Inputs

- [`roblox.snapshots`, `steam.players`](../contracts/kafka-topics.md) - archived by the sink.
- [Parquet master dataset](../contracts/parquet-master.md) - source of hourly aggregates and of `sort` events for the tracked set.
- [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly) - latest `hour_ts` (where to continue) and the games with an hourly document (retention).
- [`bv_matches`](../contracts/mongodb-collections.md#bv_matches) - Roblox games kept in the tracked set.
- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - latest players for tracked-set ranking; documents to delete.
- Steam APIs (external): `GetMostPlayedGames` (top-100 chart), store search `popularnew`, `appdetails` (release date confirmation).

## Outputs

- [Parquet master dataset](../contracts/parquet-master.md).
- [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly).
- Deletions in [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) (retention).
- [`tracked_games`](../contracts/mongodb-collections.md#tracked_games).

## Design / rules

### Batch cycle

One long-running process (`python -m gametrend.batch`, service `batch`) with one SparkSession repeats:

1. **Sink** (Kafka -> Parquet).
2. **Hourly aggregates**, then the **retention sweep**: one step; the sweep runs only after the aggregates succeeded.
3. A pause of `BATCH_INTERVAL_S` seconds of wall time (default 3600). This is scheduling, not processing logic (AGENTS.md rule 8): for an accelerated replay it is set by hand to about 3600 / acceleration ([RUNNING.md](../RUNNING.md#running-each-area)). Running more often gains nothing: the aggregates only take closed hours and nobody reads Parquet live.

A step never overlaps with itself. Training and evaluation of the models ([ml](../ml/README.md)) are to be added after the retention sweep.

**Failure policy:**

- The sink is isolated from the aggregates: when the aggregates fail, the next cycle still archives. Kafka keeps events only 72 h; aggregates can always be recomputed from Parquet.
- One failing hour fails the whole aggregates step and nothing after it is written, so the latest `hour_ts` does not move past it and no hole is left behind the recompute window. Aggregates stand still and it shows in the log.
- Every failing cycle logs an ERROR with the step, how many cycles in a row it failed and since when; recovery is logged once.
- After a step failure the process checks the SparkSession; when it is unusable the process exits non-zero and docker compose restarts it ([infra](../infra/README.md#design--rules)). A restart is safe: the sink resumes from its checkpoint, the aggregates continue from MongoDB and are whole-document upserts.

### Kafka -> Parquet sink

- Structured Streaming with the `availableNow` trigger: each cycle archives what is in Kafka at its start, then returns. Own checkpoint `ARCHIVE_CHECKPOINT_DIR`, never shared with speed ([infra](../infra/README.md#volumes)); `startingOffsets=earliest`, `failOnDataLoss=false` (a gap longer than the Kafka retention is skipped, see [Recovery](#recovery-and-authority)).
- Records are validated with the shared contract check ([common](../common/README.md)) inside `mapInPandas`: `json.loads`, check, conversion to a row of the [master layout](../contracts/parquet-master.md#layout). No second validation in Spark (`from_json` + filters). Integer columns are nullable `Int64` in pandas, so a missing value never becomes 0 or NaN. Invalid records are dropped and counted (Spark accumulator).
- The Parquet columns are derived from the contract check's schema (`field_types()`), not listed a second time.
- `maxOffsetsPerTrigger` (`BATCH_MAX_OFFSETS_PER_TRIGGER`, default 500 000) cuts a large backlog (first run, up to 72 h of Kafka) into several micro-batches.
- `coalesce(1)` before writing: each micro-batch writes one file per `(platform, date)` it touches, so with hourly cycles about 48 files per date. No compaction (the dataset is never rewritten).
- One log line per cycle: micro-batches, records read, invalid, written.

### Hourly aggregates

Computed from Parquet with the shared functions from [common](../common/README.md#how-speed-and-batch-call-the-metric-functions), never from speed output.

- **"Now"** is the latest event `ts` in Parquet over both platforms ([conventions § Time](../contracts/conventions.md#time)). It is read per platform from the newest `date` partition (partition pruning, not a scan of the dataset); a date directory holding only uncommitted files gives nothing and the previous date is tried.
- **Range** of a cycle: closed hours (ending at or before the hour of "now") from `max(hour_ts) in game_metrics_hourly - BATCH_RECOMPUTE_HOURS` (default 2) on; when the collection is empty, from the hour of the earliest event in Parquet. The range comes from MongoDB itself, so no separate state file is needed and a pause leaves no gap. Late events of the last K hours (Steam polls every 15 min; restarts) are taken into account by the recompute; later ones are ignored (known limitation; a full recompute is a drop of the collection).
- Hours are computed in jobs of 24 consecutive hours (bounds what is collected on the driver on a first run). A job reads events with observation time in `[first hour - 1 h, last hour + 1 h)`; the extra hour only supplies the predecessor for `new_visits` (`last_with_visits`); its own values are not written.
- Before grouping, exact copies are dropped (`dropDuplicates` over all event columns, Kafka metadata left out, see [Parquet duplicates](../contracts/parquet-master.md#duplicates)). `dropDuplicates` on the key alone would keep an arbitrary row; the same key with different values is left to `latest_per_bucket`, which decides by the values.
- Per game, `applyInPandas`. Timestamps cross the pandas boundary as epoch microseconds (no session or local time zone involved). Spark hands nullable `bigint` to pandas as `float64`; values are converted back to `int` and a non-integral value or one above 2^53 raises instead of being written.
- Per game and hour: observations (`game` -> `players` from `playing`, Steam `players`) go through `latest_per_bucket`; `players` is the last bucket; `peak_players` is the maximum over every player observation of the hour, not only the last one of each bucket (Roblox has about 5 observations per bucket, a peak inside a bucket counts); `new_visits` comes from `visit_deltas` over the whole job window and `hourly_new_visits`, never from `F.sum`, so an hour without a valid delta stays absent; `updated` from the latest `game` event (equal `ts`: the later `updated`); `up`/`down` from the latest `votes` event (equal `ts`: higher `up`, then `down`). `sort` and `meta` are not read. A game with no metric field in an hour gets no document.
- **Writes:** one multi-document transaction per hour (`session.with_transaction`, `bulk_write` in the session): readers never see an hour half recomputed. `replaceOne` with upsert, not `$set`: batch owns the whole document, so a field that is absent after a recompute disappears. `id`, `up`, `down` as BSON `Long`. Hours are written in ascending order.

### Retention sweep

- Cutoff: [`retention_cutoff`](../common/README.md#design--rules) of "now" (the latest event `ts` in Parquet). The retention is `BATCH_RETENTION_HOURS` (default 336 h = 14 days, e.g. 48 h for the demo, whose archive is shorter); less than 25 h is refused at start.
- Independent of the recompute range: every cycle sweeps every hour older than the cutoff that still has 5-minute documents (from the oldest `ts` per platform, by the `{platform, ts}` index). Deleting only together with the recompute would miss hours that pass the cutoff long after they were computed.
- Per `(platform, hour)`: the 5-minute documents of a game are deleted only when that game has a `game_metrics_hourly` document for that hour (`delete_many` with the games' ids). Games without one keep their 5-minute documents; they are counted and the oldest such hour is logged. This matches the history read, which chooses its source per game and hour.
- No transaction: the hourly documents are committed before the sweep. For the moment between both, the 5-minute and the hourly documents of an hour coexist; [`read_metrics`](../common/README.md#design--rules) picks one source per game and hour, so there is no gap and no double count.
- Known limitation: kept 5-minute documents stay the oldest in `game_metrics`, so each sweep walks every hour from them to the cutoff (a few hundred small queries at 14 days).

### Daily

Tracked set rebuild, clone matching ([matching](../matching/README.md)), saturation.

### Tracked set rules

- Rebuilt once per day.
- Roblox: games with a `sort` event in the last 7 days (from the master dataset) plus Roblox games currently in `bv_matches`. Cap ~1,500: over the cap, matched games are kept first, then games by latest `players`.
- Steam (<= 300): today's top-100 chart games plus clone candidates (all `popularnew` games released in the last 90 days, release date confirmed by `appdetails`), ranked by current players if over the cap.
- `poll_minutes`: the top ~500 Roblox games by latest `players` get 5; all other Roblox games and all Steam games get 15.
- `is_clone_candidate` as defined in [`tracked_games`](../contracts/mongodb-collections.md#tracked_games).

### Recovery and authority

- A sink outage shorter than the [Kafka retention](../contracts/kafka-topics.md#topic-settings) loses no data. Longer outages lose the expired offsets (`failOnDataLoss=false`: Spark logs it and continues); the gap is visible in `kafka_offset` of the master dataset and history is incomplete there.
- Hourly aggregates are authoritative and recomputed from Parquet. `games` and `game_metrics_hourly` can be rebuilt from Parquet after the loss of the MongoDB volume.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/batch/__main__.py` | entry point, `python -m gametrend.batch`: SparkSession, MongoDB client, the cycle forever; exit 1 on a dead SparkSession |
| `src/gametrend/batch/config.py` | `Settings`, `settings_from_env` (`KAFKA_BOOTSTRAP`, `MONGO_URI`, `ARCHIVE_CHECKPOINT_DIR`, `MASTER_DIR`, `BATCH_MAX_OFFSETS_PER_TRIGGER`, `BATCH_INTERVAL_S`, `BATCH_RECOMPUTE_HOURS`, `BATCH_RETENTION_HOURS`) |
| `src/gametrend/batch/archive.py` | master layout from the contract (`COLUMNS`, `EVENT_COLUMNS`, `KAFKA_COLUMNS`, `PARTITION_COLUMNS`, `SPARK_SCHEMA`), `to_row`, `rows_frame`, `map_frames` (the `mapInPandas` function) |
| `src/gametrend/batch/sink.py` | Spark I/O of the sink: `read_kafka`, `to_master_rows`, `start_sink`, `run_sink` -> `SinkReport` |
| `src/gametrend/batch/hours.py` | `partition_dates`, `data_now`, `hours_to_compute`, `chunks`, epoch-microsecond conversion |
| `src/gametrend/batch/hourly.py` | `aggregate_game` (one game, its hours), `frame_events`, `aggregate_frame` (the `applyInPandas` function), `to_int`, `to_document`, `group_by_hour` |
| `src/gametrend/batch/aggregate.py` | Spark I/O of the aggregates: `SparkMaster` (`latest_ts`, `earliest_ts`, `compute`), `compute_hours` (window, `dropDuplicates`, `applyInPandas`) |
| `src/gametrend/batch/retention.py` | `hours_to_sweep`, `split_ids` |
| `src/gametrend/batch/mongo.py` | `BatchStore` (`last_hour`, `write_hour` in a transaction, `sweep`), `replace_ops`, `SweepReport` |
| `src/gametrend/batch/loop.py` | `hourly_step`, `run_cycle` (step isolation, `FailureStreak`), `run_forever`, `HourlyReport` |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). `tests/batch/`, no network, no Docker; MongoDB is replaced by fakes.

- archive: columns from the contract, Kafka metadata outside the event columns, native Spark types, a row of every kind, absent fields absent, `date` at midnight, invalid records (unknown topic, broken JSON, contract failure, a kind of the other platform, impossible dates), dtypes (`Int64`, UTC), a counter above 2^53 exact, missing as null and a real 0 kept, the empty frame, invalid records counted in `map_frames`;
- hours: "now" over platforms, the first range from the archive start, open hour excluded, recompute window, no gap after a pause, chunks;
- hourly: a Roblox and a Steam hour, a peak in the middle of a bucket, the predecessor hour not in the peak, zero players kept, `new_visits` absent without predecessor or after a reset, zero delta, the 1 h gap, several hours equal to one at a time, exact duplicates change nothing, the same key with different values in 20 orders gives one result, votes-only hour, no document without metric fields, float/NaN input as Spark gives it, `to_int` refusing inexact values;
- retention and MongoDB: hours up to the cutoff, the cutoff hour not swept, demo retention, games without an hourly document kept, one transaction per hour, empty hours skipped, `Int64` and whole-document upserts;
- cycle: first run from the archive start with the cutoff from data "now", recompute after the latest aggregate, a failing hour writing nothing and skipping the sweep, no data, sink and aggregates isolated, an ERROR with the streak on every failing cycle, a dead SparkSession ending the process;
- config: required variables, defaults, overrides, invalid values, the 25 h minimum;
- `spark` marker: the sink on a local stream (partitions, `_spark_metadata`, types, nulls, invalid count, checkpoint) and sink -> master -> hourly documents ("now", earliest event, replayed copies with other offsets, empty master).

## Open questions

- Daily order of tracked set rebuild and clone matching (they depend on each other through `bv_matches` and `is_clone_candidate`). Owner C.
- What "saturation" as a daily job stores, if anything; the dashboard can compute it from `bv_matches` + `game_metrics`. Owner C with D.
- Who writes `games` when it is rebuilt from Parquet: the contract lists only speed as its writer. Either batch becomes a writer of [`games`](../contracts/mongodb-collections.md#games) or the rebuild goes through speed. Owner C with B.
