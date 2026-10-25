# Speed layer

## Purpose

Consume Kafka events with Spark Structured Streaming and keep 5-minute game metrics and game metadata in MongoDB up to date, including growth, trend score and the +3 h projection. Not responsible for training models ([ml](../ml/README.md)) or for long-term history ([batch](../batch/README.md)).

## Owner

Package B, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| streaming job (Kafka -> `game_metrics`, `games`) | planned | |
| conditional upsert writer | planned | |
| live scoring (growth, trend score, projection) | planned | |

## Inputs

- [`roblox.snapshots`, `steam.players`](../contracts/kafka-topics.md) - all event kinds.
- [`model_coeffs`](../contracts/mongodb-collections.md#model_coeffs) - latest document per model and platform, read at the start of each micro-batch.
- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - history needed for growth and projection features.

## Outputs

- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - 5-minute documents.
- [`games`](../contracts/mongodb-collections.md#games) - metadata from `meta` events.

## Design / rules

### Event roles

- `game` events supply the player history (`players` <- `playing`, `visits`, `favorites`, `updated`).
- `votes` events only enrich the same time bucket (`up`, `down`).
- `sort` events are archived for the daily tracked-set rebuild and never write player counts.
- `meta` events upsert `games`.
- Steam `players` events write `players`.

### Buckets and deduplication

- One `game_metrics` document per `(platform, id)` and 5-minute bucket (see [`ts`](../contracts/mongodb-collections.md#game_metrics)).
- Within a bucket, the latest event of each kind wins by its original observation timestamp. Older observations are rejected even if they arrive later.
- `observed_at` guards player data, `votes_observed_at` guards vote data, `games.observed_at` guards metadata. The same rule applies to Steam.
- A write sets only the fields of its event kind. A `votes` event that arrives before the `game` event of its bucket creates the document without `players`; the later `game` event fills it.

### Conditional upsert pattern

Writes go through `foreachBatch` with `pymongo` `bulk_write(..., ordered=False)`; the Spark connector's plain upsert cannot express the timestamp guard.

```python
UpdateOne(
    {"platform": p, "id": i, "ts": bucket,
     "observed_at": {"$not": {"$gte": obs_ts}}},   # matches missing, null or older
    {"$set": {"observed_at": obs_ts, "players": ..., "visits": ..., "favorites": ...}},
    upsert=True)
```

If a newer observation already exists, the filter does not match, the upsert tries to insert a duplicate key and fails with `E11000`. This is the expected "older observation rejected" outcome: count it, do not retry it.

### Scoring

- Growth and `new_visits` follow the [metric definitions](../contracts/mongodb-collections.md#metric-definitions), using the shared code from [common](../common/README.md).
- Trend score (Roblox) uses the latest `expected_players` coefficients and the [cold start rule](../ml/README.md#expected-players-model-roblox-only) of that model.
- `projection_3h` uses the latest `projection_3h` coefficients and is applied as described in [ml](../ml/README.md#3-h-projection-one-model-per-platform).

### Recovery

- A speed-only outage resumes from the speed checkpoint ([infra](../infra/README.md#design--rules)) as long as it is shorter than the [Kafka retention](../contracts/kafka-topics.md#general).
- Timestamp-conditional upserts make replays safe; a unique index alone would let an old replay overwrite newer values.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/speed/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). Coverage: TBD.

## Open questions

- How growth lookups read history inside a micro-batch (MongoDB reads vs Spark state). Owner B.
- Trend score needs `genre_l1` and game age (from `created`) from [`games`](../contracts/mongodb-collections.md#games), but `games` is neither in Inputs nor listed as read by speed in the contract. Decide how speed gets them and update Inputs and the contract. Owner B with D.
