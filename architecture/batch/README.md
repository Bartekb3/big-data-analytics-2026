# Batch layer

## Purpose

Archive all events in the Parquet master dataset, compute authoritative hourly aggregates, apply retention to 5-minute metrics and rebuild the tracked set daily. Not responsible for clone matching ([matching](../matching/README.md)) or model training ([ml](../ml/README.md)), although it schedules them.

## Owner

Package C, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| Kafka -> Parquet sink | planned | |
| hourly aggregates | planned | |
| retention transaction | planned | |
| tracked set rebuild (daily) | planned | |
| job scheduling (hourly / daily sequence) | planned | |

## Inputs

- [`roblox.snapshots`, `steam.players`](../contracts/kafka-topics.md) - archived by the sink.
- [Parquet master dataset](../contracts/parquet-master.md) - source of hourly aggregates and of `sort` events for the tracked set.
- [`bv_matches`](../contracts/mongodb-collections.md#bv_matches) - Roblox games kept in the tracked set.
- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - latest players for tracked-set ranking; documents to delete.
- Steam APIs (external): `GetMostPlayedGames` (top-100 chart), store search `popularnew`, `appdetails` (release date confirmation).

## Outputs

- [Parquet master dataset](../contracts/parquet-master.md).
- [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly).
- Deletions in [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) (retention).
- [`tracked_games`](../contracts/mongodb-collections.md#tracked_games).

## Design / rules

### Hourly sequence

Run in this order; a job never overlaps with itself:

1. **Kafka -> Parquet sink:** Structured Streaming with the `availableNow` trigger and a checkpoint on a volume, writing the [Parquet master dataset](../contracts/parquet-master.md).
2. **Hourly aggregates** into [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly), computed from Parquet with the shared functions from [common](../common/README.md), never from speed output. Only closed hours covered by the archive.
3. **Retention:** in ONE multi-document transaction (`session.with_transaction`), upsert the hourly documents and delete 5-minute documents older than the [retention cutoff](../common/README.md#design--rules). 5-minute documents whose hour has no hourly aggregate are kept.
4. **Training and evaluation** of both models ([ml](../ml/README.md)).

### Daily

Tracked set rebuild, clone matching ([matching](../matching/README.md)), saturation.

### Tracked set rules

- Rebuilt once per day.
- Roblox: games with a `sort` event in the last 7 days (from the master dataset) plus Roblox games currently in `bv_matches`. Cap ~1,500: over the cap, matched games are kept first, then games by latest `players`.
- Steam (<= 300): today's top-100 chart games plus clone candidates (all `popularnew` games released in the last 90 days, release date confirmed by `appdetails`), ranked by current players if over the cap.
- `poll_minutes`: the top ~500 Roblox games by latest `players` get 5; all other Roblox games and all Steam games get 15.
- `is_clone_candidate` as defined in [`tracked_games`](../contracts/mongodb-collections.md#tracked_games).

### Recovery and authority

- A sink outage shorter than the [Kafka retention](../contracts/kafka-topics.md#general) loses no data. Longer outages lose the expired offsets: record the gap and mark history incomplete.
- Hourly aggregates are authoritative and recomputed from Parquet. `games` and `game_metrics_hourly` can be rebuilt from Parquet after the loss of the MongoDB volume.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/batch/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). Coverage: TBD.

## Open questions

- Scheduler for the hourly and daily sequences (cron in a container, a loop service, ...). Owner C with A (infra).
- Daily order of tracked set rebuild and clone matching (they depend on each other through `bv_matches` and `is_clone_candidate`). Owner C.
- What "saturation" as a daily job stores, if anything; the dashboard can compute it from `bv_matches` + `game_metrics`. Owner C with D.
