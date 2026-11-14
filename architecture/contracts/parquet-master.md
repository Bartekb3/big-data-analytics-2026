# Parquet master dataset

**Producer:** [batch](../batch/README.md) (Kafka -> Parquet sink).
**Consumers:** [batch](../batch/README.md) (hourly aggregates, tracked set rebuild, recovery).

## General

- Location: `/data/master` (Docker volume, see [infra](../infra/README.md)).
- Format: Parquet, Snappy-compressed, written by the Spark file sink. Its `_spark_metadata` log lists the committed files: readers read the directory with Spark (`spark.read.parquet("/data/master")`), which follows that log, and never list the Parquet files themselves.
- Append-only; kept for the whole project. Never rewritten or deleted.
- Content: every event from both [Kafka topics](kafka-topics.md) that passes the contract check, all kinds (`game`, `votes`, `sort`, `meta`, `players`), with the fields and meaning defined there. Records that fail the check are dropped and counted by the sink, as by the other consumers; they are not part of the dataset. Metadata (`meta`) and the update history (`updated` in `game`) are included, so `games` and `game_metrics_hourly` can be rebuilt from Parquet (see [batch recovery](../batch/README.md#recovery-and-authority)).

## Layout

One wide dataset for both topics, one row per event. Columns:

| column | type | meaning |
|---|---|---|
| `platform` | string | `'roblox'` / `'steam'`, from the topic |
| every field of the [Kafka contract](kafka-topics.md) | see below | same name as in Kafka (`playing`, `players`, `short_description`, ...); null when the event has no such field |
| `kafka_partition`, `kafka_offset` | int64 | where the record was read from; metadata, not part of the event |
| `date` | date | UTC date of `ts` (observation time, not write time) |

Types of the Kafka fields: integers int64; ISO-8601 timestamps (`ts`, `updated`, `created`) Parquet timestamp (UTC instant); `released` date; strings string; `genres` list of string.

A null is an absent field, never a substitute value ([conventions § Missing values](conventions.md#missing-values)).

Partitioning: `platform`, then `date` (`/data/master/platform=roblox/date=2026-11-06/...`). Replayed events with an old `ts` land in the partition of their observation date.

## Duplicates

Re-runs, replays and producer restarts write the same event again (with another `kafka_offset`). The dataset keeps them; readers handle them:

- Exact copies, equal in every column except `kafka_partition`, `kafka_offset` (and `date`, which follows from `ts`), may be dropped.
- Rows with the same key (`platform`, `id`, `kind`, `ts`, and for `sort` also `country` and `sort`) but different values are not copies: readers resolve them with the [metric definitions](mongodb-collections.md#metric-definitions) (latest observation per bucket, ties by the values), never by picking an arbitrary row.
