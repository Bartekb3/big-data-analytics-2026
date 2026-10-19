# Parquet master dataset

**Producer:** [batch](../batch/README.md) (Kafka -> Parquet sink).
**Consumers:** [batch](../batch/README.md) (hourly aggregates, tracked set rebuild, recovery).

## General

- Location: `/data/master` (Docker volume, see [infra](../infra/README.md)).
- Format: Parquet, Snappy-compressed.
- Append-only; kept for the whole project. Never rewritten or deleted.
- Content: every event from both [Kafka topics](kafka-topics.md), all kinds (`game`, `votes`, `sort`, `meta`, `players`), with the fields and meaning defined there. Metadata (`meta`) and the update history (`updated` in `game`) are included, so `games` and `game_metrics_hourly` can be rebuilt from Parquet after the loss of the MongoDB volume.

## Open

The following are not decided yet; the `batch` owner defines them here before the sink is implemented:

- Column types (e.g. `ts` as Parquet timestamp vs string) and how kind-specific fields are laid out (one wide schema with nullable columns vs one dataset per topic or kind).
- Partitioning (e.g. by `topic` / date).
- Whether Kafka metadata (topic, partition, offset) is stored per row.
