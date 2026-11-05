# Architecture index

Single source of truth for the design of GameTrend Radar. Start here, then open only the documents your task needs (see [Reading the architecture docs](../AGENTS.md#reading-the-architecture-docs)).

## Data flow

```
 Roblox / Steam APIs
        │
        ▼
 [ingestion] producers ──► Kafka topics ───────────────► [speed] Spark Structured Streaming
        ▲                  (roblox.snapshots,                 │  reads model_coeffs
        │                   steam.players)                    ▼
        │                        │                      MongoDB: game_metrics, games
        │                        ▼                            │
        │                  [batch] Kafka -> Parquet sink      │
        │                        │                            │
        │                        ▼                            │
        │                  Parquet master dataset             │
        │                        │                            │
        │                        ▼                            │
        │                  [batch] hourly aggregates,         │
        │                  retention, tracked set ──► MongoDB: game_metrics_hourly, tracked_games
        │                                                     │
        │                  [matching] daily clone matching ─► MongoDB: bv_matches
        │                  [ml] hourly training ────────────► MongoDB: model_coeffs, model_health
        │                                                     │
        └──────── tracked_games ◄─────────────────────────────┤
                                                              ▼
                                                   [dashboard] Streamlit

 [common] shared metric logic used by speed, batch and dashboard
 [infra]  docker compose, Kafka, MongoDB replica set, volumes
 [demo]   accelerated data for the live presentation

 [collector] supporting tool outside the Lambda flow: archives raw API responses
             (original timestamps) for replay producers
 [mockapi]   serves the collector archive in the shape of the real APIs, on a replay
             clock; ingestion polls it instead of the rate-limited live APIs
```

## Table of contents

| path | owns | status |
|---|---|---|
| this file, § Data flow | how data moves between areas | - |
| this file, § Table of contents | which file owns which fact, area status summary | - |
| this file, § Owners | packages, owners, `git config user.name`, GitHub handles | - |
| [RUNNING.md](RUNNING.md) | prerequisites, setup, services and ports, run and test commands, troubleshooting | living |
| [_template.md](_template.md) | structure of an area document | - |
| **Contracts** | | |
| [contracts/conventions.md](contracts/conventions.md) | types, timestamps, IDs, missing values, keys | agreed |
| [contracts/kafka-topics.md](contracts/kafka-topics.md) | Kafka topics and their settings (partitions, retention), event payloads, producers and consumers | agreed |
| [contracts/parquet-master.md](contracts/parquet-master.md) | Parquet master dataset (location, format, content, retention), its producers and consumers | agreed (partly open) |
| [contracts/mongodb-collections.md](contracts/mongodb-collections.md) | MongoDB collections, keys, field semantics, metric definitions, writers, readers, retention | agreed |
| **Areas** (each also owns its per-module status, design rules, code map and test coverage) | | |
| [ingestion/README.md](ingestion/README.md) | Roblox and Steam producers | planned |
| [speed/README.md](speed/README.md) | streaming metrics into MongoDB | planned |
| [batch/README.md](batch/README.md) | Parquet sink, hourly aggregates, retention, tracked set | planned |
| [matching/README.md](matching/README.md) | daily Steam -> Roblox clone matching | planned |
| [ml/README.md](ml/README.md) | expected-players and +3 h projection models | planned |
| [dashboard/README.md](dashboard/README.md) | Streamlit dashboard | planned |
| [common/README.md](common/README.md) | shared metric logic and history reads | planned |
| [infra/README.md](infra/README.md) | docker compose, Kafka, MongoDB, volumes | planned |
| [demo/README.md](demo/README.md) | accelerated data for the presentation | planned |
| [collector/README.md](collector/README.md) | raw API archive for replay (supporting tool, not part of the Lambda flow) | in-progress |
| [mockapi/README.md](mockapi/README.md) | mock Roblox and Steam APIs serving the archive on a replay clock | in-progress |

Area status here is a summary; the per-module status lives in each area document. Status meanings: [AGENTS.md](../AGENTS.md#keeping-the-documentation-true).

## Owners

Replace the placeholders and keep `git config user.name` exact (agents use it to find their area). GitHub handles are for requesting PR reviews.

| package | areas | owner | `git config user.name` | GitHub |
|---|---|---|---|---|
| A | `ingestion`, `infra`, `demo` | Jan Cwalina | Janusz99bis | `@TODO-A` |
| B | `speed`, `common` | TODO | TODO | `@TODO-B` |
| C | `batch`, `matching` | TODO | TODO | `@TODO-C` |
| D | `ml`, `dashboard` | TODO | TODO | `@TODO-D` |
| E | `collector`, `mockapi` | Jan Cwalina | Janusz99bis | `@TODO-E` |
