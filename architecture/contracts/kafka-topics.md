# Kafka topics

**Producers:** [ingestion](../ingestion/README.md), [demo](../demo/README.md) (format must be identical).
**Consumers:** [speed](../speed/README.md), [batch](../batch/README.md) (Parquet sink).

## General

- Encoding: JSON, one event per message.
- Message key: the game `id` (as a string of digits).
- Retention: 72 h.
- Only the fields listed here are sent. Records that fail validation are dropped by the producer and counted in [`ingest_stats`](mongodb-collections.md#ingest_stats).

Common fields of every event:

| field | type | meaning |
|---|---|---|
| `kind` | string | event kind, see the tables below |
| `ts` | ISO-8601 UTC string | producer observation time (unchanged on retry) |
| `id` | integer | Roblox `universeId` / Steam `appid` |

## `roblox.snapshots`

| kind | extra field | type | meaning |
|---|---|---|---|
| `game` | `playing` | integer | current concurrent players |
| | `visits` | integer | cumulative visit counter |
| | `favorites` | integer | cumulative favourites counter |
| | `updated` | ISO-8601 UTC string | last update of the game by its developer |
| `votes` | `up` | integer | cumulative up-votes |
| | `down` | integer | cumulative down-votes |
| `sort` | `country` | string | country of the explore sort |
| | `rank` | integer | position of the game in that sort |
| | `playing` | integer | player count shown in the sort |
| `meta` | `name` | string | game name |
| | `genre_l1` | string | top-level genre |
| | `genre_l2` | string | second-level genre |
| | `created` | ISO-8601 UTC string | creation date of the game |
| | `description` | string | game description |

`updated` travels with every `game` event, so the Roblox update history is kept in the master dataset.

## `steam.players`

| kind | extra field | type | meaning |
|---|---|---|---|
| `players` | `players` | integer | current concurrent players |
| `meta` | `name` | string | game name |
| | `genres` | array of strings | genre descriptions |
| | `released` | `YYYY-MM-DD` string | release date |
| | `short_description` | string | store short description |

Steam charts (`GetMostPlayedGames`) and store search (`popularnew`) are not streamed; they are read daily by [batch](../batch/README.md) to build the tracked set.

API sources and polling cadence are internal to [ingestion](../ingestion/README.md#api-sources-and-cadence).
