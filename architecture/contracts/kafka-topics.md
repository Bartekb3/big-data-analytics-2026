# Kafka topics

**Producers:** [ingestion](../ingestion/README.md), [demo](../demo/README.md) (format must be identical).
**Consumers:** [speed](../speed/README.md), [batch](../batch/README.md) (Parquet sink).

## General

- Encoding: JSON, one event per message.
- Message key: the game `id` (as a string of digits).
- Only the fields listed here are sent. Records that fail validation are dropped by the producer and counted in [`ingest_stats`](mongodb-collections.md#ingest_stats).
- Every field listed is required, so a record without one fails validation, except in `meta` events: there only `name` is required, and a field the API has no value for is absent ([missing values](conventions.md#missing-values)).

Common fields of every event:

| field | type | meaning |
|---|---|---|
| `kind` | string | event kind, see the tables below |
| `ts` | ISO-8601 UTC string | producer observation time, see [conventions](conventions.md#time) |
| `id` | integer | Roblox `universeId` / Steam `appid` |

### Topic settings

[infra](../infra/README.md) creates the topics at startup with these settings. Automatic topic creation is off, so a mistyped topic name fails instead of creating a new topic.

| topic | partitions | replication factor |
|---|---|---|
| `roblox.snapshots` | 3 | 1 |
| `steam.players` | 1 | 1 |

Both topics:

| setting | value | why |
|---|---|---|
| `retention.ms` | `259200000` (72 h) | the shortest time events stay readable |
| `segment.ms` | `86400000` (24 h) | Kafka deletes a whole segment, once its newest record is past `retention.ms`, so events stay readable for 72 h to about 96 h; with the default segment time (7 days) it would be up to about 10 days |
| `message.timestamp.type` | `LogAppendTime` | the record timestamp is the broker's time, so replayed events with an old `ts` do not expire at once |

Consumers take event time from `ts`, never from the record timestamp.

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
| | `sort` | string | ID of the explore sort (e.g. `top-trending`) |
| | `rank` | integer | position of the game in that sort, starting at 1 |
| | `playing` | integer | player count shown in the sort |
| `meta` | `name` | string | game name |
| | `genre_l1` | string | top-level genre |
| | `genre_l2` | string | second-level genre |
| | `created` | ISO-8601 UTC string | creation date of the game |
| | `description` | string | game description |

`updated` travels with every `game` event, so the Roblox update history is kept in the master dataset.

A game can be in several sorts of one country at once, so one observation gives one `sort` event per sort. `(id, ts, country, sort)` identifies a `sort` event.

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
