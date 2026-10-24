# MongoDB collections

**Producers and consumers:** per collection, see the writer and readers columns below.

Database `gametrend` (deployment: see [infra](../infra/README.md#design--rules)). General rules (types, keys, missing values, upserts) are in [conventions.md](conventions.md).

| collection | writer | readers | retention |
|---|---|---|---|
| [`game_metrics`](#game_metrics) | speed (insert/update), batch (retention delete) | speed (history for growth and projection), batch (tracked-set ranking), dashboard, common history reads, ml (projection evaluation) | 14 days |
| [`game_metrics_hourly`](#game_metrics_hourly) | batch | ml, dashboard, common history reads | whole project |
| [`games`](#games) | speed (from `meta` events) | matching, ml, dashboard | whole project |
| [`tracked_games`](#tracked_games) | batch (daily rebuild) | ingestion, matching | rebuilt daily |
| [`bv_matches`](#bv_matches) | matching (daily rebuild) | batch (tracked set), dashboard | whole project |
| [`model_coeffs`](#model_coeffs) | ml (hourly) | speed, dashboard (model quality) | whole project |
| [`model_health`](#model_health) | ml (hourly) | dashboard | whole project |
| [`ingest_stats`](#ingest_stats) | ingestion | dashboard (health page) | TBD |

## `game_metrics`

5-minute metrics per game. One document per `(platform, id, ts)`.

| field | type | meaning |
|---|---|---|
| `platform` | string | `'roblox'` / `'steam'` |
| `id` | Long | game id |
| `ts` | Date | start of the 5-minute bucket (`floor(observation ts / 5 min)`) |
| `observed_at` | Date | observation time of the player data in this document |
| `votes_observed_at` | Date | observation time of the vote data (Roblox) |
| `players` | int | concurrent players (Roblox `playing`, Steam `players`) |
| `visits` | Long | cumulative visits (Roblox) |
| `new_visits` | int | visits since the previous observation, see [metric definitions](#metric-definitions) (Roblox) |
| `favorites` | Long | cumulative favourites (Roblox) |
| `updated` | Date | last developer update of the game (Roblox) |
| `up`, `down` | Long | cumulative votes (Roblox) |
| `growth_1h`, `growth_3h`, `growth_24h` | double | percentage growth, see [metric definitions](#metric-definitions) |
| `trend_score` | double | Roblox only, see [metric definitions](#metric-definitions) |
| `projection_3h` | double | players expected at `ts + 3h`; absent when the game has less than 24 h of history |

Steam documents carry only `players` (plus keys, `observed_at`, growth and `projection_3h`). A document can exist without `players` (votes arrived first); readers must handle that.

```js
db.game_metrics.createIndex({ platform: 1, id: 1, ts: 1 }, { unique: true })
db.game_metrics.createIndex({ platform: 1, ts: -1 })    // "Trending now": latest bucket across games
```

## `game_metrics_hourly`

Hourly aggregates computed from the Parquet master dataset; authoritative for history older than the `game_metrics` retention. One document per `(platform, id, hour_ts)`, only for closed hours.

| field | type | meaning |
|---|---|---|
| `platform`, `id` | string, Long | game key |
| `hour_ts` | Date | start of the UTC hour |
| `players` | int | last player observation of the hour |
| `peak_players` | int | max player observation of the hour |
| `new_visits` | int | sum of valid visit deltas in the hour (Roblox) |
| `updated` | Date | last `updated` value of the hour (Roblox) |
| `up`, `down` | Long | last non-null vote observation of the hour (Roblox) |

```js
db.game_metrics_hourly.createIndex({ platform: 1, id: 1, hour_ts: 1 }, { unique: true })
```

## `games`

Game metadata, upserted from `meta` events.

| field | type | meaning |
|---|---|---|
| `platform`, `id` | string, Long | game key |
| `name` | string | game name |
| `genre_l1`, `genre_l2` | string | Roblox genres |
| `genres` | array of string | Steam genres |
| `description` | string | Roblox description / Steam `short_description` |
| `created` | Date | Roblox creation date |
| `released` | Date | Steam release date (00:00 UTC) |
| `observed_at` | Date | observation time of this metadata version (guards against older replays) |

```js
db.games.createIndex({ platform: 1, id: 1 }, { unique: true })
db.games.createIndex({ platform: 1, genre_l1: 1 })
```

## `tracked_games`

The set of games the producers poll, built by the [tracked set rules](../batch/README.md#tracked-set-rules).

| field | type | meaning |
|---|---|---|
| `platform`, `id` | string, Long | game key |
| `is_clone_candidate` | bool | Steam clone candidate, or Roblox game present in `bv_matches` |
| `poll_minutes` | int | `5` or `15` |

```js
db.tracked_games.createIndex({ platform: 1, id: 1 }, { unique: true })
```

## `bv_matches`

Steam -> Roblox "possible counterpart" pairs.

| field | type | meaning |
|---|---|---|
| `steam_id` | Long | Steam `appid` |
| `roblox_id` | Long | Roblox `universeId` |
| `confidence` | double | 0..1 match confidence |
| `title_score` | double | 0..1 title similarity |
| `desc_score` | double | 0..1 description similarity; absent when a description was empty |
| `matched_at` | Date | time of the matching run that produced the pair |

```js
db.bv_matches.createIndex({ steam_id: 1, roblox_id: 1 }, { unique: true })
db.bv_matches.createIndex({ roblox_id: 1 })           // Roblox details -> its Steam counterparts
```

## `model_coeffs`

Model coefficients and preprocessing. Speed reads the latest document per `model` and `platform`.

| field | type | meaning |
|---|---|---|
| `model` | string | `'expected_players'` / `'projection_3h'` |
| `platform` | string | `'roblox'` / `'steam'` |
| `trained_at` | Date | training time |
| `coeffs` | object | `{ <feature>: <coefficient>, intercept }` |
| `preprocessing` | object | everything needed to build features, e.g. `{ genre_levels: [...] }` |
| `r2`, `rmse` | double | `expected_players` evaluation; absent before enough history exists |

```js
db.model_coeffs.createIndex({ model: 1, platform: 1, trained_at: -1 }, { unique: true })
```

## `model_health`

Hourly evaluation of the +3 h projection.

| field | type | meaning |
|---|---|---|
| `platform` | string | |
| `evaluated_at` | Date | evaluation time |
| `mae_model` | double | MAE in players of `projection_3h` |
| `mae_now` | double | MAE of the "same as now" baseline |
| `mae_yesterday` | double | MAE of the "same time yesterday" baseline |
| `n` | int | number of scored targets |

```js
db.model_health.createIndex({ platform: 1, evaluated_at: -1 }, { unique: true })
```

## `ingest_stats`

Producer counters for the health page (data freshness, API errors).

| field | type | meaning |
|---|---|---|
| `ts` | Date | counter interval |
| `feed` | string | producer feed name |
| `ok` | int | records sent |
| `dropped` | int | records dropped by validation |
| `errors` | int | API errors |

```js
db.ingest_stats.createIndex({ feed: 1, ts: 1 }, { unique: true })
```

## Metric definitions

Semantics of derived fields. Speed and batch must compute them identically (shared code in [common](../common/README.md)).

- **Growth** (`growth_1h`, `growth_3h`, `growth_24h`): percentage change of actual player observations versus the observation 1 h / 3 h / 24 h earlier, with at most 15 minutes of lookup tolerance around the historical reference. Absent when the denominator is zero or either observation is missing. A latest player count of zero is valid.
- **`new_visits`**: nonnegative difference between consecutive cumulative visit observations. If the counter decreases, treat it as a reset and leave the delta absent; never negative, never a sum of cumulative counters.
- **Trend score** (Roblox): `actual_players / max(1, exp(predicted_log1p_players) - 1)`, with the prediction from the `expected_players` model. Above/below 1 means above/below expected activity, not rising/falling; growth gives direction. Steam games have no trend score.
- **Projection**: `projection_3h` of the latest 5-minute document.
- **Saturation**: the number of matched Roblox games of a Steam game and their total latest players (see [dashboard](../dashboard/README.md#matched-games)).
