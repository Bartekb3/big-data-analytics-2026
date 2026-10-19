# ML models

## Purpose

Train and evaluate two Spark MLlib linear regressions every hour and publish their coefficients for the speed layer: the expected-players model (basis of the Roblox trend score) and the +3 h player projection. Not responsible for applying the models to live data (that is [speed](../speed/README.md)).

## Owner

Package D, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| expected players model (Roblox) | planned | |
| +3 h projection model (per platform) | planned | |
| projection evaluation (`model_health`) | planned | |

## Inputs

- [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly) - last 7 days, read with the MongoDB Spark Connector.
- [`games`](../contracts/mongodb-collections.md#games) - `genre_l1`, `created`.
- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics) - `projection_3h` and actual `players` for evaluation.

## Outputs

- [`model_coeffs`](../contracts/mongodb-collections.md#model_coeffs) - coefficients and preprocessing, hourly.
- [`model_health`](../contracts/mongodb-collections.md#model_health) - projection evaluation, hourly.

## Design / rules

Both models are retrained hourly (step 4 of the [batch hourly sequence](../batch/README.md#hourly-sequence)) on the last 7 days of `game_metrics_hourly`. Coefficients and preprocessing are stored together so the speed layer can rebuild features exactly.

### Expected players model (Roblox only)

- Target `log(1 + players)` (zero players are valid).
- Features are context only: hour of day, day of week, `genre_l1` (one-hot, fitted on training rows; unseen levels encoded as all zeros), game age (days since `created`), trailing mean of `log(1 + players)` over the previous 7 days (earlier observations only), like ratio `up / (up + down)` (neutral 0.5 when votes are missing). The game's own recent growth is excluded, so the model is not circular. No future observations are used.
- Cold start: a game is scored only after 24 h of history.
- Evaluation: a fit on the first six days is tested on the last completed day (R2, RMSE in players, sample count), then the serving model is refitted on all seven days. Evaluation is unavailable before enough history exists.
- Steam games get no trend score; they are ranked by 1 h / 3 h / 24 h growth.

### +3 h projection (one model per platform)

- Target `log(1 + players)` three hours ahead.
- Features: current `log(1 + players)`, its least-squares slope over the last 3 h (at least 3 observations), hour of day of the target time (one-hot).
- Applied by the speed layer to every 5-minute document of a game with at least 24 h of history; `projection_3h = max(0, expm1(prediction))`. Otherwise `projection_3h` is absent.
- Evaluation (hourly, written to `model_health`): on the last completed UTC day, for every 5-minute document with `projection_3h`, the target is the actual `players` of the bucket at `ts + 3h`. MAE in players against two baselines on exactly the same targets: **same as now** (`players` at `ts`) and **same time yesterday** (`players` at `ts + 3h - 24h`). Only targets where all three values exist are scored; `n` is the sample count.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/ml/` | not implemented yet |

## Tests

```
uv run pytest tests/ml
uv run pytest tests/ml -m "not spark"
```

## Open questions

- Exact keys of `coeffs` / `preprocessing` (feature names); they are shared with speed, so agree them with owner B before implementing.
- Projection is trained on hourly data but applied to 5-minute documents; confirm the slope feature is computed consistently. Owner D with B.
