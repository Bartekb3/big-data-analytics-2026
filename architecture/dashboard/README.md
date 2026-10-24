# Dashboard

## Purpose

Live Streamlit dashboard: trending games, game details with player history, growth and projection, Steam opportunities with their matched Roblox games, and a health page. Read-only; it never writes to MongoDB.

## Owner

Package D, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| "Trending now" view | planned | |
| game details (Roblox / Steam) | planned | |
| Steam opportunities + matched games | planned | |
| health page (freshness, API errors, model health) | planned | |

## Inputs

- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics), [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly) - player history, through the shared history read in [common](../common/README.md).
- [`games`](../contracts/mongodb-collections.md#games) - names and metadata.
- [`bv_matches`](../contracts/mongodb-collections.md#bv_matches) - matched games.
- [`model_health`](../contracts/mongodb-collections.md#model_health), [`model_coeffs`](../contracts/mongodb-collections.md#model_coeffs) - model quality.
- [`ingest_stats`](../contracts/mongodb-collections.md#ingest_stats) - data freshness and API errors.

## Outputs

None.

## Design / rules

### Navigation

- Game details are selected by `(platform, id)`. Clicking a game name opens that game's details: metadata, player history, growth and projection when available.
- Missing history or metrics ([shown as unavailable](../contracts/conventions.md#missing-values)) do not prevent opening details.

### Matched games

- Selecting a Steam opportunity opens its Steam details and a matched-Roblox table. Read `bv_matches` by `steam_id`, look up `games` with `platform = 'roblox'` and `id = roblox_id` (`$lookup`, or a second `find` with `$in`), and attach the latest `game_metrics` document with a non-null `players` for that Roblox game. One row per matched Roblox ID, never one per historical observation.
- Each row shows a clickable name, latest players, `growth_3h`, creation date, confidence and the metrics timestamp. Order by latest players descending (missing values last), then Roblox ID.
- Roblox details list all linked possible Steam counterparts, each linking to its Steam details. For each Steam ID, show the same matched-Roblox table under **Other Roblox games matched to this Steam game**, excluding the current Roblox ID. Keep groups separate when a game has multiple Steam matches; no genre-based fallback or extra similarity model.
- Steam games with no matches show an empty matched-games table. Roblox games with no Steam match show their own metrics and **No Steam match found**.
- Saturation: match counts use distinct Roblox IDs for the selected Steam game; player totals use each matched game's latest available player count once. Show the number of matched games without player data next to the known-player total; never treat missing counts as zero. A successful match run with no matches gives count and total zero.
- User-facing labels are **matched games** or **possible counterparts**, never claims of verified clones or a proven Steam origin.

### Metrics shown

- Roblox games are ranked by trend score, Steam games by 1 h / 3 h / 24 h growth (see [metric definitions](../contracts/mongodb-collections.md#metric-definitions)).
- Model evaluation (`r2`/`rmse`, `model_health`) is shown as unavailable before enough history exists.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/dashboard/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests).

Test the query/assembly logic (row building, ordering, missing values) separately from Streamlit rendering.

## Open questions

- Refresh interval of the live views (also matters for the [demo](../demo/README.md)). Owner D.
