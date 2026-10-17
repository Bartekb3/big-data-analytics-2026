# Contracts (draft, to be agreed by the team before any code)

Single source of truth for Kafka payloads and PostgreSQL tables. Field names come from the real API samples in `api_test/responses/`. Change this file first, then the code.

## Kafka topics (JSON, key = game id, retention 24 h)

### `roblox.snapshots`
Common fields: `kind`, `ts` (ISO-8601 UTC, producer time), `id` (Roblox `universeId`).

| kind | extra fields | source |
|---|---|---|
| `game` | `playing`, `visits`, `favorites` | `games.roblox.com/v1/games` |
| `votes` | `up`, `down` | `games.roblox.com/v1/games/votes` |
| `sort` | `country`, `sort_id`, `rank`, `playing`, `up`, `down` | `explore-api/v1/get-sorts` (`playerCount`, `totalUpVotes`, `totalDownVotes`) |

### `steam.players`
`ts`, `id` (Steam `appid`), `players` (`player_count`).

## Tracked set rules

- The Roblox producer registers every game seen in a ranking list (`sort` events) or refreshes its `last_seen`. The daily matcher registers clone candidates (`is_clone_candidate = true`).
- Cadence: the top ~500 tracked Roblox games by latest `players` are polled every 5 min, the rest every 15 min. Steam games (<= 300: chart games and clone candidates, ranked by current players) every 15 min. Clone candidates are all `popularnew` games released in the last 90 days.
- The cap is per platform: ~1,500 Roblox games and <=300 Steam games. At the Roblox cap: evict games with the oldest `last_seen` first; clone candidates are evicted last.

## Clone matching rules

- `confidence = w * title_score + (1 - w) * desc_score`, start with `w = 0.5`. `title_score` is the fuzzy token-set ratio of the normalised titles (0..1). `desc_score` is the TF-IDF cosine similarity (0..1) between the Steam `short_description` and the Roblox `description`; the TF-IDF vocabulary/IDF is fitted on the descriptions of the current candidates and their search hits. If either description is empty, `confidence = title_score`. No bonus for the Steam title appearing in the description.
- Split the manual sample of ~100 pairs into tuning and held-out evaluation sets before choosing threshold and `w`. Report held-out precision and sample size; inspect rejected candidates for missed matches. `title_score` and `desc_score` are stored so tuning can be redone without refetching.
- Hard rule: Roblox `created` must be after the Steam release date.
- After a successful daily recomputation, matches that are no longer produced are deleted. If a Roblox or Steam API call fails, the run is aborted and the previous `bv_matches` stay unchanged.

## Matched-game reads and navigation

- Game details are selected by `(platform, id)`. Clicking a game name opens that game's details, including its metadata, player history, growth and projection when available.
- Selecting a Steam opportunity opens its Steam details and a matched-Roblox table. Read `bv_matches` by `steam_id`, join `games` on `platform = 'roblox'` and `id = roblox_id`, and attach the latest metrics row with a non-null `players` from `game_metrics_view` for that Roblox game. Return one row per matched Roblox ID, never one per historical observation.
- Each matched-Roblox row shows a clickable name, latest players, `growth_3h`, creation date, confidence and the metrics timestamp. Order by latest players descending (missing values last), then Roblox ID. Missing history or metrics appear as unavailable, not zero, and do not prevent opening details.
- Roblox details list all linked possible Steam counterparts, each linking to its Steam details. For each Steam ID, read the same matched-Roblox table under **Other Roblox games matched to this Steam game**, excluding the current Roblox ID. Keep groups separate when a game has multiple Steam matches; no genre-based fallback or extra similarity model is used.
- Steam games with no matches show an empty matched-games table. Roblox games with no Steam match show their own metrics and **No Steam match found**. Missing matches do not block the details page.
- Match counts use distinct Roblox IDs for the selected Steam game; player totals use each matched game's latest available player count once. Show the number of matched games without player data alongside the known-player total; never treat missing counts as zero. A successful match run with no matches has count and total zero.
- User-facing labels are **matched games** or **possible counterparts**, not claims of verified clones or a proven Steam origin. The existing `bv_matches` schema and matching algorithm remain the source of these relationships.

## Roblox event precedence and deduplication

- **Roles:** `game` events supply the player history (`players`, `visits`, `favorites`). `votes` events only enrich the same time bucket (`up`, `down`). `sort` events only drive discovery (register or refresh a row in `tracked_games`, update `last_seen`) and never write player counts.
- **Time bucket:** `ts` is floored to 5 minutes: `bucket = floor(ts / 5 min)`. One row per `(platform, id, grain, bucket)`.
- **Deduplication:** within a bucket the latest event of each kind wins by its original observation timestamp. Conditional upserts reject older observations even if they arrive later; producers retain the timestamp when retrying the same observation. `observed_at` records player observation time and `votes_observed_at` records vote observation time. Apply the same player rule to Steam.
- **Partial updates:** a write sets only the columns its event kind provides (`INSERT ... ON CONFLICT DO UPDATE SET <own columns>`). Absent fields are never overwritten with NULL. A `votes` event that arrives before the `game` event of its bucket creates the row with `players = NULL`; the later `game` event fills it.
- **Steam:** `steam.players` events write `players` only (no votes, visits or favorites).

## PostgreSQL tables

```sql
-- metadata, upserted by producers only when changed
CREATE TABLE games (
  platform  text NOT NULL,              -- 'roblox' | 'steam'
  id        bigint NOT NULL,
  name      text NOT NULL,
  genre_l1  text, genre_l2 text,        -- Roblox
  genres    text[],                     -- Steam
  description text,                     -- Roblox description / Steam short_description (clone matching)
  created   timestamptz,                -- Roblox creation date
  released  date,                       -- Steam release date
  updated   timestamptz,                -- Roblox last update
  observed_at timestamptz NOT NULL,      -- when this metadata version was observed
  PRIMARY KEY (platform, id)
);

-- append-only history of Roblox updates; the producer inserts a row when games.updated changes
-- Historical updates cannot be recovered from an API returning only the latest update.
-- Include games and game_updates in the daily metadata export described below.
CREATE TABLE game_updates (
  platform text NOT NULL, id bigint NOT NULL,
  updated timestamptz NOT NULL, seen_at timestamptz NOT NULL,
  PRIMARY KEY (platform, id, updated)
);

-- tracked set (Roblox cap ~1,500; Steam <=300)
CREATE TABLE tracked_games (
  platform text NOT NULL, id bigint NOT NULL,
  last_seen timestamptz NOT NULL, is_clone_candidate boolean DEFAULT false,
  PRIMARY KEY (platform, id)
);

-- speed layer writes grain='5m'; batch writes grain='1h' and deletes 5m rows older than 14 days
CREATE TABLE game_metrics (
  platform text NOT NULL, id bigint NOT NULL,
  ts timestamptz NOT NULL, grain text NOT NULL,  -- '5m' | '1h'
  observed_at timestamptz, votes_observed_at timestamptz,
  players int, peak_players int, visits bigint, new_visits bigint, favorites int,
  up int, down int,
  growth_1h real, growth_3h real, growth_24h real,
  -- Forecasts are read from predictions, not copied into every metrics bucket.
  trend_score real,                              -- actual / expected
  PRIMARY KEY (platform, id, grain, ts)         -- ts is the 5-min (or hour) bucket start
);

-- Hourly aggregation: batch independently normalizes archived Parquet events with
-- the same shared calculations as speed; never use speed output as the master input.
-- Aggregate only closed hours covered by the archive checkpoint:
--   players = last value of the hour, peak_players = max, visits = last, favorites = last,
--   new_visits = sum of valid deltas, up/down = last non-null vote observation,
--   growth_* = shared calculation; hourly trend scores use the model version active
--   at scoring time and are not silently rewritten with a newer model.
-- Keep observation timestamps and do not turn missing observations into zeroes.
-- The batch job inserts the hourly rows and deletes the 5-min rows older than the cutoff
-- (14 days) in ONE transaction, so readers never see a gap.
-- All database sessions use UTC. Readers use one view and never touch grains directly.
-- Retain older fine-grained rows as a fallback if their authoritative hour is missing:
CREATE VIEW game_metrics_view AS
  SELECT m.* FROM game_metrics m WHERE m.grain = '5m'
    AND (m.ts >= date_trunc('hour', now() - interval '14 days')
      OR NOT EXISTS (
        SELECT 1 FROM game_metrics h
        WHERE h.platform = m.platform AND h.id = m.id AND h.grain = '1h'
          AND h.ts = date_trunc('hour', m.ts)))
  UNION ALL
  SELECT * FROM game_metrics WHERE grain = '1h' AND ts <  date_trunc('hour', now() - interval '14 days');
-- Hourly rows are authoritative for the hours they cover; 5-min rows are provisional
-- and are replaced by (not corrected into) the hourly rows.

-- One row per game and forecast origin, containing all 12 steps.
-- Curve element k targets t + k * 15m; the final element is the +3h forecast.
-- The speed driver publishes the curve atomically; batch evaluates its final step.
CREATE TABLE predictions (
  platform text NOT NULL, id bigint NOT NULL,
  ts timestamptz NOT NULL,                       -- origin t, UTC 15-min boundary
  target_ts timestamptz NOT NULL,                -- t + 3h
  generated_at timestamptz NOT NULL,
  model_revision text NOT NULL,                 -- pinned amazon/chronos-2 revision
  forecast_curve real[] NOT NULL,               -- median, 12 steps
  lower_curve real[] NOT NULL, upper_curve real[] NOT NULL, -- q0.1/q0.9
  CHECK (cardinality(forecast_curve) = 12 AND cardinality(lower_curve) = 12
         AND cardinality(upper_curve) = 12),
  baseline_now real, baseline_yesterday real, baseline_linear real,
  actual real, actual_ts timestamptz,             -- closest real observation within 10 min
  PRIMARY KEY (platform, id, ts)
);

-- batch outputs
CREATE TABLE bv_matches (
  steam_id bigint NOT NULL, roblox_id bigint NOT NULL,
  confidence real NOT NULL, title_score real, desc_score real, matched_at date NOT NULL,
  PRIMARY KEY (steam_id, roblox_id)
);

CREATE TABLE model_coeffs (
  platform text NOT NULL, trained_at timestamptz NOT NULL,
  coeffs jsonb NOT NULL,                         -- feature -> coefficient, plus intercept
  preprocessing jsonb NOT NULL,                  -- genre levels, any scaling means/stds
  r2 real, rmse real,
  PRIMARY KEY (platform, trained_at)
);

-- producer counters for the health view
CREATE TABLE job_runs (
  job text NOT NULL, started_at timestamptz NOT NULL,
  finished_at timestamptz, status text NOT NULL, -- running | succeeded | failed | skipped
  data_through timestamptz, error text,
  PRIMARY KEY (job, started_at)
);

CREATE TABLE ingest_stats (
  ts timestamptz NOT NULL, feed text NOT NULL,
  ok int, dropped int, errors int,
  PRIMARY KEY (feed, ts)
);
```

## Recovery and authority

- Kafka keeps messages for 24 h. If the archive sink misses expired offsets, those observations cannot be recovered; explicitly record the offset gap and mark history incomplete, even after new data becomes fresh. A speed-only outage is recoverable from the intact archive. This is a local, single-machine prototype, not a highly available deployment.
- Speed and archive consumers have distinct persistent checkpoints. The Kafka-to-Parquet sink uses a checkpoint on a volume. PostgreSQL writes use timestamp-conditional upserts; a primary key alone does not prevent an old replay from replacing newer values. Replay never regenerates forecasts as if they had been made historically.
- Hourly batch rows are authoritative, recomputed from Parquet. A manual replay command rebuilds recent 5-min rows and historical hourly rows from the archive using shared functions; pause live metrics publication during a rebuild. Forecasts and their evaluation evidence are retained separately.
- The serving view and deletion job use the SAME UTC hour-aligned 14-day cutoff. Publish/check archive coverage before deleting fine-grained rows; retain rows when the required hourly data is incomplete.
- Every day the batch worker exports metadata, update history, tracked IDs and matches to versioned files outside the PostgreSQL volume. This permits recovery to the last export, not complete recovery after host/disk loss. Current APIs cannot reconstruct previously observed update timestamps. Exports share the existing master-data volume; no extra service is needed.
- Each producer group, archive run, aggregation, training, matching and forecasting run records job_runs status. Health shows job freshness separately from source data freshness, offset gaps, forecast failures, runtime and coverage. Jobs do not overlap with themselves; archive persistence takes priority over training and matching.

## Expected players model

- One model per platform, trained hourly on the last 7 days of hourly rows of the tracked games of that platform; target `log(1 + players)` (zero players are valid).
- Features, Roblox: hour of day, day of week, `genre_l1` (one-hot), game age (days), trailing mean of `log(1 + players)` over the previous 7 days, like ratio `up / (up + down)`. Use the latest vote observation at or before the feature timestamp, at most one hour old; missing votes or no votes use neutral 0.5. No future observations are used.
- Features, Steam: the same without like ratio; genre is the first Steam genre (one-hot).
- Trailing means and metadata features use only information available before the evaluated row. Metadata uses an exported version observed before the row, or unknown/neutral features if none exists; do not apply today's genre to historical evaluation rows. Evaluate a model fitted on the preceding six days against the last completed day, then refit the serving model on all seven days. Fit genre encoding on training rows only. Report RMSE/R2 in player-count space with sample counts; show evaluation unavailable before enough history exists.
- Cold start: a game is scored only with at least 24 h of history. Genre levels unseen in training are encoded as all zeros.
- Coefficients and preprocessing are stored together in `model_coeffs`; the speed job scores with the latest row of its platform.

## Definitions

- **Trend score** = `actual_players / max(1, expm1(predicted_log1p_players))`; the target is already in player units. Above/below 1 means above/below expected activity, not necessarily rising/falling. Growth supplies direction.
- **Projection** = the latest valid Chronos-2 median curve (see contract below). The +3h column is its final step, with forecast origin and freshness displayed.
- **Forecast evaluation** = +3h MAE of `forecast_curve[12]` against exactly the same observed targets for Chronos-2, persistence, yesterday and linear baselines; show sample count and per-platform results. Evaluate interval coverage separately. Intermediate steps are stored for charts; the required evaluation is the final +3h step.
- **Saturation** = number of matched Roblox clones and their total players.


## Chronos-2 projection contract

- Primary forecaster: pretrained `amazon/chronos-2`, zero-shot player-history inference only; no fine-tuning or covariates initially. Pin the model revision and package versions at implementation, cache weights on a persistent volume and record the revision in every prediction row. Sources: [Amazon model card](https://huggingface.co/amazon/chronos-2), [official inference API](https://github.com/amazon-science/chronos-forecasting/blob/main/src/chronos/chronos2/pipeline.py).
- Ownership: the speed-job Python driver loads one CPU model instance outside Spark executors. After publishing current metrics, schedule a bounded background inference task every 15 minutes, reading committed history without blocking the Spark callback; never load a model per game, partition or dashboard request. This adds no Compose service. Only one forecast run may be active; skip overlapping scheduled origins and log the skip rather than building a backlog. Forecast failure must not roll back successful metric ingestion; log it and keep earlier forecasts visibly stale.
- At origin `t` (a UTC 15-minute boundary), build up to seven days of 15-minute player history (672 points), using only observations at or before `t`. Each grid value is the latest actual observation in `(grid_time - 15m, grid_time]`. Do not invent five-minute points for 15-minute feeds. Require a contiguous usable latest 24 hours (96 points); truncate context after longer earlier gaps. Fill at most one consecutive missing grid point from the preceding value, flag it as imputed, and never use a future value. Require a real observation in the latest interval; otherwise skip and show the reason.
- Predict 12 future 15-minute steps. Store quantiles 0.1, 0.5 and 0.9; clamp negative outputs to zero, retain ordered quantiles and round only for display. The chart shows the median and the 10th–90th percentile band; the band is a model interval, not guaranteed coverage. Do not mix steps from different origins or model revisions.
- Publish a complete 12-step curve as one row in one transaction. Read the most recent completed origin not later than now. After 30 minutes without a fresh curve mark it stale; after three hours or when no future step remains show projection unavailable. Missing forecasts never become zero or a silently substituted linear forecast. Eligibility and skipped-game counts are visible; all eligible tracked games are the intended scope.
- Freeze baselines at issuance using only data available at origin: persistence = latest actual count; yesterday = real observation closest to `target_ts - 24h` within 10 minutes; linear = nonnegative extrapolation from the least-squares fit to actual observations over the last three hours (at least four observations). Linear is an evaluation baseline only.
- The hourly batch evaluator fills the +3h target from real archived observations closest to target time within 10 minutes, earlier observation winning ties. Do not score imputed targets. Missing targets remain pending until the archive covers the tolerance window; then they are marked unavailable in reporting. Actuals and baselines are shared across comparisons; forecasts are never recalculated during evaluation.
- Before claiming the deployment size: benchmark CPU latency and peak memory with 50 games, then the full eligible set, seven-day context and 12 steps. The full cycle must fit the 15-minute refresh interval with resource headroom alongside Spark. The previous 6 GB estimate is not a verified budget for Chronos-2. Record hardware, dependency/model revisions, batch size, elapsed time and peak memory. If it fails, revise the documented resource allocation; do not silently shrink coverage or enable paid cloud services.

## Shared metric rules

- Growth at 1h, 3h and 24h compares actual player observations, with a maximum 15-minute lookup tolerance around the historical reference. Percentage growth is unavailable when its denominator is zero or either observation is missing; latest player count zero remains valid.
- `new_visits` is the nonnegative difference between ordered cumulative visit observations. If a counter decreases, treat it as a reset and leave the delta unavailable; do not interpret it as negative visits. Do not sum cumulative counters.

## Retention and disk sizing

- Keep prediction curves and their real evaluation targets for 14 days; show health metrics for that retained window. At the maximum 1,800 games and 96 runs/day this is 172,800 curve rows/day, rather than 2,073,600 step rows/day. Arrays contain 12 median and interval values each. Actual eligible coverage is reported, not assumed.
- The report's Parquet estimate covers source events only. PostgreSQL rows/indexes, forecast curves, metadata exports, Kafka retention, checkpoints and model weights require additional disk space; measure them during the full-stack benchmark. Do not present the master-data estimate as total project storage.
