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
- At the cap (~1,500): evict games with the oldest `last_seen` first; clone candidates are evicted last.

## Clone matching rules

- `confidence = w * title_score + (1 - w) * desc_score`, start with `w = 0.5`. `title_score` is the fuzzy token-set ratio of the normalised titles (0..1). `desc_score` is the TF-IDF cosine similarity (0..1) between the Steam `short_description` and the Roblox `description`; the TF-IDF vocabulary/IDF is fitted on the descriptions of the current candidates and their search hits. If either description is empty, `confidence = title_score`. No bonus for the Steam title appearing in the description.
- Threshold and `w` are tuned on the manual sample of ~100 pairs. `title_score` and `desc_score` are stored so the tuning can be redone without refetching.
- Hard rule: Roblox `created` must be after the Steam release date.
- After a successful daily recomputation, matches that are no longer produced are deleted. If a Roblox or Steam API call fails, the run is aborted and the previous `bv_matches` stay unchanged.

## Roblox event precedence and deduplication

- **Roles:** `game` events supply the player history (`players`, `visits`, `favorites`). `votes` events only enrich the same time bucket (`up`, `down`). `sort` events only drive discovery (register or refresh a row in `tracked_games`, update `last_seen`) and never write player counts.
- **Time bucket:** `ts` is floored to 5 minutes: `bucket = floor(ts / 5 min)`. One row per `(platform, id, grain, bucket)`.
- **Deduplication:** within a bucket the latest `game` event wins (by `ts`). Re-delivered Kafka messages therefore produce the same row.
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
  PRIMARY KEY (platform, id)
);

-- append-only history of Roblox updates; the producer inserts a row when games.updated changes
-- (metadata and this table can be re-fetched/rebuilt from the APIs, so no backup is planned)
CREATE TABLE game_updates (
  platform text NOT NULL, id bigint NOT NULL,
  updated timestamptz NOT NULL, seen_at timestamptz NOT NULL,
  PRIMARY KEY (platform, id, updated)
);

-- tracked set (cap ~1,500)
CREATE TABLE tracked_games (
  platform text NOT NULL, id bigint NOT NULL,
  last_seen timestamptz NOT NULL, is_clone_candidate boolean DEFAULT false,
  PRIMARY KEY (platform, id)
);

-- speed layer writes grain='5m'; batch writes grain='1h' and deletes 5m rows older than 14 days
CREATE TABLE game_metrics (
  platform text NOT NULL, id bigint NOT NULL,
  ts timestamptz NOT NULL, grain text NOT NULL,  -- '5m' | '1h'
  players int, peak_players int, visits bigint, new_visits bigint, favorites int,
  up int, down int,
  growth_1h real, growth_3h real,
  projected_3h real,                             -- speed layer only
  trend_score real,                              -- actual / expected
  PRIMARY KEY (platform, id, grain, ts)         -- ts is the 5-min (or hour) bucket start
);

-- Hourly aggregation (batch job, from the 5-min rows of the closed hour):
--   players = last value of the hour, peak_players = max, visits = last, favorites = last,
--   new_visits = sum, up/down = last, trend_score = last, growth_* = last.
-- The batch job inserts the hourly rows and deletes the 5-min rows older than the cutoff
-- (14 days) in ONE transaction, so readers never see a gap.
-- Readers use one view and never touch the grains directly:
CREATE VIEW game_metrics_view AS
  SELECT * FROM game_metrics WHERE grain = '5m' AND ts >= now() - interval '14 days'
  UNION ALL
  SELECT * FROM game_metrics WHERE grain = '1h' AND ts <  now() - interval '14 days';
-- Hourly rows are authoritative for the hours they cover; 5-min rows are provisional
-- and are replaced by (not corrected into) the hourly rows.

-- one row per forecast; the hourly batch job fills actual and baseline_yesterday
CREATE TABLE predictions (
  platform text NOT NULL, id bigint NOT NULL,
  ts timestamptz NOT NULL,                       -- forecast made at t
  target_ts timestamptz NOT NULL,                -- t + 3 h
  forecast real, baseline_now real,              -- baseline_now = players at t
  baseline_yesterday real,                       -- players at t + 3 h - 24 h
  actual real,                                   -- players at t + 3 h (nearest observation within 10 min)
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
CREATE TABLE ingest_stats (
  ts timestamptz NOT NULL, feed text NOT NULL,
  ok int, dropped int, errors int,
  PRIMARY KEY (feed, ts)
);
```

## Recovery and authority

- Kafka keeps messages for 24 h. A consumer or sink outage longer than that loses the missed data; this is accepted and shown as a gap in the health view (data age per feed).
- The Kafka-to-Parquet sink uses a checkpoint on a volume. Postgres writes are idempotent (primary keys plus upserts), so replays are harmless.
- Hourly batch rows are authoritative. The speed layer only provides provisional 5-min rows for the last 14 days.

## Expected players model

- One model per platform, trained hourly on the last 7 days of hourly rows of the tracked games of that platform; target `log(1 + players)` (zero players are valid).
- Features, Roblox: hour of day, day of week, `genre_l1` (one-hot), game age (days), trailing mean of `log(1 + players)` over the previous 7 days, like ratio `up / (up + down)`.
- Features, Steam: the same without like ratio; genre is the first Steam genre (one-hot).
- Trailing means use only observations before the evaluated one (no leakage in training or evaluation). The hold-out is the last day.
- Cold start: a game is scored only with at least 24 h of history. Genre levels unseen in training are encoded as all zeros.
- Coefficients and preprocessing are stored together in `model_coeffs`; the speed job scores with the latest row of its platform.

## Definitions

- **Trend score** = `actual players / expected players` (model trained on `log(1 + players)`; both values converted back from the log scale).
- **Projection** = least-squares slope fitted on the actual timestamps of the last 3 h (Roblox top games every 5 min, others and Steam every 15 min), at least 4 observations, extrapolated +3 h, clamped at 0. Fewer observations: no projection.
- **Forecast evaluation** = MAE of `forecast` against `actual` at `t + 3 h`, compared with `baseline_now` (players at `t`) and `baseline_yesterday` (players at `t + 3 h - 24 h`).
- **Saturation** = number of matched Roblox clones and their total players.
