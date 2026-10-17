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

## PostgreSQL tables

```sql
-- metadata, upserted by producers only when changed
CREATE TABLE games (
  platform  text NOT NULL,              -- 'roblox' | 'steam'
  id        bigint NOT NULL,
  name      text NOT NULL,
  genre_l1  text, genre_l2 text,        -- Roblox
  genres    text[],                     -- Steam
  created   timestamptz,                -- Roblox creation date
  released  date,                       -- Steam release date
  updated   timestamptz,                -- Roblox last update
  PRIMARY KEY (platform, id)
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
  players int, visits bigint, new_visits bigint, favorites int,
  up int, down int,
  growth_1h real, growth_3h real,
  projected_3h real,                             -- speed layer only
  trend_score real,                              -- actual / expected
  PRIMARY KEY (platform, id, grain, ts)
);

-- one row per forecast, actual filled in 3 h later
CREATE TABLE predictions (
  platform text NOT NULL, id bigint NOT NULL,
  ts timestamptz NOT NULL,                       -- forecast made at
  forecast real, baseline_now real, baseline_yesterday real,
  actual real,
  PRIMARY KEY (platform, id, ts)
);

-- batch outputs
CREATE TABLE bv_matches (
  steam_id bigint NOT NULL, roblox_id bigint NOT NULL,
  confidence real NOT NULL, matched_at date NOT NULL,
  PRIMARY KEY (steam_id, roblox_id)
);

CREATE TABLE model_coeffs (
  trained_at timestamptz PRIMARY KEY,
  coeffs jsonb NOT NULL,                         -- feature -> coefficient, plus intercept
  r2 real, rmse real
);

-- producer counters for the health view
CREATE TABLE ingest_stats (
  ts timestamptz NOT NULL, feed text NOT NULL,
  ok int, dropped int, errors int,
  PRIMARY KEY (feed, ts)
);
```

## Definitions

- **Trend score** = `actual players / expected players` (model trained on `log(players)`, score is `exp(log actual - log expected)`).
- **Projection** = least-squares slope over the last 3 h of 5-min points, extrapolated +3 h, clamped at 0.
- **Saturation** = number of matched Roblox clones and their total players.
