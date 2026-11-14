# Data conventions

**Producers and consumers:** every area.

Rules that apply to every contract (Kafka, Parquet, MongoDB). A contract file may tighten them, never relax them.

## Identifiers

- A game is identified by `(platform, id)`.
- `platform` is the string `'roblox'` or `'steam'`.
- `id` is the Roblox `universeId` or the Steam `appid`; a 64-bit integer (BSON `Long` in MongoDB).

## Time

- All timestamps are UTC.
- In Kafka JSON: ISO-8601 strings with a `Z` suffix (e.g. `2026-10-16T17:32:59.826Z`). Date-only values (Steam release date) are `YYYY-MM-DD`.
- In MongoDB: BSON `Date`, never strings. Date-only values are stored as `00:00 UTC` of that day.
- `ts` in an event is the producer's **observation time**, kept unchanged across retries and replays.
- Processing logic takes time from event timestamps or an injected clock, never from the wall clock (AGENTS.md rule 8).
- **"Now" is data time:** where a job needs "now" (retention cutoff, history reads, evaluation windows, health pages), it uses the latest observation time it can see in its own input, e.g. the latest event `ts` in Parquet for batch, the latest `observed_at` in `game_metrics` for history reads. Never the wall clock and never the mock's replay clock, so the same code works on the mock, on the live APIs and in an accelerated replay. Logic receives "now" as a parameter.
- This assumes no event carries a `ts` later than the moment it was fetched: ingestion rejects such events (the same criterion as the mock's check, `ts <= X-Mock-Time`), because one future `ts` would move "now" and with it every cutoff.

## Missing values

- A missing or unavailable value is an **absent field**. Never write `0`, `""`, `null` placeholders or substitute values for missing data.
- `0` is a valid observation (e.g. zero players) and must be kept distinct from "missing".
- Readers show missing values as unavailable, never as zero.

## Keys and writes

- Every MongoDB collection has a unique compound index on its business key; the default `_id` is not used as a business key.
- Every MongoDB write is an upsert on that key.
- Writers set only the fields they own; a write never overwrites a field it does not provide.

## Changing a contract

See [AGENTS.md § Workflow](../../AGENTS.md#workflow).
