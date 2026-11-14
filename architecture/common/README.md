# Common

## Purpose

Shared code that several areas must compute identically: metric definitions (bucketing, growth, `new_visits`), the history read across 5-minute and hourly data, and the retention cutoff. Not a place for area-specific helpers.

## Owner

Package B, see [owners](../README.md#owners). Other areas consume it; changes in behaviour need review from the consuming owners.

## Status

| module | status | notes |
|---|---|---|
| metric functions (bucket, growth, `new_visits`) | in-progress | used by speed and batch |
| history read (`read_metrics`) | planned | used by dashboard (and ml if needed) |
| retention cutoff | in-progress | used by batch retention and history read |

## Inputs

- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics), [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly) - for history reads.

## Outputs

None (library code).

## Design / rules

- Metric functions implement the [metric definitions](../contracts/mongodb-collections.md#metric-definitions) exactly; speed and batch call the same functions on sequences of 5-minute buckets of one game, so live and hourly numbers agree.
- Metric functions take plain Python values (`int | None`, timezone-aware UTC `datetime`) and raise `ValueError` on NaN, `pd.NA`, numpy scalars or naive datetimes. Callers reading pandas or other loose sources convert explicitly with the normalisation helper first; a batch error must fail loudly, never write NaN.
- History reads go through one function, planned as `read_metrics(platform, id, from, to)`, choosing per UTC hour: the hour's 5-minute documents from `game_metrics` if it has any, otherwise its `game_metrics_hourly` document. This does not depend on the history read and the batch retention job computing the same cutoff.
- Retention cutoff: start of the UTC hour of "now" minus the [`game_metrics` retention](../contracts/mongodb-collections.md). "Now" is data time, passed in by the caller ([conventions § Time](../contracts/conventions.md#time)). The retention can be overridden (e.g. shorter for the demo) but never below 25 h, because speed reads `game_metrics` back 24 h + 20 min for `growth_24h`.
### How speed and batch call the metric functions

Both work on the observations of one game, in time order:

1. `latest_per_bucket(observations)`: one `Observation` per 5-minute bucket.
2. `visit_deltas(buckets, previous)`: `new_visits` per bucket. `previous` is the latest earlier bucket with `visits` (speed: from `game_metrics`; batch: `last_with_visits` of the previous hour). A caller that splits a sequence into parts chains them by passing `last_with_visits(previous part)`; the result equals one call on the whole sequence.
3. `hourly_new_visits(deltas of one hour)` (batch): the hourly value, absent when no delta is valid.
4. `growths(buckets, history)` (speed): `history` is every observation known for the game, already merged with `buckets` through `latest_per_bucket`.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/common/metrics.py` | `Observation`; `bucket_start`, `hour_start`; `latest_per_bucket`, `last_with_visits`; `visit_delta`, `visit_deltas`, `hourly_new_visits`; `growth`, `find_reference`, `growths`; constants `BUCKET`, `MAX_VISIT_GAP`, `GROWTH_TOLERANCE`, `GROWTH_HORIZONS` |
| `src/gametrend/common/normalize.py` | `to_python` (NaN / NaT / `pd.NA` -> None, numpy scalars -> int / float, `pd.Timestamp` -> UTC `datetime`), `to_observation`; recognises pandas and numpy without importing them |
| `src/gametrend/common/retention.py` | `retention_cutoff(now, retention)`; constants `GAME_METRICS_RETENTION`, `MIN_RETENTION` |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). `tests/common/`, no network, no Spark; the `normalize` tests need pandas (dev group).

- buckets: boundaries, other time zones, naive datetimes rejected; one observation per bucket, ties decided by values in any input order;
- `new_visits`: chains with and without `previous`, reset, zero delta, buckets without `visits`, the 1 h gap boundary (1 h - 1 ms, 1 h, 1 h + 1 ms), counters above 2^31, a reset inside one bucket, unsorted input and NaN / `pd.NA` rejected, a sequence computed in parts equals the whole sequence (with hourly sums);
- hourly sum: only missing deltas give missing, zero deltas give 0, NaN and negative deltas rejected;
- growth: zero and missing reference, current zero (-100), the 15 min tolerance boundary, ties, references without players skipped, references from history, from the micro-batch and from both merged;
- normalisation: pandas and numpy missing values and scalars, a nullable `Int64` frame;
- retention cutoff: full hour, mid hour, other retention, naive `now`, the 25 h minimum.

## Open questions

None yet.
