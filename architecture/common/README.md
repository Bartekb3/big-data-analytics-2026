# Common

## Purpose

Shared code that several areas must compute identically: metric definitions (bucketing, growth, `new_visits`), the history read across 5-minute and hourly data, and the retention cutoff. Not a place for area-specific helpers.

## Owner

Package B, see [owners](../README.md#owners). Other areas consume it; changes in behaviour need review from the consuming owners.

## Status

| module | status | notes |
|---|---|---|
| metric functions (bucket, growth, `new_visits`) | planned | used by speed and batch |
| history read (`read_metrics`) | planned | used by dashboard (and ml if needed) |
| retention cutoff | planned | used by batch retention and history read |

## Inputs

- [`game_metrics`](../contracts/mongodb-collections.md#game_metrics), [`game_metrics_hourly`](../contracts/mongodb-collections.md#game_metrics_hourly) - for history reads.

## Outputs

None (library code).

## Design / rules

- Metric functions implement the [metric definitions](../contracts/mongodb-collections.md#metric-definitions) exactly; speed and batch call the same functions, so live and hourly numbers agree.
- History reads go through one function, planned as `read_metrics(platform, id, from, to)`: 5-minute documents from `game_metrics` after the retention cutoff (plus any kept older 5-minute documents of hours without an aggregate), and `game_metrics_hourly` for older hours.
- The history read and the batch retention job use the SAME cutoff: start of the UTC hour of now minus the [`game_metrics` retention](../contracts/mongodb-collections.md), where "now" is passed in (no wall clock in logic).
- Function signatures are free for the owner to design; document them in the code map once implemented.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/common/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests).

Metric functions are pure and must be fully covered, including the missing-value and counter-reset cases.

## Open questions

None yet.
