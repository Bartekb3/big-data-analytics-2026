# Demo data

## Purpose

Make the data visibly change in real time during a few-minute live presentation of the app. Real API data changes too slowly (5-15 minute polling, trends over hours), so the demo needs an accelerated or simulated data source.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| approach | planned | to be designed by the owner (see Design) |

## Inputs

TBD by the owner.

## Outputs

TBD by the owner. Events published to Kafka must follow [kafka-topics.md](../contracts/kafka-topics.md) exactly, so the other areas need no demo-specific code.

## Design / rules

To be designed and written here by the owner before implementation. Known constraints:

- Other areas must not need demo-specific branches; they rely on AGENTS.md rule 8 (no wall clock in processing logic) so that time can be accelerated.
- Demo data must not pollute the real dataset (MongoDB, Kafka, Parquet).
- The dashboard refresh interval must be short enough for changes to be visible (coordinate with [dashboard](../dashboard/README.md)).

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/demo/` | not implemented yet |

## Tests

Commands: [RUNNING.md](../RUNNING.md#running-tests). Coverage: TBD.

## Open questions

- Approach (e.g. accelerated replay of recorded data, scripted scenarios, synthetic generator) and how it is isolated from real data. Owner A.
- Which components run during the demo (speed only, or also batch and ml). Owner A with B, C, D.
