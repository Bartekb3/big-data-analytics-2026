# Infrastructure

## Purpose

Everything needed to run the system locally with `docker compose up`: Kafka, Spark, MongoDB, service containers, volumes and configuration. Not responsible for application logic.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| `docker-compose.yml` | planned | |
| Kafka (topics, retention) | planned | |
| MongoDB replica set + indexes | planned | |
| Spark | planned | |
| volumes (`/data/master`, checkpoints) | planned | |
| `.env.example` / configuration | planned | |

## Inputs

None.

## Outputs

Running services that implement the storage side of the contracts:

- Kafka topics from [kafka-topics.md](../contracts/kafka-topics.md), with the retention defined there.
- MongoDB database `gametrend` with the collections and indexes from [mongodb-collections.md](../contracts/mongodb-collections.md).
- The `/data/master` volume for the [Parquet master dataset](../contracts/parquet-master.md).

## Design / rules

- Local, single-machine prototype, not a highly available deployment.
- MongoDB 7 runs as a single-node replica set `rs0` (required for the multi-document retention transaction).
- Indexes are created at startup exactly as listed in the MongoDB contract.
- Speed and archive (Parquet sink) consumers get distinct persistent checkpoint volumes.

### Disk sizing

- Retention periods are defined in the contracts: [MongoDB](../contracts/mongodb-collections.md), [Kafka](../contracts/kafka-topics.md#general), [Parquet](../contracts/parquet-master.md#general).
- The proposal estimates about 0.5M source events/day, 5-8 MB/day of Parquet. MongoDB documents and indexes, checkpoints and Kafka retention come on top; measure total disk usage during the full-stack run and record it here.

## Code map

| path | what it contains |
|---|---|
| `infra/` | not implemented yet |
| `docker-compose.yml` | not created yet |

## Tests

Manual: the stack starts with `docker compose up` and the services are reachable (document the check in [RUNNING.md](../RUNNING.md)).

## Open questions

- Spark deployment (local mode inside service containers vs a standalone cluster). Owner A with B and C.
- Where indexes are created (init script vs application startup). Owner A.
