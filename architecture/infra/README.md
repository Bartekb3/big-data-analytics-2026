# Infrastructure

## Purpose

Everything needed to run the system locally with `docker compose up`: Kafka, Spark, MongoDB, service containers, volumes and configuration. Not responsible for application logic.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| `docker-compose.yml` | in-progress | Kafka only |
| Kafka (topics, retention) | in-progress | |
| MongoDB replica set + indexes | planned | |
| Spark | planned | |
| volumes (`/data/master`, checkpoints) | planned | |
| `.env.example` / configuration | planned | |

## Inputs

None.

## Outputs

Running services that implement the storage side of the contracts:

- Kafka topics from [kafka-topics.md](../contracts/kafka-topics.md#topic-settings), with the settings defined there.
- MongoDB database `gametrend` with the collections and indexes from [mongodb-collections.md](../contracts/mongodb-collections.md).
- The `/data/master` volume for the [Parquet master dataset](../contracts/parquet-master.md).

## Design / rules

- Local, single-machine prototype, not a highly available deployment.
- Kafka is one KRaft broker. Setting any `KAFKA_*` variable makes the image skip its default config, so `docker-compose.yml` sets all of them.
- Kafka data lives in the named volume `gametrend_kafka-data`: it survives `restart`, `kill` and `down`, and only `down -v` deletes it. `CLUSTER_ID` is stored there, so it must not change.
- `kafka-init` runs on every `up -d`, not on `restart`. It keeps existing topics and their data, applies the contract's settings to them, and fails if a topic has a different partition count, which Kafka cannot lower.
- A service that needs the topics uses `depends_on` on `kafka-init` with `service_completed_successfully`: Compose then starts it after `kafka-init` and not at all if `kafka-init` fails. `service_healthy` on `kafka` is not enough.
- Nothing restarts a stopped service: there is no restart policy.
- MongoDB 7 runs as a single-node replica set `rs0` (required for the multi-document retention transaction).
- Indexes are created at startup exactly as listed in the MongoDB contract.
- Speed and archive (Parquet sink) consumers get distinct persistent checkpoint volumes.

### Disk sizing

- Retention periods are defined in the contracts: [MongoDB](../contracts/mongodb-collections.md), [Kafka](../contracts/kafka-topics.md#topic-settings), [Parquet](../contracts/parquet-master.md#general).
- The proposal estimates about 0.5M source events/day, 5-8 MB/day of Parquet. MongoDB documents and indexes, checkpoints and Kafka retention come on top; measure total disk usage during the full-stack run and record it here.

## Code map

| path | what it contains |
|---|---|
| `docker-compose.yml` | the services (Kafka only so far) |
| `infra/create-topics.sh` | creates the Kafka topics and applies their settings, run by `kafka-init` |

## Tests

Manual: the stack starts and the topics match the contract, see [RUNNING.md](../RUNNING.md#infrastructure).

## Open questions

- Spark deployment (local mode inside service containers vs a standalone cluster). Owner A with B and C.
- Where indexes are created (init script vs application startup). Owner A.
- Restart policy for long runs (`kafka` stays down after a crash or a Docker restart). Owner A.
