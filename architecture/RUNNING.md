# Running GameTrend Radar (developer guide)

How to set up, start and test the project **in its current state of implementation** (kept current by [AGENTS.md](../AGENTS.md) rule 10). If something here does not work, fix the file or report it.

## What works now

| area | can be run? | how |
|---|---|---|
| API investigation | yes | `python api_test/api_test.py` (standard library only, calls the real APIs, writes `api_test/responses/`) |
| Kafka, MongoDB | yes | see [Infrastructure](#infrastructure) |
| Spark image (`speed`, `batch` services) | image and `spark` tests yes, jobs not yet | see [Spark](#spark) |
| mock API | yes | see [Running each area](#running-each-area) (no Docker image yet) |
| other areas | not yet | see the [index](README.md) |

## Prerequisites

- Python 3.11
- [uv](https://docs.astral.sh/uv/) (Python dependency and environment manager)
- Docker Desktop with Docker Compose v2 or newer, tested with 5.6.1 (for Kafka, Spark, MongoDB), with about 8 GB of memory for Docker
- No Java on this computer: Spark and the `spark`-marked tests run in the Spark container

## Setup

```
uv sync                       # create .venv and install dependencies (without pyspark, which only the Spark image installs)
cp .env.example .env          # local configuration (never commit .env)
```

`.env` is not loaded automatically: pass it to the command, e.g. `uv run --env-file .env python -m gametrend.mockapi serve`.

## Infrastructure

`docker-compose.yml` is owned by [infra](infra/README.md). A plain `up` starts Kafka and MongoDB; the Spark services need the `spark` profile (see [Spark](#spark)).

Start (bash / zsh):

```
docker compose up -d && docker compose wait kafka-init mongo-init    # exit code 0 = ready
```

Start (PowerShell 5.1, which has no `&&`): run the two lines one after the other and check that the second ends with exit code 0.

```
docker compose up -d
docker compose wait kafka-init mongo-init
```

Do not replace this with `up -d --wait`: it does not wait for the `*-init` services, and `wait` without a preceding `up` returns 0 for an init that never started.

Stop:

```
docker compose --profile spark down                 # stop everything, keep the data
docker volume rm gametrend_kafka-data               # after down: reset only Kafka (likewise gametrend_mongo-data, ...)
docker compose --profile spark down -v              # stop and DELETE ALL data: Kafka, MongoDB, checkpoints and the Parquet master dataset
```

| service | address | purpose |
|---|---|---|
| `kafka` | `127.0.0.1:9094` from this computer, `kafka:9092` from other containers | the broker |
| `kafka-init` | none | creates the topics, then exits; `Exited (0)` is its normal state |
| `mongodb` | `mongodb://127.0.0.1:27017/?directConnection=true` from this computer (`MONGO_URI` in `.env.example`), `mongodb://mongodb:27017/?replicaSet=rs0` from other containers | MongoDB, replica set `rs0`, database `gametrend` |
| `mongo-init` | none | initiates `rs0` and creates the indexes, then exits; `Exited (0)` is its normal state |
| `speed` | Spark UI `http://127.0.0.1:4040` | speed job (profile `spark`) |
| `batch` | Spark UI `http://127.0.0.1:4041` | batch jobs (profile `spark`) |

Check that the topics match the [contract](contracts/kafka-topics.md#topic-settings):

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic roblox.snapshots
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic steam.players
```

Check MongoDB: the first command prints `PRIMARY`; the log of `mongo-init` lists the indexes of every collection, to compare with the [contract](contracts/mongodb-collections.md).

```
docker compose exec mongodb mongosh --quiet --eval "rs.status().members[0].stateStr"
docker compose logs mongo-init
```

### Spark

```
docker compose --profile spark build      # build gametrend-spark:dev; again after a change of uv.lock or infra/spark/
docker compose --profile spark up -d      # start speed and batch (fails until their jobs exist)
docker stats --no-stream                  # memory per service, against the limits in docker-compose.yml
```

A code change in `src/` needs only a restart of the service (`docker compose restart speed`), not a rebuild.

Check that the Kafka connector is in the image and needs no network. The expected error is about the missing `kafka.bootstrap.servers` option; `Failed to find data source: kafka` means the connector is missing.

```
docker run --rm --network none gametrend-spark:dev python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().read.format('kafka').load()"
```

Copy the Parquet master dataset to `export/` (ignored by git), e.g. for the report. Works in bash and PowerShell:

```
docker run --rm -v gametrend_master-data:/data/master:ro -v "${PWD}/export:/out" alpine cp -r /data/master /out
```

## Running each area

| area | command | needs |
|---|---|---|
| ingestion | TBD | Kafka, MongoDB (`tracked_games`) |
| speed | TBD | Kafka, MongoDB, Spark |
| batch | TBD | Kafka, MongoDB, Spark, `/data/master` volume |
| matching | TBD | MongoDB, network (Roblox search API) |
| ml | TBD | MongoDB, Spark |
| dashboard | TBD | MongoDB |
| demo | TBD | TBD |
| mockapi | `uv run python -m gametrend.mockapi serve --data-dir <collector data dir>` | a collector archive |
| mockapi reference client | `uv run python -m gametrend.mockapi check --base-url http://127.0.0.1:8080 --duration 300 --data-dir <collector data dir>` | a running mock |

`serve` options (each also an environment variable, see `.env.example`): `--port` (8080), `--a0` (replay start, default the archive start), `--start` (wall-clock instant of `--a0`, default now; pass the same pair to continue after a restart), `--speed` (1), `--errors replay|off`, `--inject <p>`. `check` without `--data-dir` only measures gaps and leads; with it, it also compares every answer with the archive. Behaviour: [mockapi](mockapi/README.md#using-the-mock-from-a-producer).

## Running tests

Rules for tests (no network, no Docker, fixtures, markers) are in [AGENTS.md § Testing](../AGENTS.md#testing).

```
uv run pytest                          # all tests
uv run pytest tests/<area>             # one area
uv run pytest -m "not spark"           # skip tests that start a local SparkSession
```

`uv run pytest` alone also collects the `spark`-marked tests, which fail on this computer without Java: use `-m "not spark"` here and run those tests in the Spark container (it uses the current `src/` and `tests/`; build the image first, see [Spark](#spark)):

```
docker compose run --rm --no-deps speed pytest -m spark
```

Exit code 5 means that no `spark` tests exist yet.

## Troubleshooting

| what you see | cause | what to do |
|---|---|---|
| a client in a container hangs, then `TimeoutException`; log: `Connection to node 1 (/127.0.0.1:9094)` | it used the address meant for this computer, and the broker sent it `127.0.0.1:9094` | use `kafka:9092` in containers |
| `Connect to ipv6#[::1]:9094 failed`, in some runs, though the records arrive | the client on this computer uses `localhost`, which can resolve to IPv6, where the port is not published | use `127.0.0.1:9094` |
| `produce()` raises nothing, the delivery callback gets `UNKNOWN_TOPIC_OR_PART` after 30 s | the topic does not exist (a typo, or `kafka-init` has not finished) | fix the name, or wait for `kafka-init` |
| `kafka-init` exits with 1: `exists with a partition count other than` | a topic was created by hand or the contract changed; Kafka cannot lower the partition count | `docker compose down`, then `docker volume rm gametrend_kafka-data` deletes all Kafka data; the next start recreates the topics |
| `kafka` exits with 1: `Invalid cluster.id` | `CLUSTER_ID` differs from the one stored in the volume | restore the old value, or delete `gametrend_kafka-data` as above |
| `mongo-init` exits with 1: `IndexOptionsConflict` or `IndexKeySpecsConflict` | an index was created by hand or the contract changed | drop that index with `mongosh`, or `docker compose down`, then `docker volume rm gametrend_mongo-data` |
| a client on this computer: `ServerSelectionTimeoutError` mentioning `mongodb:27017` | it connected with `replicaSet=rs0` and was sent the member name, which resolves only inside Docker | use `MONGO_URI` from `.env.example` (`directConnection=true`) |
| `Failed to find data source: kafka` in a Spark container | the image was built without the connector or is outdated | `docker compose --profile spark build` |
| the Spark image build fails at `uv sync --frozen` | `uv.lock` does not match `pyproject.toml` | `uv lock`, then build again |
| `ports are not available` for 9094 | another program uses the port | stop that program |
| `WARN ... STALE_BROKER_EPOCH`, once during startup | the broker registers several times during startup | nothing, it is unfenced milliseconds later |
| `WARNING: Due to limitations in metric names` when creating a topic | the topic name has a `.` | nothing, it concerns only names that differ by `.` and `_` |
