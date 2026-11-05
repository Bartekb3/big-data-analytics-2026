# Running GameTrend Radar (developer guide)

How to set up, start and test the project **in its current state of implementation** (kept current by [AGENTS.md](../AGENTS.md) rule 10). If something here does not work, fix the file or report it.

## What works now

| area | can be run? | how |
|---|---|---|
| API investigation | yes | `python api_test/api_test.py` (standard library only, calls the real APIs, writes `api_test/responses/`) |
| Kafka | yes | see [Infrastructure](#infrastructure) |
| mock API | yes | see [Running each area](#running-each-area) (no Docker image yet) |
| other areas | not yet | see the [index](README.md) |

## Prerequisites

- Python 3.11
- [uv](https://docs.astral.sh/uv/) (Python dependency and environment manager)
- Docker Desktop with Docker Compose v2 or newer, tested with 5.6.1 (for Kafka, Spark, MongoDB)
- Java 17 (needed by local PySpark for `spark`-marked tests) - TBD by the `infra` owner

## Setup

```
uv sync                       # create .venv and install dependencies
cp .env.example .env          # local configuration (never commit .env)
```

`.env` is not loaded automatically: pass it to the command, e.g. `uv run --env-file .env python -m gametrend.mockapi serve`.

## Infrastructure

Only Kafka exists so far; `docker-compose.yml` is owned by [infra](infra/README.md).

```
docker compose up -d && docker compose wait kafka-init    # start Kafka and create the topics; exit code 0 = ready
docker compose down                                       # stop, keep the data
docker compose down -v                                    # stop and DELETE all Kafka data
```

Keep the two start commands joined with `&&`: `up -d --wait` does not wait for `kafka-init`, and `wait` alone returns 0 for a `kafka-init` that never started.

| service | address | purpose |
|---|---|---|
| `kafka` | `127.0.0.1:9094` from this computer, `kafka:9092` from other containers | the broker |
| `kafka-init` | none | creates the topics, then exits; `Exited (0)` is its normal state |

Check that the topics match the [contract](contracts/kafka-topics.md#topic-settings):

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic roblox.snapshots
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic steam.players
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

## Troubleshooting

| what you see | cause | what to do |
|---|---|---|
| a client in a container hangs, then `TimeoutException`; log: `Connection to node 1 (/127.0.0.1:9094)` | it used the address meant for this computer, and the broker sent it `127.0.0.1:9094` | use `kafka:9092` in containers |
| `Connect to ipv6#[::1]:9094 failed`, in some runs, though the records arrive | the client on this computer uses `localhost`, which can resolve to IPv6, where the port is not published | use `127.0.0.1:9094` |
| `produce()` raises nothing, the delivery callback gets `UNKNOWN_TOPIC_OR_PART` after 30 s | the topic does not exist (a typo, or `kafka-init` has not finished) | fix the name, or wait for `kafka-init` |
| `kafka-init` exits with 1: `exists with a partition count other than` | a topic was created by hand or the contract changed; Kafka cannot lower the partition count | `docker compose down -v` deletes all Kafka data and recreates the topics |
| `kafka` exits with 1: `Invalid cluster.id` | `CLUSTER_ID` differs from the one stored in the volume | restore the old value, or `docker compose down -v` |
| `ports are not available` for 9094 | another program uses the port | stop that program |
| `WARN ... STALE_BROKER_EPOCH`, once during startup | the broker registers several times during startup | nothing, it is unfenced milliseconds later |
| `WARNING: Due to limitations in metric names` when creating a topic | the topic name has a `.` | nothing, it concerns only names that differ by `.` and `_` |
