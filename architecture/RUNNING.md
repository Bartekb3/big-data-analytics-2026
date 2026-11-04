# Running GameTrend Radar (developer guide)

How to set up, start and test the project **in its current state of implementation** (kept current by [AGENTS.md](../AGENTS.md) rule 10). If something here does not work, fix the file or report it.

## What works now

| area | can be run? | how |
|---|---|---|
| API investigation | yes | `python api_test/api_test.py` (standard library only, calls the real APIs, writes `api_test/responses/`) |
| mock API | yes | see [Running each area](#running-each-area) (no Docker image yet) |
| other areas | not yet | see the [index](README.md) |

## Prerequisites

- Python 3.11
- [uv](https://docs.astral.sh/uv/) (Python dependency and environment manager)
- Docker Desktop with Docker Compose v2 (for Kafka, Spark, MongoDB)
- Java 17 (needed by local PySpark for `spark`-marked tests) - TBD by the `infra` owner

## Setup

```
uv sync                       # create .venv and install dependencies
cp .env.example .env          # local configuration (never commit .env)
```

## Infrastructure

> Not available yet: `docker-compose.yml` is owned by `infra`. Document here which services exist, how to start them and on which ports.

```
docker compose up -d <services>
```

| service | port | purpose |
|---|---|---|
| TBD | | |

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

None yet.
