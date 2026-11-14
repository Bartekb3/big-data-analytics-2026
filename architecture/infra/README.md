# Infrastructure

## Purpose

Everything needed to run the system locally with `docker compose up`: Kafka, Spark, MongoDB, service containers, volumes and configuration. Not responsible for application logic.

## Owner

Package A, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| `docker-compose.yml` | in-progress | Kafka, MongoDB and the Spark services; the speed and batch jobs themselves do not exist yet |
| Kafka (topics, retention) | in-progress | |
| MongoDB replica set + indexes | implemented | |
| Spark (shared image, local mode) | implemented | |
| volumes (`/data/master`, checkpoints) | implemented | |
| `.env.example` / configuration | implemented | |

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
- Kafka and MongoDB listen on localhost only and have no authentication.

### MongoDB

- MongoDB 7 runs as a single-node replica set `rs0` (required for the multi-document retention transaction). The member is named `mongodb:27017`: containers connect with `replicaSet=rs0`, processes on this computer with `directConnection=true`, because that name does not resolve there. The addresses are in `docker-compose.yml` (containers) and `.env.example` (`MONGO_URI`, this computer).
- Data and the replica set configuration live in the named volume `gametrend_mongo-data`.
- The `mongodb` healthcheck only checks that `mongod` answers. `mongo-init` waits for it, initiates `rs0` if it is not initiated yet, waits for PRIMARY (max 60 s), creates every index exactly as listed in the [MongoDB contract](../contracts/mongodb-collections.md), then exits. Applications create no indexes.
- `mongo-init` runs on every `up -d`, like `kafka-init`. It keeps the data and identical indexes, and fails if an index exists with other options than in the contract (`IndexOptionsConflict`).
- A service that uses MongoDB depends on `mongo-init` with `service_completed_successfully` (and on `kafka-init` the same way if it uses Kafka). `docker compose restart` does not rerun `mongo-init`; the node becomes PRIMARY again by itself within seconds.

### Spark

- Local mode (`local[*]`), no cluster: each job (`speed`, `batch`) is its own container with driver and executors in one JVM.
- One shared image `gametrend-spark:dev` (`infra/spark/Dockerfile`): Python 3.11 and Java 17 on Debian bookworm, the locked dependencies from `pyproject.toml` / `uv.lock` (including the `spark` extra and the dev group, so pytest), and the Spark Kafka connector.
- The connector has the version of the installed pyspark, is downloaded with its dependencies when the image is built and placed in pyspark's jars directory (skipping libraries pyspark already ships). Jobs start without network and without `spark.jars.packages`.
- No MongoDB Spark connector: jobs write and read MongoDB with pymongo.
- The image holds only dependencies. `src/`, `tests/` and `pyproject.toml` are bind-mounted read-only and found through `PYTHONPATH`, so a code change needs a restart, and only a change of `uv.lock` or `infra/spark/` needs a rebuild.
- Spark defaults (UTC session time zone, shuffle partitions) are in `infra/spark/spark-defaults.conf`, not in job code.
- `speed` and `batch` are in the compose profile `spark`: a plain `up -d` starts only Kafka and MongoDB.
- Tests marked `spark` run in this image, not on the host (no Java needed on the host).
- Containers do not load `.env`; their addresses and paths are set in `docker-compose.yml`.

### Volumes

- Named volumes: `kafka-data`, `mongo-data`, `speed-checkpoint` (speed), `archive-checkpoint` (Parquet sink) and `master-data` (`/data/master`). Speed and the sink never share a checkpoint.
- Named volumes, not bind mounts into the repo: on Docker Desktop for Windows, bind mounts are slow with many small files and do not reliably keep the rename and locking behaviour that checkpoints and Parquet commits rely on. Parquet files are copied out on demand (command in [RUNNING.md](../RUNNING.md#infrastructure)).
- `docker compose down -v` deletes every volume, including the Parquet master dataset, which the contract says is never deleted. Remove a single volume instead when only one store must be reset.

### Memory

- Every service has a `mem_limit` and the JVMs have a fixed heap (`KAFKA_HEAP_OPTS`, `SPARK_DRIVER_MEMORY`, MongoDB's WiredTiger cache), all set in `docker-compose.yml`. Together they stay within about 6.5 GB, sized for Docker Desktop with 8 GB.

### Disk sizing

- Retention periods are defined in the contracts: [MongoDB](../contracts/mongodb-collections.md), [Kafka](../contracts/kafka-topics.md#topic-settings), [Parquet](../contracts/parquet-master.md#general).
- The proposal estimates about 0.5M source events/day, 5-8 MB/day of Parquet. MongoDB documents and indexes, checkpoints and Kafka retention come on top; measure total disk usage during the full-stack run and record it here.

## Code map

| path | what it contains |
|---|---|
| `docker-compose.yml` | the services: `kafka`, `kafka-init`, `mongodb`, `mongo-init`, `speed`, `batch` (profile `spark`), and the volumes |
| `infra/create-topics.sh` | creates the Kafka topics and applies their settings, run by `kafka-init` |
| `infra/init-mongo.js` | initiates `rs0`, waits for PRIMARY and creates the contract's indexes, run by `mongo-init` |
| `infra/spark/Dockerfile` | the shared Spark image |
| `infra/spark/fetch_kafka_jars.py` | image build step: puts the Kafka connector and its dependencies into pyspark's jars |
| `infra/spark/spark-defaults.conf` | Spark defaults for every job |
| `.dockerignore` | limits the build context to the files the Spark image needs |

## Tests

Manual, see [RUNNING.md](../RUNNING.md#infrastructure): the stack starts, the topics match the contract, `rs0` is PRIMARY with the contract's indexes, a second `up -d` keeps everything, and the Spark image loads the Kafka source without network. `fetch_kafka_jars.py` has no unit tests; building the image is its test (it fails when the connector is missing).

## Open questions

- Restart policy for long runs (`kafka` stays down after a crash or a Docker restart). Owner A.
