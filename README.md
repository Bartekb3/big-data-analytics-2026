# GameTrend Radar

Real-time trend analytics for Roblox and Steam games.

## What it does

Tracks player counts across ~2,000 Roblox games and ~500 Steam games every 5 minutes,
computes trend scores and short-term projections, automatically matches Steam hits to their
Roblox clones, and displays everything in a live Streamlit dashboard.

## Architecture

Lambda architecture running locally via `docker compose up`:

```
Roblox / Steam APIs
        │
        ▼
  Python producers  ──►  Apache Kafka  ──►  Spark Structured Streaming (speed layer)
                                       │
                                       └──►  Parquet master dataset  ──►  Spark batch jobs
                                                                               │
                                                                               ▼
                                                                          MongoDB  ──►  Streamlit dashboard
```

## Repo structure

```
architecture/   – design source of truth: index, data contracts, one doc per area, RUNNING.md
src/gametrend/  – code, one package per area
tests/          – unit tests, one folder per area
infra/          – infrastructure configuration (Kafka, MongoDB, ...)
docs/M1/        – Milestone 1 proposal (PDF + LaTeX source), frozen
api_test/       – API investigation scripts and raw JSON responses
```

Developers: start with [architecture/README.md](architecture/README.md); to run the project see
[architecture/RUNNING.md](architecture/RUNNING.md). Coding agents follow [AGENTS.md](AGENTS.md).

## Team

Bartłomiej Borycki · Michał Iwaniuk · Jan Cwalina · Miłosz Chodań  
Big Data Analytics 2026, Warsaw University of Technology