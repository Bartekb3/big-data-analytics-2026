# AGENTS.md

GameTrend Radar: real-time trend analytics for Roblox and Steam games (Lambda architecture: producers -> Kafka -> Spark speed + batch layers -> MongoDB -> Streamlit). Four people build it, each with a GenAI coding agent. This file keeps all agents working on **one** system instead of four.

## Constitution

These rules are non-negotiable. If a task conflicts with one of them, stop and tell the user.

1. **Contracts are law.** Data that crosses an area boundary has exactly the format defined in `architecture/contracts/`. Never add, remove, rename or retype a field without changing the contract in the same PR and requesting review from every affected area owner.
2. **Read before you write.** Start every task at `architecture/README.md`, then read your area document and the contracts it links to. Never guess field names, types or semantics.
3. **Stay in your area.** Edit code only in the area(s) owned by your user. A change in someone else's area needs the user's explicit consent and a review from that area's owner.
4. **Docs follow verified code.** Update `architecture/` only after the user confirms the code works. Until then, list what will need updating. Never change a status to `implemented` yourself; the user does that.
5. **Never silently diverge.** Whenever code, area documentation and contracts disagree, tell the user, even if it is outside the current task.
6. **Logic is tested.** Every new piece of logic comes with unit tests in `tests/<area>/` in the same PR. Unit tests need no network and no Docker.
7. **Pure logic, thin I/O.** Keep computation (normalisation, growth, deduplication, scoring, ...) in plain functions, separate from Kafka, Spark, MongoDB and HTTP code, so it is unit-testable.
8. **No wall clock in logic.** Processing logic never calls `datetime.now()` / `time.time()`. Time comes from event timestamps or from an injected clock. This keeps the system replayable and accelerable for the demo.
9. **Missing is not zero.** A missing value is an absent field, never `0`, `""` or a substitute. See `architecture/contracts/conventions.md`.
10. **Keep RUNNING.md runnable.** Any verified change to how something is installed, started or tested updates `architecture/RUNNING.md`.

## Reading the architecture docs

`architecture/` is the single source of truth for design. `docs/M1/` is a frozen course deliverable: do not edit it and do not treat it as current.

```
architecture/
  README.md          index: table of contents, data-flow map, owners, statuses  <- ALWAYS START HERE
  RUNNING.md         how to set up, start and test the system in its CURRENT state
  _template.md       template for an area document
  contracts/         data formats on area boundaries (Kafka, Parquet, MongoDB, conventions)
  <area>/README.md   one document per area: purpose, inputs, outputs, design rules, code map, tests
```

Reading order:

1. `architecture/README.md` - find your area, its owner and status.
2. `architecture/<area>/README.md` - the area you are working in.
3. The contracts linked from that area's **Inputs** and **Outputs** sections, plus `contracts/conventions.md`.
4. Other areas only when the task actually touches them.

Do not read every file up front. Go from the index down.

Contracts describe **data** (fields, types, keys, semantics) at area boundaries. Function signatures and internal structure are not contracts; each owner is free to design them, and area documents describe them once they exist.

## Areas and ownership

Code area `<area>` lives in `src/gametrend/<area>/`, its tests in `tests/<area>/`, its docs in `architecture/<area>/README.md`. Infrastructure files live in `infra/` and the repo root (`docker-compose.yml`, `.env.example`).

Areas: `ingestion`, `speed`, `batch`, `matching`, `ml`, `dashboard`, `common`, `infra`, `demo`.

At the start of a task, find your user's area(s): compare `git config user.name` with the owners table in `architecture/README.md`. If it does not match, or the table still has placeholders, ask the user which area they own.

## Workflow

1. Read (see above). If the task is unclear against the docs, ask before coding.
2. Branch from `main`: `<area>/<short-description>` (e.g. `ingestion/roblox-game-poller`).
3. Code and unit tests together. Run the tests of your area.
4. If the code needs a contract change: edit the contract file in the same PR, describe the change in the PR description, and make sure the producers and consumers of that contract review it (`.github/CODEOWNERS` requests them automatically). Never code against a contract you changed silently.
5. Open a PR into `main`; another team member reviews it.

## Keeping the documentation true

Area documents and RUNNING.md describe the system **as it is**, contracts describe the agreed data formats, and each module carries a status:

| status | meaning |
|---|---|
| `planned` | designed, not started; the design is the target |
| `in-progress` | being built; the doc may lag the code |
| `implemented` | the user confirmed it works; the doc must match the code |

Rules for the agent:

- **At the end of every task that changes behaviour**, list which files in `architecture/` (area docs, contracts, RUNNING.md, index) will need updating, and remind the user to confirm when the code works.
- **After the user confirms the code works**, update the area doc (design, code map, tests), the index and RUNNING.md so they match the code. Leave the status change to `implemented` to the user, or apply it only when they ask.
- **When you notice docs and code disagree** while reading, say so explicitly and propose the fix; do not quietly follow either one.
- You may set a module to `in-progress` when you start work on it.
- Never rewrite another area's document beyond the parts your change touches.

## Testing

- Python 3.11, dependencies managed with `uv` from a single root `pyproject.toml`; tests run with `pytest` (see `architecture/RUNNING.md` for the exact commands).
- Tests for area `<area>` live in `tests/<area>/` and run with `uv run pytest tests/<area>`.
- Unit tests use no network and no Docker. Use recorded API payloads from `api_test/responses/` as fixtures instead of calling APIs.
- Tests that need a local `SparkSession` are marked `@pytest.mark.spark` so they can be skipped with `-m "not spark"`.
- Integration runs with `docker compose` are done manually; the user decides when something works.

## Do not

- Create a second source of truth (notes, duplicate schemas, copies of contracts in code comments). Link to `architecture/` instead.
- Commit secrets; use `.env` (ignored) and keep `.env.example` up to date.
