# Clone matching

## Purpose

Once a day, automatically find Roblox games that are possible counterparts ("clones") of new Steam hits and store them with a confidence score. Not responsible for choosing the Steam clone candidates (that is the tracked set in [batch](../batch/README.md)) or for displaying matches ([dashboard](../dashboard/README.md)).

## Owner

Package C, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| Roblox search + detail fetch | planned | |
| title / description scoring | planned | |
| `bv_matches` rebuild | planned | |
| evaluation on a labelled sample | planned | |

## Inputs

- [`tracked_games`](../contracts/mongodb-collections.md#tracked_games) - Steam games with `is_clone_candidate = true`.
- [`games`](../contracts/mongodb-collections.md#games) - Steam `name`, `description` (`short_description`), `released`.
- Roblox APIs (external): search (`omni-search`), `games.roblox.com/v1/games` for `description` and `created` of the hits.

## Outputs

- [`bv_matches`](../contracts/mongodb-collections.md#bv_matches).

## Design / rules

- For each Steam clone candidate, search its title in the Roblox search API and take the top 10-20 hits (about 40-80 queries/day).
- `description` and `created` of the hits are fetched from `games.roblox.com/v1/games` (they are empty or missing in search results).
- Titles are normalised: emoji, bracketed tags such as `[UPD 3]` and punctuation removed, lower case. A short stopword list (`simulator`, `legends`, ...) reduces the weight of generic words.
- Hard rule: Roblox `created` must be after the Steam release date.
- `confidence = 0.5 * title_score + 0.5 * desc_score`.
  - `title_score`: fuzzy token-set ratio of the normalised titles (`rapidfuzz`, 0..1).
  - `desc_score`: TF-IDF cosine similarity (`scikit-learn`, 0..1) between the Steam `short_description` and the Roblox `description`; vocabulary/IDF fitted on the descriptions of the current candidates and their search hits.
  - If either description is empty, `confidence = title_score`.
- Pairs with `confidence` above one fixed threshold (configuration) are stored, together with `title_score` and `desc_score`.
- After a successful run, matches that are no longer produced are deleted. If a Roblox or Steam API call fails, the run is aborted and the previous `bv_matches` stay unchanged.
- Evaluation: the team labels about 100 candidate pairs and reports precision and sample count on the whole labelled sample (no tuning split); rejected candidates are inspected for missed matches.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/matching/` | not implemented yet |

## Tests

```
uv run pytest tests/matching
```

Use `api_test/responses/roblox_search_response.json`, `roblox_games_response.json` and `steam_appdetails_response.json` as fixtures.

## Open questions

- Value of the confidence threshold (after labelling). Owner C.
- Where the labelled sample and evaluation results are stored. Owner C.
