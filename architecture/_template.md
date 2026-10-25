# <Area name>

> Copy this file to `architecture/<area>/README.md`. Keep the section order; write "none" or "TBD" instead of deleting a section.

## Purpose

What this area is responsible for, in two or three sentences. What it is explicitly NOT responsible for.

## Owner

Package X, see [owners](../README.md#owners).

## Status

| module | status | notes |
|---|---|---|
| `<module>` | planned / in-progress / implemented | |

## Inputs

Data this area reads. Link to the contract section, do not copy fields here.

- [`<topic / collection>`](../contracts/<file>.md#<anchor>) - what is read and when

## Outputs

Data this area writes. Link to the contract section, do not copy fields here.

- [`<topic / collection>`](../contracts/<file>.md#<anchor>) - what is written and when

## Design / rules

Internal behaviour: algorithms, ordering, error handling, configuration. Implementation details are free; describe what other people need to know to trust and operate this area.

## Code map

| path | what it contains |
|---|---|
| `src/gametrend/<area>/...` | |

## Tests

What the tests cover and which fixtures they use. Commands are in RUNNING.md; link there instead of copying them:

Commands: [RUNNING.md](../RUNNING.md#running-tests).

## Open questions

Undecided points, each with who decides.
