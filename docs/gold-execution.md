# How the gold layer is executed, and how it is verified

Gold is a **Fabric Warehouse** (`wh_gold`), written in T-SQL. That decision is in
`docs/design-decisions.md` #1 and it does not change here. What this page records is the narrower
question of *what runs the T-SQL while there is no tenant*, because the answer affects what a reader
is entitled to believe about the SQL in `src/warehouse/`.

## Two different claims, kept apart

| Claim | What backs it |
|---|---|
| "This logic produces the right numbers" | Execution. The reconciliation test ties bronze → silver → gold. |
| "This T-SQL would run on Fabric Warehouse" | Static analysis. `tools/fabric_tsql_lint.py` parses every `.sql` file with `sqlglot` and rejects constructs outside the documented Fabric Warehouse surface area. |

Neither substitutes for the other, and conflating them is the trap. A local SQL Server will happily
accept `IDENTITY`, enforced foreign keys and triggers — none of which Fabric Warehouse supports — so
"it ran locally" is evidence about the logic and *no* evidence at all about portability. The linter
is what carries the portability claim, and it is the verification that always works, with or without
a database.

## Substrate: the SQL Server spike, and why it was abandoned

The plan allotted 15 timeboxed minutes to standing up `mcr.microsoft.com/mssql/server:2022-latest`
under `platform: linux/amd64` on Apple Silicon. Docker itself was fine (29.4.1, aarch64), but the
image pull made no measurable progress in ~16 minutes — no layer had landed on disk — and the
timebox exists precisely so this does not become the afternoon. So:

- **Gold logic is executed in Spark SQL** against the silver Delta tables, which is enough to prove
  the joins, the surrogate-key assignment, the FX forward-fill and the aggregate all produce the
  numbers the reconciliation test demands.
- **The T-SQL procs remain the deliverable artefact**, reviewed as code and validated statically by
  the linter. They are what would be deployed; the Spark SQL is a harness, not a second
  implementation to keep in sync.
- **`docker-compose.yml` is kept** as the documented optional path. If the image ever pulls, the
  same procs can be executed verbatim without touching anything else. It is retained rather than
  deleted because the decision was availability, not design.

## The honest consequence

The T-SQL in `src/warehouse/` has **never been executed by any engine**. It is parsed, subset-checked
and reviewed; it is not run. That is a real gap and it is stated here, in the README status box, and
in the interview walkthrough, rather than left for someone to discover. The Spark SQL harness proves
the *logic*; the linter proves the *dialect*; nothing available off-tenant proves the two together,
and claiming otherwise would be the one thing that makes the rest of this repo untrustworthy.

The narrow place this actually bites: T-SQL that is dialect-valid and logically correct can still
fail on a distributed MPP engine for reasons a parser cannot see — a query plan that spills, a
`MERGE` against a table distributed on the wrong column. Those are the questions to ask on day one
with tenant access, and they are the first items in `docs/fabric-deployment.md`'s runbook.
