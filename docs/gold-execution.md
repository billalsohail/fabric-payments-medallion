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
accept `IDENTITY(1,1)`, enforced foreign keys and triggers — none of which Fabric Warehouse accepts,
though it does accept a bare `bigint IDENTITY`; see `docs/fabric-tsql-subset.md` — so
"it ran locally" is evidence about the logic and *no* evidence at all about portability. The linter
is what carries the portability claim, and it is the verification that always works, with or without
a database.

## Substrate: the SQL Server spike, and why it was abandoned

The plan allotted 15 timeboxed minutes to standing up `mcr.microsoft.com/mssql/server:2022-latest`
under `platform: linux/amd64` on Apple Silicon. Docker itself was fine (29.4.1, aarch64), but the
image pull made no measurable progress in ~16 minutes — no layer had landed on disk — and the
timebox exists precisely so this does not become the afternoon. So:

- **The procs themselves are executed**, statement by statement, transpiled to Spark SQL and run
  against the silver Delta tables. That is enough to prove the joins, the surrogate-key assignment,
  the point-in-time dimension lookups, the FX forward-fill and the aggregate all produce the numbers
  the reconciliation test demands.
- **The T-SQL procs remain the deliverable artefact**, and crucially they are the *only* copy of the
  gold logic. `src/lib/gold.py` restates none of it; it is a harness, not a second implementation to
  keep in sync, so there is no way for the local numbers to be right while the shipped T-SQL is
  wrong. The DDL is translated the same way, so the column names, order, types and nullability gold
  runs against are the ones in the deliverable `CREATE TABLE`s.
- **`docker-compose.yml` is kept** as the documented optional path. If the image ever pulls, the
  same procs can be executed verbatim without touching anything else. It is retained rather than
  deleted because the decision was availability, not design.

## The honest consequence

The T-SQL in `src/warehouse/` has **never been executed by a T-SQL engine**. The distinction matters
enough to state twice, because the first draft of this page said "never executed by any engine" and
that is no longer true: `src/lib/gold.py` reads each statement out of the `.sql` files, transpiles it
from `tsql` to `spark` with sqlglot, and runs it. So the procs are not merely reviewed artefacts —
their logic is executed, and `tests/test_gold_recon.py` reconciles the result against silver row by
row.

What that does not establish is anything about the dialect. sqlglot cheerfully transpiles constructs
Fabric Warehouse rejects, and Spark cheerfully runs them, so a green reconciliation is evidence about
arithmetic and joins and *no* evidence about portability. That claim belongs entirely to the linter.
The Spark harness proves the *logic*; the linter proves the *dialect*; nothing available off-tenant
proves the two together, and claiming otherwise would be the one thing that makes the rest of this
repo untrustworthy.

The translation also drops what Delta has no equivalent for — `BEGIN TRAN`, `TRY`/`CATCH`, `IF`
guards — and each of those omissions is enumerated in `src/lib/gold.py`'s module docstring with the
argument for why it is acceptable. The `IF` skip is the one that could silently change behaviour, so
it is checked rather than trusted: the harness refuses to run a proc that has DML inside an `IF`
block.

The narrow place this actually bites: T-SQL that is dialect-valid and logically correct can still
fail on a distributed MPP engine for reasons a parser cannot see — a query plan that spills, a
`MERGE` against a table distributed on the wrong column. Those are the questions to ask on day one
with tenant access, and they are the first items in `docs/fabric-deployment.md`'s runbook.
