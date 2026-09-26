# Payments medallion lakehouse — Fabric-targeted, locally runnable

A metadata-driven medallion lakehouse over a synthetic UK card-payments estate: seven source feeds
land as files, are ingested append-only into bronze, conformed and quality-gated into silver with
SCD2 history, and served from a gold star schema. The transformation code is written as Microsoft
Fabric notebook code; it runs on a laptop.

> ### Status
>
> **What runs:** landing → bronze → silver, end-to-end, from a cold start, on PySpark 3.5 /
> Delta 3.2 — the same pairing as the Fabric Spark runtime. 69 tests pass locally, including
> run-it-twice idempotency, SCD2 interval invariants, and a reconciliation that ties every bronze
> row to a silver row, a quarantined row or a deduplicated one.
>
> **What is written but not yet built:** the gold warehouse layer, the Fabric T-SQL subset linter,
> CI, the semantic model and the `fabric/` deployment artefacts. See
> [Build state](#8-build-state) — this repo is mid-build and the roadmap is stated rather than
> implied.
>
> **What has never touched Microsoft Fabric:** all of it. I could not provision a tenant inside the
> project window — the Fabric trial requires a work or school account. So nothing here is a claim
> of live-tenant validation. The code is written *against* Fabric's shape and constraints behind a
> thin execution-context shim, and the plan is to make that claim checkable by a linter and a
> deployment runbook rather than by assertion. Where a file is authored against a Fabric format
> that has never round-tripped through a real workspace, it says so in its own header.

---

## 1. What this repo is

Seven feeds, each chosen because it forces a different mechanism to exist:

| Feed | Format | Arrival | Why it is here |
|---|---|---|---|
| `transactions` | JSONL | daily, 50k–2M rows | Volume; **a new column appears at month 10** → schema evolution |
| `accounts` | CSV | daily CDC (`_op` I/U/D) | The SCD2 driver — `risk_band` moves over time |
| `customers` | CSV | **monthly** full snapshot | SCD2 from snapshots rather than CDC; contains PII |
| `merchants` | Parquet | **monthly** full snapshot | Proves bronze is format-agnostic via config, not code |
| `disputes` | JSONL | **0–90 days after the transaction** | Late arrivals — the feed that breaks "process today's partition" |
| `fx_rates` | CSV | daily reference | Interior date gaps → forward-fill, and a batch-level DQ rule |
| `card_products` | CSV | static | The degenerate case the same notebook still has to handle |

The data is generated, not downloaded, and that is a design decision rather than a convenience:
a public dataset cannot be asked to produce a duplicate rate of 0.3%, a column that appears in
month ten, or a dispute that arrives sixty days late. Every defect is injected at a contracted rate
under a fixed seed, and every one has a test that asserts the pipeline caught it. The contract for
both the schemas and the dirt is [`docs/data-contracts.md`](docs/data-contracts.md), and
`tests/test_contracts.py` fails if the generator drifts from that page.

At `tiny` scale (the CI profile): 50k transactions over 120 days. At `demo`: 2M over 18 months.
`tiny` spans 120 days rather than 30 deliberately — a 30-day span produces only one monthly
master-data snapshot, leaving SCD2 nothing to close out, and truncates the 90-day dispute lag so
that no late arrival is actually late. A CI profile that cannot exercise the two hardest mechanisms
in the pipeline is not worth having.

---

## 2. Why it is built this way

**The problem this repo is trying to solve is portability of judgement, not portability of code.**
Anyone can write a Spark job. The question a Fabric-adopting team actually has is whether the person
writing it understands where Fabric differs from what they know, and has designed around those
differences rather than discovered them in production.

So the organising constraint is: **the transformation code must be Fabric notebook code, unmodified,
while still running on a laptop.** That is what `src/runtime/context.py` is for. Every notebook asks
it for `get_spark()`, `read_table(layer, name)`, `write_table(...)`, `table_ref(...)`, `sql_exec(...)`.
Locally those resolve to a configured `SparkSession` and Delta paths under `./_onelake/`; on Fabric
they resolve to the ambient `spark` and lakehouse table names. Nothing above the shim knows which
substrate it is on.

Three consequences of that constraint are worth naming, because they are the things a reviewer
should look for:

- **No notebook imports another notebook.** On Fabric a notebook can only `%run` another, not
  `import` it, so anything two notebooks share has to be library code attached to the Spark
  Environment. That is why `load_config` lives in `src/lib/config.py` and is re-exported by both
  `nb_01` and `nb_02` rather than defined in one and imported by the other. It looks like an odd
  indirection until you know the platform rule it protects.
- **Spark and Delta are pinned to 3.5 / 3.2** because that is the Fabric runtime pairing. Version
  parity with the deployment target is a choice, not a coincidence.
- **Gold is a Warehouse, not a lakehouse.** The lakehouse SQL endpoint on Fabric is read-only, so
  T-SQL DDL and multi-table transactional loads need a Warehouse — and a Warehouse has a
  deliberately narrower T-SQL surface than SQL Server: no `IDENTITY`, no sequences, no *enforced*
  keys. Surrogate keys are therefore generated with `ROW_NUMBER() OVER (...)` over the current max,
  and the data-quality layer — not a foreign key — *is* the integrity guarantee.

The second organising idea is that **an eighth feed should be a row in a table, not an edit to a
notebook.** `nb_01_bronze_ingest.py` and `nb_02_silver_transform.py` each handle all seven feeds
with no per-entity branch anywhere in either file. Every difference between `transactions` (JSONL,
incremental, schema-evolving) and `card_products` (CSV, static, overwritten) is a column in
`meta_source_config`. That claim is the one most worth attacking when reading the code: grep either
notebook for an entity name and you should only find it in a docstring.

---

## 3. How to run it

**Prerequisites.** A JDK (Temurin 17 is what this was built on), Docker only if you want the gold
layer later, and [`uv`](https://docs.astral.sh/uv/). Python 3.11 is pinned — PySpark 3.5 does not
support 3.12+, and `make setup` will create the right interpreter for you.

```bash
make all        # cold start: generate landing data, seed the control plane, run, test
```

Or step by step, which is more instructive:

```bash
make setup                  # uv venv (python 3.11) + pinned deps
make generate SCALE=tiny    # deterministic landing files under ./_onelake/files/landing/
make seed                   # the metadata control plane: config, DQ rules, watermarks
make run                    # the pipeline, driven entirely by meta_source_config
make run                    # run it again — this is the interesting one
make test                   # 69 tests
```

The second `make run` is the point. It should do almost nothing, and say so:

```
fx_rates      skipped   watermark 2026-09-22 is current
accounts      skipped   silver watermark 2026-09-22 is current
card_products succeeded 12 rows      # static feed: always overwrites, by design
disputes      succeeded 0 rows       # re-read its rolling 90-day window, wrote nothing
```

`disputes` is the one that earns its keep. It reprocesses a 90-day window on every run to absorb
late arrivals, so it genuinely re-reads and re-presents rows it has already loaded — and writes
zero, because the MERGE is guarded on a content hash. That is the difference between a pipeline that
is idempotent and one that has merely never been run twice.

Useful extras:

```bash
make idempotency   # run, run --force-reload, then assert bronze is byte-for-byte unchanged
make test-fast     # skip the end-to-end cases
make lint          # ruff, plus the Fabric T-SQL subset linter once gold exists
make reset-lake    # drop bronze/silver/meta/quarantine, keep the generated landing files
make clean         # remove the venv, the caches and the entire local lake
```

A first full run at `tiny` takes a few minutes, most of it Spark start-up. The whole lake lives in
`./_onelake/` and is git-ignored; nothing outside the repo directory is touched.

### What a cold run actually produces

```
14 steps, 124,745 rows, 0 failures

entity          bronze  →  silver
card_products       12       12
fx_rates           920      920
customers        2,000    2,000
merchants          500      500
accounts        10,507    6,996   versions   (SCD2: no-op changes do not create history)
transactions    50,158   49,708             (292 quarantined, 158 deduplicated)
disputes           256      256
```

And the data-quality gate fires on the way through, which is what it is for:

```
transactions: 10 rules, 0 fatal, 4 warned
  amount_minor.range              7/50,000   0.014%  negative amounts
  currency_code.enum_domain       6/50,000   0.012%  not ISO 4217
  decline_reason_code.expression 22/50,000   0.044%  inconsistent with status
  merchant_id.not_null          257/46,963   0.547%  conditional — ATM rows correctly excluded
```

That last rate has a different denominator from the others, and deliberately: ATM withdrawals
legitimately have no merchant, so the rule is scoped to `channel <> 'ATM'` and its rate is computed
over the rows it actually applies to. A rule whose rate is diluted by rows it was never asked about
is a rule that cannot be given a meaningful threshold.

---

## 4. Architecture, and how it maps to Fabric

```
                 LOCAL (runs today)                    FABRIC (deployment target)
Landing          ./_onelake/files/landing/         →   OneLake  Files/landing/
Bronze           ./_onelake/bronze/*   (Delta)     →   lh_bronze            Lakehouse
Silver           ./_onelake/silver/*   (Delta)     →   lh_silver            Lakehouse
Quarantine       ./_onelake/quarantine/*           →   lh_silver  q_* tables
Control plane    ./_onelake/meta/*     (Delta)     →   lh_meta              Lakehouse
Gold             SQL Server 2022 in Docker         →   wh_gold              Warehouse (T-SQL)
Orchestration    orchestration/run.py              →   pl_master            Data Factory pipeline
Semantic model   TMDL source + measures.dax        →   Direct Lake semantic model
Report           static HTML                       →   Power BI report
```

`orchestration/run.py` is a deliberate 1:1 mirror of the Fabric pipeline graph it stands in for —
`Lookup` over `meta_source_config` → `ForEach` → `Notebook` activity → stored procedure — with a
bounded thread pool where Fabric would have `ForEach` concurrency. It is not a generic runner that
happens to work; its shape is the artefact.

The local gold bridge (loading silver Delta into SQL Server) is the **only** step in this repo that
disappears entirely on Fabric, where a Warehouse stored procedure cross-database-queries the
lakehouse SQL endpoint directly.

---

## 5. The control plane

Five Delta tables, seeded by `nb_99_seed_metadata.py`, that between them contain every per-feed
decision:

| Table | Holds |
|---|---|
| `meta_source_config` | One row per feed: format, load type, merge keys, SCD type, partition column, DQ rule set, effective-from and operation columns, enabled flag |
| `meta_dq_rules` | One row per rule: type, parameters, scope condition, severity, failure threshold |
| `meta_watermark` | High-water mark per `(entity, layer)` — bronze and silver advance independently |
| `meta_run_log` | One row per step: rows read / written / quarantined / deduplicated, status, timing |
| `meta_dq_results` | One row per rule per batch: the verdict, the rate, and whether it breached |

Two of those deserve a note, because both are places where the obvious design is wrong.

**The watermark has a layer dimension.** Bronze and silver advance at different rates — silver may
be nine days behind bronze after a failed run — so one watermark per entity cannot represent the
state. The layer is a separate column rather than a suffix on the entity name, because composite
keys are never concatenated anywhere in this repo: a separator that can occur in the data is a
collision waiting to be someone's incident.

**A data-quality result records a verdict, not just a count.** `rows_failed` is a number;
`breached` is the decision. `warn`-severity rules escalate by *rate* against a threshold, which is
why row-level and batch-level rules are distinguished in the engine — a rate is meaningless for a
rule whose defect is a row that does not exist, like an FX date gap.

---

## 6. What each layer promises, and refuses

**Bronze is append-only, source-faithful, and free of business logic.** No casting, no renaming, no
filtering, no deduplication. CSV lands as all strings, because that is what CSV *is*. Typing it here
would mean a malformed date becomes a silent `NULL` in the one layer whose job is to be the
reproducible record of what arrived. It adds exactly four audit columns: `_batch_id` (the replay
unit), `_ingest_ts`, `_source_file`, and `_row_hash` (computed here, over the source bytes, so that
SCD2's change detection in silver cannot be fooled by a transformation).

Idempotency in bronze is two mechanisms answering two different questions. The **watermark** decides
what to read. **`_batch_id`** decides what a retry replaces — and it is derived from the window
being loaded rather than from the clock, so re-running the same window produces the same id, and the
write deletes that batch before reinserting it. Both are needed: the watermark alone makes the happy
path a no-op but cannot repair a run that died after writing half its rows.

**Silver is where every value is finally asserted to be what the contract says it is,** and the
order of operations is the design:

```
scope  →  type gate  →  cast  →  dedupe  →  DQ gate  →  write
```

Each of those orderings is forced, not preferred:

- **Type gate before cast**, because `cast` in Spark is silent and lossy: `cast('2026-13-45' as
  date)` is `NULL`, not an error. A naive silver notebook converts every malformed source value into
  a well-typed null and then reports a clean run. So each column is first checked for castability,
  the failures are quarantined *with the value that failed still intact*, and only then is the cast
  applied. The evidence is destroyed by the cast, so it has to be collected before it.
- **Cast before dedupe**, because "keep the newest row per key" must be a typed comparison. Lexical
  ordering of timestamp strings is right until a source changes its format.
- **Dedupe before the DQ gate**, because the `unique` rules are asserted *after* deduplication — and
  because the 90-day dispute window legitimately re-presents keys it has already seen.

Nothing is dropped. A row that fails is written to `q_<entity>` together with the id of the rule
that caught it, so "292 rows were quarantined" is always answerable with "these ones, for this
reason."

**SCD2** (`src/lib/scd2.py`) uses half-open intervals, a `9999-12-31` sentinel for the open version,
hash-based change detection so that a no-op update does not manufacture history, and `_op = 'D'`
handled as a logical close-out rather than a delete. It refuses out-of-order CDC rather than
silently inverting an interval, and it collapses two changes at the same instant into one version
rather than emitting a zero-width row that no point-in-time join could ever return.
`tests/test_scd2.py` states the boundary that produces honestly: the dimension preserves every
*observable* state, which is fewer than every state in the feed.

---

## 7. What the tests actually prove

`make test` — 69 tests. The ones that matter:

| Claim | Test |
|---|---|
| Running the pipeline twice changes nothing | `test_bronze.py`, `test_silver.py`, `make idempotency` |
| A forced reload rewrites the same bytes | `test_a_forced_silver_reload_rewrites_nothing` |
| Every bronze row is accounted for downstream | `test_every_bronze_row_is_accounted_for` |
| A malformed value is quarantined, not cast to null | `test_an_uncastable_value_is_quarantined_rather_than_cast_to_null` |
| Defects are quarantined, not merely counted | `test_defects_were_actually_quarantined_not_merely_counted` |
| A DQ failure genuinely fails a run | `test_dq.py` |
| A type contract cannot be weakened from config | `test_a_type_contract_cannot_be_smuggled_in_as_a_config_rule` |
| One `is_current` row per key, no overlaps, no zero-width versions | `test_scd2.py` |
| A transaction joins to the *then*-current dimension version | `test_a_transaction_joins_to_the_then_current_version` |
| Two sequential batches equal one combined batch | `test_incremental_load_matches_a_single_load` |
| A delete-only batch still closes its incumbent | `test_a_delete_only_batch_closes_the_incumbent` |
| Late-arriving disputes land without duplicating | `test_the_dispute_window_reprocesses_without_duplicating` |
| The month-10 new column is absorbed, not fatal | `test_schema_evolution` |
| The generator still matches the published contract | `test_contracts.py` |
| The same seed produces byte-identical data | `test_determinism.py` |

Two notes on how these are written, because they are the difference between a suite that checks the
work and one that agrees with it:

Reconciliation is asserted against the counts **recorded in `meta_run_log`**, not against counts
recomputed from the tables. Recomputing them would test that Spark can count; reading them back
tests that the pipeline reported the truth about what it did. Relatedly, the row count a MERGE
reports is read from the Delta commit's `operationMetrics`, not from the size of its input — a
replay presents every row and changes none, and a notebook that logged its input size would claim
49,708 rows written for a load that wrote nothing. A number that cannot say "nothing happened"
cannot be used to confirm that something did.

Several invariants are asserted against the *real* generated feed rather than a hand-built frame,
because the interesting cases are the ones a hand-built frame forgets to include: a key that changes
five times, a key deleted and then reactivated, a third of all rows changing nothing. A fixture
containing only the cases its author remembered is a fixture that agrees with the implementation by
construction.

---

## 8. Build state

Honest, because the alternative is worse. Three days were budgeted; this is the end of day two.

| Area | State |
|---|---|
| Execution-context shim, data contracts, deterministic generator | **Done**, tested |
| Control plane (5 metadata tables) | **Done**, seeded |
| Bronze ingest, idempotent, schema-evolving | **Done**, tested |
| Local orchestrator mirroring `pl_master` | **Done** |
| DQ rule engine, SCD2, type contracts | **Done**, tested |
| Silver transform, all seven feeds | **Done**, tested |
| Gold: star-schema DDL + load procedures | Designed ([`docs/gold-execution.md`](docs/gold-execution.md)); not written |
| `tools/fabric_tsql_lint.py` — Fabric T-SQL subset linter | Not written |
| CI (`.github/workflows/ci.yml`) | Not written — **the status box will say "verified in CI" only once it is** |
| Semantic model (TMDL + DAX), static dashboard | Not written |
| `fabric/` deployment artefacts + runbook | Not written; will be labelled UNVALIDATED in every file |
| `docs/architecture.md`, `design-decisions.md`, `databricks-to-fabric.md`, `cost-and-capacity.md` | Not written |

Beyond this build, the honest list of what a production version needs and this does not have:
live-tenant validation; streaming ingestion (Eventstream → Eventhouse) for authorisations; Purview
lineage and classification; Mirroring from a source Azure SQL rather than file extracts; Warehouse
dynamic data masking and row-level security (designed, unverifiable without a tenant); alerting;
and a retention and disaster-recovery story.

---

## 9. Repo map

```
docs/data-contracts.md        The frozen contract: seven schemas, and the injected defect rates.
                              Start here. Everything else answers to this page.
docs/gold-execution.md        Why gold runs where it runs, and the fallback if Docker fights back.

src/runtime/context.py        The shim. The keystone: local ↔ Fabric, one code path above it.
src/runtime/params.py         Fabric notebook parameter-cell resolution.

src/lib/config.py             meta_source_config access (shared because notebooks cannot import
                              notebooks on Fabric).
src/lib/contracts.py          Per-column type contracts and the castability gate.
src/lib/dq.py                 The rule engine: row rules, batch rules, severity, quarantine.
src/lib/scd2.py               Generic SCD2 merge. Half-open intervals, hash change detection.
src/lib/watermark.py          Per (entity, layer) high-water marks.
src/lib/run_log.py            Step-level run logging as a context manager.
src/lib/determinism.py        Seeded generation primitives.

src/notebooks/nb_00_*.py      Generate landing data. A feature, not fixture setup.
src/notebooks/nb_01_*.py      Bronze ingest — one notebook, seven feeds, no branches.
src/notebooks/nb_02_*.py      Silver transform — one notebook, seven feeds, no branches.
src/notebooks/nb_99_*.py      Seed the control plane.

orchestration/run.py          Local stand-in for the pl_master Fabric pipeline.
tests/                        69 tests. conftest.py builds an isolated lake per module.
```

If you are reviewing this and have ten minutes, read in this order: the header of
`src/notebooks/nb_02_silver_transform.py` (the five-step order and why each step cannot move),
`src/runtime/context.py` (the shim that makes the Fabric claim checkable), and
`tests/test_scd2.py` (what the SCD2 implementation refuses to promise).

---

## 10. Not in scope

Real-time Intelligence, ML and fraud scoring, Purview, Mirroring, User Data Functions, multi-region,
and a live Power BI report. Each is a deliberate cut rather than an oversight, and
`docs/architecture.md` will give each one a sentence under "what I would add next, and why."
