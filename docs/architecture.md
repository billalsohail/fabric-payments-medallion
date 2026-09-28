# Architecture: the structure, the seams, and what I would add next

This page is the **structural** view. It deliberately does not repeat three things that are already
written down elsewhere, because a fact stated twice is a fact that can disagree with itself:

| Already written | Where |
|---|---|
| The layer map, local ↔ Fabric, table by table | [`README.md`](../README.md) §4 |
| What each layer promises and refuses, and the forced order of silver's five steps | [`README.md`](../README.md) §6 |
| The fourteen numbered trade-offs, each with the alternative it rejected | [`docs/design-decisions.md`](design-decisions.md) |
| The Fabric item inventory, the dev→prod pipeline, the first-two-hours runbook | [`docs/fabric-deployment.md`](fabric-deployment.md) |

What is left, and what this page is for: **which way the dependencies point, where the system is cut
and what each cut promises, who owns each fact, what changing one thing actually costs, and what I
would build next.** The last of those is a promise made in [`README.md`](../README.md) §10 and kept
in §6 below.

---

## 1. The dependency direction

Every import in this repo points one way. That is checkable rather than aspirational — the
parenthesised notes below are the actual module-level imports:

```
tools/                     stdlib + sqlglot only.  Imports NOTHING from src/.
  fabric_tsql_lint.py      ├─ sqlglot
  tmdl.py, extract_dax.py  └─ stdlib, and tmdl

                                         ▲  no edge here, in either direction
                                         │
dashboard/build_dashboard.py  ──────────────►  src.lib.gold, src.runtime.context, tools.tmdl
       (top of the graph: depends on everything, depended on by nothing)

orchestration/run.py       ──►  src.runtime.context          (module level)
                           ──►  src.notebooks.nb_01 / nb_02, src.lib.gold   (lazily, in-function)

src/notebooks/*.py         ──►  src.lib.*, src.runtime.*
       nb_01 ──► config, run_log, watermark
       nb_02 ──► config, contracts, dq, run_log, scd2, watermark
       nb_00 ──► determinism        (and nothing else from src/lib)

src/lib/*.py               ──►  src.runtime.context          (and contracts ──► dq, within lib)

src/runtime/context.py     ──►  stdlib + pyspark.  The bottom of the graph.
src/runtime/params.py      ──►  stdlib only.
```

Three properties of that shape are load-bearing, and each of them is the reason for a rule that
would otherwise look like fussiness.

**`src/runtime/` imports nothing from the project.** It is the substrate — `Layer`, `Env`,
`detect_env()`, `RuntimeConfig`, `landing_path()`, `table_ref()`, `read_table()`, `write_table()`.
If it imported anything above it, the shim would depend on the code it exists to make portable, and
"this is unmodified Fabric notebook code" would stop being checkable. `docs/design-decisions.md` #8
argues for the shim's existence; this is the constraint that keeps it honest.

**No notebook imports another notebook.** Each is a Fabric Notebook item, and a name plus a
parameter list is the whole of the interface a Notebook activity can offer, so each file exposes one
function the orchestrator calls by keyword and a `main()` that takes nothing:

| Module | Entry point | Fabric equivalent |
|---|---|---|
| `nb_00_generate_landing_data` | `main()` | Notebook, run by hand or on a schedule |
| `nb_01_bronze_ingest` | `ingest(entity, until_date, batch_id, run_id, force_reload)` | Notebook activity, five parameters |
| `nb_02_silver_transform` | `transform(entity, run_id, until_date, force_reload)` | Notebook activity, four parameters |
| `nb_03_table_maintenance` | `maintain(layers, tables, actions, vacuum_retain_hours, small_file_mib, dry_run, run_id)` | Notebook activity on its own schedule, **not** in `pl_master` |
| `nb_99_seed_metadata` | `main()` | Notebook, run once per environment |

`main()` takes no arguments on purpose. Parameters arrive through `src.runtime.params`, which reads
`sys.argv` and then the environment, so the same file behaves the same run as a notebook cell, as
`python -m`, and as a Notebook activity whose parameters Fabric injects — a `main(argv)` would give
the parameter surface argv's shape, and an activity has no argv to offer.

The two orchestrated signatures are also wider than the calls against them: `orchestration/run.py`
passes four of `ingest`'s five and two of `transform`'s four, and every parameter it leaves out has
a default. Those are there for a corrective re-run rather than for the pipeline — `batch_id` pins a
batch id instead of deriving it from the partition set, so a replacement load can land under the
identity of the batch it replaces, and `until_date` and `force_reload` reproduce a past window.
`tests/test_import_graph.py` reads both sides and fails if a keyword is passed that the notebook
does not declare, because on Fabric the two sides are separate items and nothing compares them until
the activity runs.

Anything two notebooks both need lives in `src/lib/` instead. That is not a style preference: on
Fabric, one notebook calling another means `mssparkutils.notebook.run`, a second Spark session and a
string-serialised return value. A shared module is `%run`-free and unit-testable, and the
orchestrator can call the same function the pipeline would call.

**`tools/` shares no code with what it verifies.** `fabric_tsql_lint.py` imports `sqlglot` and
nothing from `src/`; it reads the `.sql` files as text. This is what makes its verdict worth
anything. A linter that imported the gold harness could be satisfied by the same misunderstanding
twice — the harness deciding a construct is fine and the linter agreeing because it asked the
harness. Two independent readers of the same `.sql` file cannot collude.
[`docs/gold-execution.md`](gold-execution.md) makes the related point about the two claims
(the harness proves the logic, the linter proves the dialect); this is the structural reason the
second claim is not circular.

**The orchestrator's lazy imports are a real constraint, not a lint workaround.** `run.py` imports
`src.runtime.context` at module level and the notebooks *inside* the stage functions. Two reasons:
importing `src.lib.gold` builds nothing until `load()` is called, and a `--stage bronze` run should
not pay to import silver's dependencies or fail if silver is mid-rewrite. The `noqa: PLC0415`
comments mark the places where that is deliberate.

---

## 2. The six seams

A seam is a place where the system is deliberately cut, where the two sides are allowed to change
independently, and where the contract between them is written down. Most of what is interesting
about this repo is at a seam rather than inside a component.

### Seam 1 — landing → bronze: *the file is the contract, and its format is a config value*

**What crosses:** files, in three formats across seven feeds — JSON for transactions and disputes,
CSV for accounts, customers, FX rates and card products, Parquet for merchants.

**The contract:** the path shape `Files/landing/<entity>/…` and nothing else. Bronze does not know
what is in the file. `source_format` is a column in `meta_source_config`, the reader is selected
from it, and every column lands as whatever the reader produced — strings, for CSV.

**What may change freely:** the format of any feed, without touching a notebook. A source moving
from CSV to Parquet is one config row.

**What breaks if crossed:** typing at this seam. `README.md` §6 states why in full; the structural
version is that bronze's job is to be the reproducible record of what arrived, and a layer that
transforms cannot also be the evidence of what it received.

**One asymmetry worth naming:** `landing_path()` returns an absolute local path under `./_onelake/`
but a *relative* `Files/landing/<entity>` on Fabric, which resolves against the notebook's **default
lakehouse**. So on Fabric this seam's location is a deployment property of the notebook item, not a
value in the config — which is why [`docs/fabric-deployment.md`](fabric-deployment.md) §1 insists on
attaching `lh_bronze` as the default lakehouse and §5 treats the default-lakehouse deployment rule
as load-bearing.

### Seam 2 — bronze → silver: *`data-contracts.md` is the contract, and it is executable*

**What crosses:** a bronze Delta table, all columns as they landed, plus four audit columns.

**The contract:** [`docs/data-contracts.md`](data-contracts.md) for the seven feeds, and
`SILVER_TYPES` in [`src/lib/contracts.py`](../src/lib/contracts.py) for the types silver will
assert. The second is the executable form of the first, and `tests/` asserts the generator still
matches.

**What may change freely:** everything about *how* silver gets there. The five-step order in
`README.md` §6 is internal to silver; nothing upstream depends on it.

**What breaks if crossed:** the evidence. A cast is silent and lossy in Spark, so the type gate has
to run before the cast and quarantine the failing rows *with the offending value intact*. That is a
statement about ordering inside silver, but the reason it is possible at all is this seam: bronze
still holds the untyped original, so silver can be re-run against it after a contract is corrected.
A silver that typed on ingest would have destroyed the only copy.

### Seam 3 — silver → gold: *the engine boundary, and the only seam whose shape changes on Fabric*

This is the most interesting seam in the repo and the one a reviewer should look at first.

**What crosses:** rows, from Spark/Delta into T-SQL.

**The contract:** the `stg.*_silver` tables in
[`src/warehouse/ddl/06_staging.sql`](../src/warehouse/ddl/06_staging.sql). Ten procs read `stg.`,
never silver directly. Truncate-and-fill, one statement per table.

**Why a staging schema rather than reading silver straight:** three reasons, all in that file's
header — a snapshot that cannot shift under a ten-proc chain; **one** seam to re-point rather than
seven; and a failure that is diagnosable, because "the staging table is empty" and "the join is
wrong" are different symptoms.

**What changes on Fabric:** only the *fill*. Locally, `src/lib/gold.py` loads silver Delta into the
staging tables. On Fabric the same statement is a cross-database query against the lakehouse SQL
analytics endpoint — `INSERT INTO stg.dim_account_silver SELECT * FROM lh_silver.dbo.dim_account;` —
and the fill step disappears. The procs are byte-identical either way. This is the **only** step in
the repo that vanishes on Fabric, which is why `README.md` §4 says so and why
[`docs/fabric-deployment.md`](fabric-deployment.md) §3 refuses to put layers in separate workspaces:
a warehouse transaction may span warehouses **in the same workspace**, so splitting the layers
across workspaces would break this seam and nothing else would notice until step 6 of the runbook.

**What breaks if crossed:** portability, in both directions. A proc that read silver Delta directly
could not be the deliverable T-SQL; a harness that reimplemented the joins in PySpark would be a
second copy of the gold logic to keep in sync. `src/lib/gold.py` restates none of it — it interprets
the `.sql` files — so there is no way for the local numbers to be right while the shipped T-SQL is
wrong. That argument is [`docs/gold-execution.md`](gold-execution.md)'s; the seam is what makes it
available.

### Seam 4 — gold → semantic model: *stored means additive, computed means correct*

**What crosses:** ten warehouse tables, read by a Direct Lake semantic model. Every partition in
[`semantic-model/definition/`](../semantic-model/definition) is `mode: directLake`.

**The contract:** one rule, stated in `09_sp_load_agg_merchant_daily.sql` and enforced on the other
side by `tests/test_semantic_model.py` — **the warehouse stores counts and sums; the model computes
every rate.** No rate is a column. Money is stored as an integer in minor units
(`design-decisions.md` #14) and divided by 100 in the measure.

**Why the rule is at the seam and not inside either side:** a stored `approval_rate` per
merchant-day is *correct in the warehouse* and *wrong in the report*, because averaging it across a
month weights a 3-attempt day like a 30,000-attempt day. Neither side can catch that alone. The
warehouse looks right; the model looks right; only the seam is wrong. `DIVIDE(SUM(numerator),
SUM(denominator))` is the only form that survives aggregation, which is also why the additive flags
are `smallint` and not `bit` — `SUM(is_declined)` has to mean something.

**The one place the seam is knowingly leaky:** `agg_merchant_daily.distinct_account_count` is not
additive, and no contract can make it so. It is exposed as its own table rather than as a Power BI
user-defined aggregation precisely so that the substitution is a report author's explicit choice,
and the measure over it returns BLANK above day grain. The exact answer lives in a second measure
that goes to the detail fact. Both are kept, so the choice is visible.

### Seam 5 — control plane ↔ everything: *declared config is read-only at runtime*

**What crosses:** six Delta tables in `lh_meta`, and they split cleanly in two:

| | Tables | Written by | Read by |
|---|---|---|---|
| **Declared** | `meta_source_config`, `meta_dq_rules` | `nb_99_seed_metadata` only, `mode="overwrite"` | notebooks, orchestrator, DQ engine |
| **Observed** | `meta_run_log`, `meta_dq_results`, `meta_watermark`, `meta_maintenance_log` | the runtime, every run | `meta_watermark` feeds the next run; the rest are read by humans and tests |

**The contract:** nothing in `src/notebooks/nb_01`, `nb_02` or `src/lib/` ever writes a declared
table. That is what makes `nb_99` re-runnable as the source of truth and what makes a config change
reviewable as a diff. `nb_99`'s own header carries the declarative-vs-observed table per column.

**The asymmetry that is easy to get wrong:** `meta_watermark` is observed state that becomes an
input. It therefore has a **layer** dimension rather than one row per entity, because bronze and
silver advance at different rates and one watermark cannot represent "silver is nine days behind
bronze after a failed run". And the layer is a separate column rather than a suffix, because a
composite key concatenated with a separator that can occur in the data is a collision waiting to
happen.

### Seam 6 — the shim ↔ the notebooks: *the seam that makes the Fabric claim checkable*

**What crosses:** fourteen public names — and the widest importer, `nb_01_bronze_ingest`, uses eight
of them; `nb_02_silver_transform` uses five. Two enums (`Layer`, `Env`); one config object and its
reader (`RuntimeConfig`, `config()`); the session (`get_spark()`);
the location functions (`landing_path()`, `list_landing_partitions()`, `table_ref()`); the table
functions (`table_exists()`, `read_table()`, `write_table()`, `delta_table()`); plus `detect_env()`
and `reset_caches()`, which exist for the environment and for the tests respectively rather than for
the notebooks.

**The contract:** a notebook names a `Layer` and a table name. It never names a path, a lakehouse, a
prefix or an environment. `table_ref()` composes `f"{cfg.lakehouse_prefix}{layer.value}.{name}"`, so
the four lakehouses — `lh_bronze`, `lh_silver`, `lh_meta`, `lh_quarantine` — are named in exactly
one place and the prefix is one environment variable.

**What this buys, precisely:** the transformation code is unmodified Fabric notebook code, and that
is a claim about a diff rather than an intention. What it does *not* buy is any evidence that the
Fabric side of the shim is right — `detect_env()`'s Fabric branch has never run on Fabric. The shim
makes the claim *checkable*; it does not make it *checked*, and the difference is the whole of
[`docs/fabric-deployment.md`](fabric-deployment.md) §9.

---

## 3. One fact, one home

The rule this repo follows when the same fact could plausibly live in two places: it lives in one,
and the other reads it. Restating it would create a pair that can disagree, and a pair that can
disagree eventually will — usually quietly, and usually in the copy nobody is looking at.

| Fact | Its only home | How everything else gets it |
|---|---|---|
| What a feed is, and how to load it | `meta_source_config` (seeded by `nb_99`) | `src/lib/config.py` is the single reader |
| What a data-quality rule asserts | `meta_dq_rules` | `src/lib/dq.py` interprets rows, never literals |
| The silver type of every column | `SILVER_TYPES` in `src/lib/contracts.py` | prose form in `docs/data-contracts.md`, asserted by tests |
| The gold transformation logic | `src/warehouse/procs/*.sql` | `src/lib/gold.py` interprets the files; it holds no copy |
| Gold column names, order, types, nullability | `src/warehouse/ddl/*.sql` | the same harness translates the DDL |
| Every measure's DAX | `semantic-model/definition/**/*.tmdl` | `measures.dax` is **generated** by `tools/extract_dax.py`, and drift fails CI |
| A lakehouse's name | `Layer` + `lakehouse_prefix` in `src/runtime/context.py` | `table_ref()` |
| What the Fabric Warehouse T-SQL subset permits | `tools/fabric_tsql_lint.py` (41 rules) | prose form in `docs/fabric-tsql-subset.md` |
| The name a proc reports itself under | the proc's own `@proc_name` DECLARE | `run.py` reads it out of the interpreted variable rather than guessing from the filename |

Two entries on that list are the interesting ones.

**`measures.dax` is generated, and CI fails on drift.** A hand-maintained second copy of 36 measures
is a guarantee that one of them will diverge. The generator makes the TMDL the only source and the
`.dax` file a build artefact that happens to be committed, which is the only arrangement in which
"the DAX in the repo is the DAX in the model" is a fact rather than a hope.

**The proc's name comes from the proc.** `03_sp_load_dim_account.sql` logs itself as
`sp_load_dim_account`, and `run.py` could have stripped the numeric prefix. It reads `@proc_name`
instead, and falls back to stripping only when a proc failed *before* its DECLARE and still needs a
name to be reported under. A filename convention is a guess; the DECLARE is the fact.

---

## 4. What changing one thing costs

This is the test of whether the metadata-driven claim in `design-decisions.md` #4 is real. It is
real for bronze and silver and deliberately **not** real for gold, and being precise about where it
stops is more useful than the claim itself.

| Change | What has to happen | Code change? |
|---|---|---|
| An eighth feed, landing to silver | one row in `SOURCE_CONFIG`, its rules in `DQ_RULES`, one entry in `SILVER_TYPES`, one contract section | no notebook, no pipeline |
| That feed also reaching gold | plus a `CREATE TABLE`, a `stg.` staging table and a load proc | **yes** — gold is not metadata-driven |
| A DQ threshold | one field in one `meta_dq_rules` row | no |
| A new DQ *rule type* | a branch in `src/lib/dq.py` and its tests | yes, one file |
| A feed's file format | one column (`source_format`) in `meta_source_config` | no |
| A silver column's type | `SILVER_TYPES`, `docs/data-contracts.md`, and a re-run of silver from bronze | yes, and the re-run is the point of seam 2 |
| A new measure | one `measure` block in TMDL, then regenerate `measures.dax` | no DAX written twice |
| A new dimension | DDL, staging table, proc, TMDL table, relationship | yes — five files, in that order |
| Moving gold off Warehouse onto Lakehouse SQL | not possible without rewriting all ten procs: the lakehouse SQL endpoint is read-only | that is `design-decisions.md` #1 |
| 100× the data | one `--scale` parameter | no — but see below |

The last row is the one with a non-obvious answer, and the honest version is: the *parameter* is one
value and the *consequences* are not. In order of what I expected to break first — small-file
pressure in bronze, then shuffle behaviour in the SCD2 merges, then the partition strategy, then the
gold surrogate-key assignment, which resolves six dimension lookups per fact row and is the
statement most likely to spill on a distributed engine. That last one is question 1 of
[`docs/fabric-deployment.md`](fabric-deployment.md) §1 for exactly this reason.

**Then I measured it, and the first item is wrong** — or rather, it is an answer to a question that
"100× the data" does not actually ask. Bronze is partitioned by `ingest_date` at one live file per
partition, so 100× the *rows* over the same window makes every file roughly 100× **bigger**
(`br_transactions`: 28 KiB per file to about 2.8 MiB), which improves the small-file profile rather
than degrading it. What degrades it is 100× the *window*, which is elapsed time and not volume at
all — and by that measure the pressure is already here, at 1×, with `br_disputes` holding 256 rows
in 95 files. The ordering above survives for items 2 to 4; item 1 needed a measurement to correct,
and [`docs/cost-and-capacity.md`](cost-and-capacity.md) §5 is that measurement. The capacity
arithmetic for the rest of the row is on the same page, which also finds that the Direct Lake
guardrails everyone reaches for first are nearly 4,000× away from binding.

---

## 5. Where the verification lives

Worth a short section because the shape is unusual on purpose: **nothing verifies itself.**

```
src/warehouse/*.sql   ──verified by──►  tools/fabric_tsql_lint.py   (sqlglot, no src/ imports)
                      ──executed by──►  src/lib/gold.py             (translates, holds no logic)
                      ──reconciled by─►  tests/test_gold_recon.py    (ties silver to gold, by hand)

semantic-model/*.tmdl ──verified by──►  tests/test_semantic_model.py (the DIVIDE rule, additivity)
                      ──extracted by──►  tools/extract_dax.py         (drift fails CI)
                      ──evaluated by──►  dashboard/build_dashboard.py (the numbers are sensible)

src/lib/*, notebooks  ──verified by──►  tests/  (384 tests; an isolated lake per module)

§1 of this page    ──verified by──►  tests/test_import_graph.py   (the ASTs, not the prose)
```

The last row is the odd one: a test whose subject is a document. §1 makes three claims about the
import graph that three other claims in this repo rest on, and a claim of that shape either has a
test or is a description of what happened to be true the day it was written.

The two claims that cannot be combined off-tenant are set out in
[`docs/gold-execution.md`](gold-execution.md): the harness proves the logic, the linter proves the
dialect, and nothing available here proves both at once. §1's "`tools/` imports nothing from `src/`"
is what keeps those two verdicts independent rather than two readings of the same assumption.

---

## 6. What I would add next, and why

Seven things are out of scope, and `README.md` §10 promises each one a sentence here. Each is a cut
rather than an oversight. They are items 2–8 below, in the order I would actually work in, which is
not the order of how impressive the items sound.

Item 1 is kept, and its number with it, because it is the one entry on this list that has moved.

**1. Delta maintenance (`nb_03_table_maintenance`) — built, and the build reversed the fix.** This
was the only genuine gap on this page rather than a boundary: the plan specified the notebook, it was
cut for time, and nothing in the repo compacted anything. It exists now — `make maintain`, `OPTIMIZE`
then `VACUUM` over the 21 lakehouse tables, one row per (table, action) into `meta_maintenance_log`.
What it found is worth more than the notebook.

**`OPTIMIZE` cannot fix bronze, and this page used to say it could.** Bin-packing happens *within* a
partition and never across one, and bronze holds exactly one file per `ingest_date` partition — so
it was already at the end state of compaction before the sweep ran. Delta's own metrics confirmed
it: 0 files removed, 0 partitions touched, on all seven tables. The same sweep took `meta_run_log`
from 48 files to 1 and 200 KB to 6 KB, which is where compaction does pay — unpartitioned append
tables. The remedy for bronze is `partition_column` in `meta_source_config`, a **design** change a
schedule cannot reach, so the notebook reports those six tables as an advisory naming that column
instead of reporting a success. [`docs/cost-and-capacity.md`](cost-and-capacity.md) §6 has the
before/after.

**The scope is deliberately narrow, and the narrowness is the design.** It is a **lakehouse** concern
only: `Layer` has no `GOLD` member, and asking for gold raises rather than being silently skipped.
`wh_gold` is a Warehouse and manages its own storage — there is no `OPTIMIZE` for a user to run
against it — so V-Order, the write-time Parquet optimisation read-heavy Direct Lake queries benefit
from, does not arise for the tables this model reads. It would arise immediately if a future semantic
model read a lakehouse table directly, and the default is the part worth knowing: V-Order is
**disabled by default in all newly created workspaces**, because the default favours write-heavy
engineering over read-heavy serving. So the lakehouse-serving version is not "remember to leave
V-Order on" but a deliberate choice per table — a read-heavy resource profile, a
`delta.parquet.vorder.enabled` table property, or an `OPTIMIZE` that applies it. The notebook
therefore **reads and reports** that property and never writes it, which is also the only thing it
could do: OSS Delta 3.2 rejects the property outright with `DELTA_UNKNOWN_CONFIGURATION`, so a line
that set it would be a line this repo could not have tested.

> Source for the default, since nothing else in this repo states it:
> [Optimize Delta Lake tables with V-Order in Fabric](https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order),
> ms.date 2026-03-01 — `spark.sql.parquet.vorder.default` is `false` in new workspaces, and the
> `spark.sql.parquet.vorder.enable` setting was removed in runtime 1.3, which is the version of this
> fact I would have got wrong from memory.

**2. Mirroring, instead of file extracts.** The accounts and customers feeds here are CSV with `_op`
and `_change_ts` columns, which is a file-shaped imitation of change data capture. Real ones would
come from an operational Azure SQL, and Fabric Mirroring puts a near-real-time replica in OneLake
without a pipeline. What makes this the second thing rather than the fifth: `src/lib/scd2.py` would
not change at all — it would read a mirrored change feed instead of parsing a column — so it removes
a whole class of hand-rolled ingestion while *proving* the SCD2 code was correctly factored. The
work is in the contract, not the merge.

**3. Real-time Intelligence — Eventstream into an Eventhouse.** Authorisation traffic is a stream
and this repo batches it, which is right for the reconciled system of record and wrong for the
question "are declines spiking right now". I would add an Eventstream writing to an Eventhouse KQL
database for the sub-minute view, and leave the batch path untouched as the reconciled truth. This
is the one cut that changes the *architecture* rather than adding to it, because it introduces a
second serving path with different consistency guarantees — and the hard part is not the ingestion,
it is deciding what a report does when the two disagree, which they will, by design, for the length
of the batch window.

**4. Purview — lineage and classification.** The repo has thorough *internal* lineage — `run_id`,
`_batch_id`, `load_batch_id`, `meta_run_log`, `stg.load_log` — and no catalogue-level lineage or PII
classification at all. `src/warehouse/ddl/02_dimensions.sql` masks `first_name`, `last_name`,
`email` and `date_of_birth` on `dim_customer` inline in the `CREATE TABLE`, which means the
definition of "this column is sensitive" lives in DDL and nowhere else. The change worth making is
not "add Purview" but *invert that*: classify the columns, and let the masking policy follow the
classification rather than restating it. A masking rule that is a
consequence of a classification cannot drift from it.

**5. ML and fraud scoring.** The repo stops at descriptive analytics. A scoring model would be a
Spark notebook writing to a silver table keyed on `transaction_id`, with MLflow in the workspace as
the registry and the score surfaced as a fact column rather than a dimension. The reason it is not
here is specific and worth saying out loud rather than hiding: `nb_00_generate_landing_data`
*injects* the fraud signal, so a model trained on this data would be measuring my own generator's
parameters and reporting excellent AUC. That is a demo, not evidence. With real data it is the
obvious next layer; with synthetic data it would be the least honest thing in the repo.

**6. A live Power BI report.** `semantic-model/` is the model — 10 tables, 14 relationships, 36
measures in TMDL — and `dashboard/build_dashboard.py` proves the measures resolve to sensible
numbers. What is missing is the report layer itself: layout, interaction, drill paths, and the
question that only a real report answers, which is whether the visuals stay in Direct Lake or trip a
guardrail and fall back to DirectQuery. Power BI Desktop is Windows-only and there is no tenant, so
this cut is imposed rather than chosen — but the part I would have got wrong by guessing is exactly
the fallback behaviour, so authoring a report blind would have produced a confident and unverified
artefact.

**7. Multi-region.** One capacity, one region, and for a payments platform that is a simplification
with a regulatory edge to it: data residency means a European account's transactions may not be
processable in another region. The design question — not a configuration question — is whether a
cross-region gold can exist at all, or whether each region gets its own warehouse and the
group-level view becomes a composite model over several of them. I have not designed that here
because designing it properly means knowing which jurisdictions, and inventing the jurisdictions to
design around would be the same mistake as training on my own generator.

**8. User Data Functions — and the honest answer is that I would not add this.** It is on the
out-of-scope list, so here is the sentence it is owed: a UDF could host the FX conversion or the DQ
rule evaluation as a callable invoked from a pipeline, and nothing in this repo needs that boundary.
The transformations are where the data is, which is where they belong. I would add it when something
outside Spark needed to call this logic — a service, a Power App — and not before. Adding it now to
demonstrate awareness of the feature would be the wrong instinct, and it seemed more useful to say
that than to invent a use for it.

---

## 7. What would falsify this page

In the order I think most likely.

1. **Seam 3's Fabric form may not be one statement.** The cross-database `INSERT INTO stg.… SELECT *
   FROM lh_silver.dbo.…` is the documented shape, but whether a warehouse transaction can span the
   warehouse and a lakehouse SQL analytics endpoint in the way the staging fill assumes is the thing
   I would test in the first hour, and if it cannot, the fill becomes a pipeline Copy activity and
   §2's "only the fill changes" becomes "the fill changes and acquires an orchestration dependency".
2. **The one-way graph is enforced, but only over static imports.**
   [`tests/test_import_graph.py`](../tests/test_import_graph.py) walks every module's AST and fails
   on an upward edge, on a relative import, on a package that no layer declares, and on a notebook
   importing a notebook — so §1 is a guarantee rather than a description, and a new top-level
   package fails the suite until someone places it in the graph deliberately. What it cannot see is
   a dynamic import: `importlib.import_module` with a computed name is invisible to an AST walk.
   `src/lib/gold.py` does use `importlib`, for loading `.sql` files rather than Python modules, so
   the blind spot is currently unoccupied — but it is the one way §1 could be wrong while the suite
   is green, and the test's own docstring says so.
3. **§4's change-cost table is reasoned, not measured, below the line marked "no code change".** The
   eighth-feed row is the one I am confident about, because the seven existing feeds differ in
   format, load type and SCD type and are all driven from the same two notebooks. The 100× row is an
   ordered guess about what breaks first, and the ordering is the part most likely to be wrong.
4. **§6's ordering is a judgement about a codebase, not about a business.** Put this in front of
   someone with a real backlog and Mirroring probably moves, because whether it is second or fifth
   depends entirely on whether their sources are already in Azure SQL.

*Where this page states a platform fact — the read-only lakehouse SQL endpoint, Mirroring's shape,
the transaction scope in seam 3 — the citation is in
[`docs/fabric-deployment.md`](fabric-deployment.md) or
[`docs/design-decisions.md`](design-decisions.md) rather than repeated here, for the reason in this
page's own opening table. The one exception is V-Order's default in §6, which nothing else in the
repo states, so it is cited where it is claimed.*
