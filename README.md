# Payments medallion lakehouse — Fabric-targeted, locally runnable

A metadata-driven medallion lakehouse over a synthetic UK card-payments estate: seven source feeds
land as files, are ingested append-only into bronze, conformed and quality-gated into silver with
SCD2 history, and served from a gold star schema. The transformation code is written as Microsoft
Fabric notebook code; it runs on a laptop.

[![ci](https://github.com/billalsohail/fabric-payments-medallion/actions/workflows/ci.yml/badge.svg)](https://github.com/billalsohail/fabric-payments-medallion/actions/workflows/ci.yml)

> ### Status
>
> **What runs:** landing → bronze → silver → gold, end-to-end, from a cold start, on PySpark 3.5 /
> Delta 3.2 — the same pairing as the Fabric Spark runtime. 312 tests pass locally, including
> run-it-twice idempotency, SCD2 interval invariants, a reconciliation that ties every bronze row to
> a silver row, a quarantined row or a deduplicated one, and a second reconciliation in which every
> difference between silver and the star schema is enumerated and attributed to a named cause.
>
> **What runs gold, precisely — because this is the part easiest to overclaim.** The ten T-SQL
> stored procedures in `src/warehouse/procs/` are the deliverable, and a local harness executes
> *them*: each statement is read out of the `.sql` file, transpiled from `tsql` to `spark` by
> sqlglot, and run against the silver Delta tables. There is no second implementation of the gold
> logic anywhere in this repo, so the local numbers cannot be right while the shipped T-SQL is
> wrong. But there is also no T-SQL engine on this machine — the SQL Server image never pulled — so
> the procs have never been executed *as T-SQL*. Their **logic** is executed and tested; the claim
> that they are valid **Fabric Warehouse** T-SQL rests entirely on `tools/fabric_tsql_lint.py`,
> which parses every `.sql` file and rejects constructs outside the documented surface area. Two
> claims, two mechanisms, deliberately never conflated —
> [`docs/gold-execution.md`](docs/gold-execution.md).
>
> **Verified in CI**, not only on my laptop: every push runs the badge above — a cold GitHub runner
> generates the feeds from a seed, runs all three stages, runs them a second time to prove the rerun
> is a no-op, then runs the suite, and separately asserts the suite was not *skipped*. That last step
> is not paranoia: `conftest.py` skips the whole session when landing data is missing, which is right
> locally and is a trap in CI, where a skipped session is a green tick. A build that proved nothing
> must not look like a build that passed.
>
> **What is written but not yet built:** the `fabric/` deployment artefacts. The semantic model is
> written — `semantic-model/` holds the Direct Lake model in TMDL, the text format Fabric's own git
> integration uses — and **no DAX engine has ever loaded it**: its correctness rests on 23 tests
> tying every column, relationship and measure reference back to the warehouse DDL, plus
> `make dashboard`, which reimplements the measures in SQL and renders them through the model's own
> format strings. That second check is the one that found a real defect — every money measure was
> reporting a figure 100× too large, and 23 passing tests could not see it, because a format string
> that formats the wrong magnitude is consistent with everything
> ([`semantic-model/README.md`](semantic-model/README.md)). See
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
it for `get_spark()`, `read_table(layer, name)`, `write_table(df, layer, name, ...)`,
`table_exists(layer, name)`, `delta_table(layer, name)` and `landing_path(entity, ingest_date)`.
Locally those resolve to a configured `SparkSession` and Delta paths under `./_onelake/`; on Fabric
they resolve to the ambient `spark` and lakehouse table names. Nothing above the shim knows which
substrate it is on.

Two things the shim deliberately does **not** offer are worth naming, because the absence is the
design rather than an omission:

- **There is no `sql_exec(...)`.** A notebook able to reach a SQL endpoint through the shim would
  put gold's T-SQL behind the same interface as bronze's DataFrame code, and
  [`docs/gold-execution.md`](docs/gold-execution.md) exists to keep those apart: they carry
  different kinds of evidence. The T-SQL is run by a separate harness, `src/lib/gold.py`, which
  transpiles it to Spark SQL and is careful to call itself a harness rather than a second
  implementation.
- **No caller is ever handed a path or a table name.** `table_ref(layer, name)` is what resolves
  `./_onelake/silver/dim_account` against `lh_silver.dim_account`, and it is internal — the four
  table functions above call it; nothing above the shim does. A notebook cannot hard-code a location
  because it is never given one.

Three consequences of that constraint are worth naming, because they are the things a reviewer
should look for:

- **No notebook imports another notebook.** On Fabric a notebook can only `%run` another, not
  `import` it, so anything two notebooks share has to be library code attached to the Spark
  Environment. That is why `load_source_config` lives in `src/lib/config.py` and both `nb_01` and
  `nb_02` bind it to a module-level `load_config` of their own, rather than one defining it and the
  other importing it. It looks like an odd
  indirection until you know the platform rule it protects.
- **Spark and Delta are pinned to 3.5 / 3.2** because that is the Fabric runtime pairing. Version
  parity with the deployment target is a choice, not a coincidence.
- **Gold is a Warehouse, not a lakehouse.** The lakehouse SQL endpoint on Fabric is read-only, so
  T-SQL DDL and multi-table transactional loads need a Warehouse — and a Warehouse has a
  deliberately narrower T-SQL surface than SQL Server: no sequences, no triggers, no *enforced*
  keys. Surrogate keys are generated with `ROW_NUMBER() OVER (...)` over the current max, and the
  data-quality layer — not a foreign key — *is* the integrity guarantee. `IDENTITY` is the
  interesting case: Fabric **does** support it, and this repo declines to use it anyway because
  Fabric's allocation is not guaranteed contiguous or reproducible, which a rerunnable load needs
  ([`docs/design-decisions.md`](docs/design-decisions.md) §12). Not using a feature and not having
  it are different claims, and `tools/fabric_tsql_lint.py` is careful to make only the second one.

The second organising idea is that **an eighth feed should be a row in a table, not an edit to a
notebook.** `nb_01_bronze_ingest.py` and `nb_02_silver_transform.py` each handle all seven feeds
with no per-entity branch anywhere in either file. Every difference between `transactions` (JSONL,
incremental, schema-evolving) and `card_products` (CSV, static, overwritten) is a column in
`meta_source_config`. That claim is the one most worth attacking when reading the code: grep either
notebook for an entity name and you should only find it in a docstring.

---

## 3. How to run it

**Prerequisites.** A JDK (Temurin 17 is what this was built on) and
[`uv`](https://docs.astral.sh/uv/). Python 3.11 is pinned — PySpark 3.5 does not support 3.12+, and
`make setup` will create the right interpreter for you. **No Docker, no database.** Gold runs through
the harness in `src/lib/gold.py`; `docker-compose.yml` is kept as the documented optional path for
executing the same procs against real SQL Server, and is not needed by anything here.

```bash
make all        # cold start: generate landing data, seed the control plane, run, test
```

Or step by step, which is more instructive:

```bash
make setup                  # uv venv (python 3.11) + pinned deps
make generate SCALE=tiny    # deterministic landing files under ./_onelake/files/landing/
make seed                   # the metadata control plane: config, DQ rules, watermarks
make run                    # bronze, silver, gold — driven entirely by meta_source_config
make run                    # run it again — this is the interesting one
make test                   # 312 tests
make maintain               # OPTIMIZE + VACUUM the Delta layers, on its own schedule
make fabric-build           # render fabric/items/ into the git-integration layout (UNVALIDATED)
```

The second `make run` is the point. It should do almost nothing, and say so:

```
fx_rates                 bronze  skipped     watermark 2026-09-20 is current
accounts                 silver  skipped     silver watermark 2026-09-20 is current
card_products            silver  succeeded   12 rows    # static feed: always overwrites, by design
disputes                 silver  succeeded    0 rows    # re-read its 90-day window, wrote nothing
sp_load_fact_transaction gold    succeeded   49,708     # MERGE, same 49,708 rows as the cold run
```

`disputes` is the one that earns its keep in silver. It reprocesses a 90-day window on every run to
absorb late arrivals, so it genuinely re-reads and re-presents rows it has already loaded — and
writes zero, because the MERGE is guarded on a content hash. That is the difference between a
pipeline that is idempotent and one that has merely never been run twice.

Gold is the opposite case and it is worth being clear about why. Every proc runs on every run: there
is no watermark on a warehouse load, and skipping one would mean trusting that nothing upstream had
been reprocessed. So gold does the work again and arrives at the same place — 6,996 account
versions, 49,708 facts, 32,654 aggregate rows, identical to the cold run — because each proc is
written to be re-runnable from the top. The second run costs ~25 seconds and buys the guarantee that
a partial failure anywhere upstream is repaired by running the pipeline again, which is the only
repair procedure this design has.

Useful extras:

```bash
make idempotency   # run, run --force-reload, then assert bronze is byte-for-byte unchanged
make test-fast     # skip the end-to-end cases
make lint          # ruff, plus the Fabric T-SQL subset linter over src/warehouse/
make reset-lake    # drop bronze/silver/gold/meta/quarantine, keep the generated landing files
make clean         # remove the venv, the caches and the entire local lake
```

A first full run at `tiny` takes a few minutes, most of it Spark start-up. The whole lake lives in
`./_onelake/` and is git-ignored; nothing outside the repo directory is touched.

### What a cold run actually produces

```
24 step(s), 225,956 rows, 0 not successful

entity          bronze  →  silver  →  gold
card_products       12       12        12
fx_rates           920      920       920
customers        8,000    2,080     2,080   versions   (4 monthly snapshots → SCD2 history)
merchants        2,000      559       559   versions
accounts        10,507    6,996     6,996   versions   (SCD2: no-op changes create no history)
transactions    50,158   49,708    49,708             (292 quarantined, 158 deduplicated)
disputes           256      256       256
                                   32,654   agg_merchant_daily
                                      366   dim_date
                                       21   seeded reference data + unknown members
```

Two columns of that table say something the row counts alone do not. `customers` lands 8,000 bronze
rows from 2,000 customers because **all four** monthly snapshots are ingested, not just the newest —
the sequence of snapshots *is* the SCD2 history, and an earlier version of the bronze notebook that
kept only the latest one produced dimensions which passed every obvious test while leaving 78% of
facts unable to find a dimension version covering their authorisation date. And gold's counts match
silver's exactly for every dimension and fact, which is the reconciliation's whole subject: gold does
not drop rows, so the only differences it is allowed to introduce are the unknown members, and
`tests/test_gold_recon.py` enumerates each one rather than tolerating a delta.

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
Gold             T-SQL procs on a Spark harness     →   wh_gold              Warehouse (T-SQL)
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

Six Delta tables, seeded by `nb_99_seed_metadata.py`. Five of them contain every per-feed decision;
the sixth is written by the maintenance notebook rather than read by the pipeline:

| Table | Holds |
|---|---|
| `meta_source_config` | One row per feed: format, load type, merge keys, SCD type, partition column, DQ rule set, effective-from and operation columns, enabled flag |
| `meta_dq_rules` | One row per rule: type, parameters, scope condition, severity, failure threshold |
| `meta_watermark` | High-water mark per `(entity, layer)` — bronze and silver advance independently |
| `meta_run_log` | One row per step: rows read / written / quarantined / deduplicated, status, timing |
| `meta_dq_results` | One row per rule per batch: the verdict, the rate, and whether it breached |
| `meta_maintenance_log` | One row per table per maintenance action: files and bytes before and after, what `OPTIMIZE` merged, what `VACUUM` removed, and the V-Order property as read |

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

`make test` — 312 tests. The ones that matter:

| Claim | Test |
|---|---|
| Running the pipeline twice changes nothing | `test_bronze.py`, `test_silver.py`, `make idempotency` |
| A forced reload rewrites the same bytes | `test_a_forced_silver_reload_rewrites_nothing` |
| Every bronze row is accounted for downstream | `test_every_bronze_row_is_accounted_for` |
| A malformed value is quarantined, not cast to null | `test_an_uncastable_value_is_quarantined_rather_than_cast_to_null` |
| Defects are quarantined, not merely counted | `test_defects_were_actually_quarantined_not_merely_counted` |
| A DQ failure genuinely fails a run, and does not advance the watermark | `test_an_error_severity_rule_fails_the_run_and_records_why` |
| A type contract cannot be weakened from config | `test_a_type_contract_cannot_be_smuggled_in_as_a_config_rule` |
| One `is_current` row per key, no overlaps, no zero-width versions | `test_scd2.py` |
| A transaction joins to the *then*-current dimension version | `test_a_transaction_joins_to_the_then_current_version` |
| Two sequential batches equal one combined batch | `test_incremental_load_matches_a_single_load` |
| A delete-only batch still closes its incumbent | `test_a_delete_only_batch_closes_the_incumbent` |
| Late-arriving disputes land without duplicating | `test_the_dispute_window_reprocesses_without_duplicating` |
| The month-10 new column is absorbed, not fatal | `test_bronze_absorbs_a_new_source_column_mid_feed` |
| The generator still matches the published contract | `test_contracts.py` |
| The same seed produces byte-identical data | `test_determinism.py` |
| Every unknown-member key in a fact has a named, correct cause | `test_every_unknown_member_has_a_named_cause` |
| Gold invents no rows and loses none | `test_fact_counts_tie_exactly_to_staging`, `test_dimension_counts_are_staging_plus_one_unknown_member` |
| Money ties to the penny between silver and the star schema | `test_money_ties_exactly_between_staging_and_gold` |
| The SCD2 dimensions actually carry history, not one row per key | `test_scd2_dimensions_actually_carry_history` |
| A proc that failed cannot report success | `test_every_proc_reported_success` (reads `stg.load_log`) |
| `distinct_account_count` is non-additive, and the warning is not hypothetical | `test_distinct_account_count_is_not_additive_and_is_only_right_per_day` |
| The T-SQL uses only the documented Fabric Warehouse subset | `test_fabric_tsql_lint.py`, `make lint` |
| The DDL declares no construct Fabric rejects | `test_warehouse_ddl.py` |
| Every modelled column exists in the warehouse, with a compatible type | `test_every_modelled_column_exists_in_the_warehouse` |
| Every model relationship has a declared foreign key behind it, and vice versa | `test_every_relationship_has_a_foreign_key_behind_it`, `test_every_foreign_key_has_a_relationship` |
| No measure divides with `/`, so a zero denominator cannot render as Infinity | `test_no_measure_divides_with_a_slash` |
| The non-additive aggregate column is reachable only through its grain guard | `test_the_non_additive_aggregate_column_is_only_reachable_through_a_guarded_measure` |
| Every resolved-date dispute measure also excludes open disputes | `test_every_resolved_date_measure_also_excludes_open_disputes` |
| Direct Lake cannot silently fall back to DirectQuery, and nothing is a calculated column | `test_the_model_forbids_falling_back_to_directquery`, `test_there_are_no_calculated_columns` |
| Every lineage tag is derivable from its object's path, so a copied table file fails | `test_every_lineage_tag_is_derived_from_its_object_path` |
| `measures.dax` has not drifted from the TMDL it is generated from | `make lint` (`tools/extract_dax.py --check`) |
| Every import in the repo points one way, and the shim depends on nothing above it | `test_import_graph.py` |
| The parameters the orchestrator passes are parameters the notebooks declare | `test_the_orchestrator_passes_parameters_the_notebooks_actually_have` |
| The shim has no SQL escape hatch, and hands no caller a path or a table name | `test_import_graph.py` |
| `OPTIMIZE` cannot merge across partitions, which is why it cannot fix bronze | `test_optimize_cannot_merge_across_partitions_and_says_so` |
| Gold is unreachable from the maintenance notebook by type, not by convention | `test_gold_is_not_a_maintainable_layer` |
| `VACUUM` removes stale files and leaves the live snapshot byte-identical | `test_vacuum_removes_stale_files_and_leaves_the_live_snapshot_alone` |
| The maintenance log is the seeder's schema, not a second copy of it | `test_the_log_uses_the_seeders_schema_rather_than_a_second_copy` |
| A money measure divides by 100 exactly when it sums a minor-units column, and a `_minor` column is never formatted as money | `test_a_measure_sums_minor_units_exactly_when_it_divides_by_100`, `test_every_minor_column_is_formatted_as_an_integer` |
| The average approved payment is a plausible figure in pounds, not a figure 100x too large | `test_dashboard_average_approved_value_is_in_pounds_not_pence` |
| Two measures agree with a third SQL path that shares no expression with the dashboard's | `test_dashboard_authorisation_rate_agrees_with_an_independent_count` |
| The three deliberate absences in `fabric/` stay absent | `test_the_warehouse_is_deliberately_not_an_item`, `test_the_pipeline_is_an_item_without_a_body`, `test_there_is_no_parameter_yml` |
| The variable library is exactly the table `docs/fabric-deployment.md` §6 publishes, value for value | `test_variable_defaults_are_the_dev_column_of_the_documented_table`, `test_a_value_set_overrides_only_what_actually_differs` |
| Rendering a notebook into Fabric's cell format loses no cell and invents none | `test_the_render_round_trips_back_to_the_same_cells` |

Three notes on how these are written, because they are the difference between a suite that checks
the work and one that agrees with it:

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

And every test that could pass over an empty set carries an explicit guard that it did not. This
sounds pedantic and it is the single most useful convention in the suite. The bronze bug described
in §3 — keeping only the newest monthly snapshot — was invisible to *every count test in this repo*,
all of which tied while 78% of `fact_transaction` pointed at the unknown merchant, because the
dimensions genuinely did contain one current row per key and gold genuinely did preserve every row
silver gave it. What found it was asking a different question: not "do the numbers agree?" but "and
*why* is this key unresolved?" `test_every_unknown_member_has_a_named_cause` enumerates the three
causes this repo has decided are correct, asserts zero rows fall outside them, and asserts each
bucket is non-empty — so if the data ever stops exercising a case, the test fails loudly instead of
passing over nothing. A check that cannot fire converts "unverified" into "falsely verified", which
is worse than having no check at all.

---

## 8. Build state

Honest, because the alternative is worse. Three days were budgeted; this is the end of day two.

| Area | State |
|---|---|
| Execution-context shim, data contracts, deterministic generator | **Done**, tested |
| Control plane (6 metadata tables) | **Done**, seeded |
| Bronze ingest, idempotent, schema-evolving | **Done**, tested |
| Local orchestrator mirroring `pl_master`, all three stages | **Done** |
| DQ rule engine, SCD2, type contracts | **Done**, tested |
| Silver transform, all seven feeds | **Done**, tested |
| Gold: star-schema DDL + 10 load procedures | **Done**; logic executed and reconciled, dialect linted, never run by a T-SQL engine ([`docs/gold-execution.md`](docs/gold-execution.md)) |
| `tools/fabric_tsql_lint.py` — Fabric T-SQL subset linter | **Done**, 70 tests, 41 rules, each citing the Microsoft Learn page and `ms.date` it came from |
| CI (`.github/workflows/ci.yml`) | **Done and green.** A cold runner generates the feeds, runs all three stages, runs them again, then runs the suite — and asserts the suite was not skipped, because a skipped session is also a green tick |
| Semantic model (TMDL) | **Done** — 10 tables, 14 relationships, 36 measures, 23 tests tying it to the warehouse DDL; **never loaded by Fabric or any DAX engine** ([`semantic-model/README.md`](semantic-model/README.md)) |
| `semantic-model/measures.dax` | **Done and generated** from the TMDL by `tools/extract_dax.py`; `make lint` fails on drift |
| Static dashboard (`dashboard/`) | **Done** — `make dashboard` reads the warehouse and emits a self-contained two-page HTML file covering 34 of the 36 measures. Not a Power BI report and not a substitute for one; it exists so the measures produce *numbers*, and the first number it produced was wrong by 100× ([`dashboard/build_dashboard.py`](dashboard/build_dashboard.py)) |
| [`docs/fabric-deployment.md`](docs/fabric-deployment.md) | **Done** — item inventory, workspace layout, the dev→prod pipeline with its deployment rules, and a first-two-hours runbook that opens with the two questions no off-tenant check can answer. Every platform claim cites the Learn page it came from; §9 lists what would falsify it, starting with an open question the docs did not settle |
| `fabric/` deployment layer | **Done, and UNVALIDATED — nothing in it has been run against a tenant.** Twelve `.platform` descriptors plus the variable library are in git; the git-integration layout itself is *generated* into `fabric/build/` by `make fabric-build`, because a file that can be derived should not also be committed. Three absences are deliberate and enforced by tests: no `wh_gold.Warehouse` item, no hand-written `pipeline-content.json`, no `parameter.yml`. 20 tests, and one format in it that no Learn page prints ([`fabric/README.md`](fabric/README.md)) |
| [`docs/fabric-tsql-subset.md`](docs/fabric-tsql-subset.md) | **Done** — the rules, the Learn pages they came from, the four corrections those pages forced, and what the linter cannot tell you |
| [`docs/design-decisions.md`](docs/design-decisions.md) | **Done** — fourteen decisions, the last four made *by* the code rather than before it, each with the cost it carries |
| [`docs/architecture.md`](docs/architecture.md) | **Done** — the structural view: the one-way dependency graph, the six seams and what each one promises, the single-owner table, what changing one thing actually costs, and "what I would add next, and why" for every cut in §10 below |
| [`docs/databricks-to-fabric.md`](docs/databricks-to-fabric.md) | **Done** — nine rows of translation, and then the places where a Databricks habit produces a design that is wrong on Fabric rather than merely unfamiliar. §10 lists five platform facts I would have stated confidently and wrongly from memory, with the Learn page that corrected each; one of them added a test to the gold suite |
| [`docs/cost-and-capacity.md`](docs/cost-and-capacity.md) | **Done** — the arithmetic, measured out of the Delta logs by `tools/lake_footprint.py` rather than estimated. It states up front the one number it cannot produce (CU-seconds per operation, which needs the Capacity Metrics app and a real capacity) and then does the half that is not a guess: on an F2 the Direct Lake guardrails are **3,855× away** from binding and the real ceiling is **one Medium node**. §5 corrects a prediction in `docs/architecture.md` §4 that measurement showed was answering the wrong question |

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
docs/fabric-tsql-subset.md    The Fabric Warehouse T-SQL subset the linter enforces, where each rule
                              came from, and what a clean lint does not prove.
docs/gold-execution.md        What executes the gold T-SQL with no tenant, and what that entitles
                              a reader to believe about it. Read before src/warehouse/.
docs/design-decisions.md      Fourteen decisions and what each one cost. The last four were forced
                              by the code, and are kept separate for that reason.
docs/databricks-to-fabric.md  The translation from a Databricks background, and the five platform
                              facts I had wrong from memory until I read the page.
semantic-model/README.md      What the model is, what the 23 tests check, and the six named gaps —
                              including why there are no RLS roles. Read before the TMDL.

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
src/lib/gold.py               The gold harness: interprets the T-SQL procs against Delta, and the
                              bridge that stages silver into the warehouse's stg schema.

src/notebooks/nb_00_*.py      Generate landing data. A feature, not fixture setup.
src/notebooks/nb_01_*.py      Bronze ingest — one notebook, seven feeds, no branches.
src/notebooks/nb_02_*.py      Silver transform — one notebook, seven feeds, no branches.
src/notebooks/nb_03_*.py      OPTIMIZE and VACUUM over the lakehouse layers, logged per table.
                              Deliberately not a pipeline stage; gold is refused, not forgotten.
src/notebooks/nb_99_*.py      Seed the control plane.

src/warehouse/ddl/            wh_gold DDL: schemas, dimensions, facts, aggregate, security.
src/warehouse/procs/          The ten load procedures. The gold deliverable.
tools/fabric_tsql_lint.py     The portability gate: 41 Fabric Warehouse subset rules over sqlglot.
tools/lake_footprint.py       Measures the lake from its Delta logs and does the F2 guardrail
                              arithmetic. Every number in docs/cost-and-capacity.md comes from here.

orchestration/run.py          Local stand-in for the pl_master Fabric pipeline, all three stages.

semantic-model/definition/    The Direct Lake model in TMDL: 10 tables, 14 relationships,
                              36 measures. Hand-authored, never loaded by Fabric.
semantic-model/measures.dax   Generated from the TMDL by tools/extract_dax.py. A runnable DAX query
                              over every measure; do not edit it.
tools/tmdl.py                 The TMDL reader the model's tests are built on, plus the lineage-tag
                              derivation.

fabric/items/                 Twelve .platform descriptors and the variable library — the two
                              things here that cannot be derived from anything else in the repo.
fabric/build_items.py         Renders those into Fabric's git-integration layout, including the
                              notebook cell format that Learn only ever shows in a screenshot.
fabric/deploy.py              The fabric-cicd wrapper. Never executed. --dry-run is the only mode
                              anything in this repo has ever proven.

tests/                        312 tests. conftest.py builds an isolated lake per module.
tests/test_import_graph.py    The one test whose subject is a document: it enforces the import
                              graph docs/architecture.md §1 describes.
```

If you are reviewing this and have ten minutes, read in this order: the header of
`src/notebooks/nb_02_silver_transform.py` (the five-step order and why each step cannot move),
`src/runtime/context.py` (the shim that makes the Fabric claim checkable), and
`tests/test_scd2.py` (what the SCD2 implementation refuses to promise).

---

## 10. Not in scope

Real-time Intelligence, ML and fraud scoring, Purview, Mirroring, User Data Functions, multi-region,
and a live Power BI report. Each is a deliberate cut rather than an oversight, and
[`docs/architecture.md`](docs/architecture.md) §6 gives each one a paragraph under "what I would
add next, and why" — in the order I would actually do them, with the reason each is a cut. Two of
those reasons are worth reading before the others: fraud scoring is absent because the generator
*injects* the fraud signal, so a model trained here would be measuring my own parameters; and User
Data Functions is the one item I argue I would **not** add, which seemed more useful than
inventing a use for it.

That section also carries one item README does not list, because it is not a cut and is no longer
missing: Delta maintenance. `nb_03_table_maintenance.py` runs `OPTIMIZE` then `VACUUM` over the
lakehouse layers and logs every action, and it keeps first place on that list because building it
**refuted** what this repo had written about it — `OPTIMIZE` cannot compact bronze here, and the
fix bronze actually needs is a partition-column decision rather than a maintenance job.
[`docs/cost-and-capacity.md`](docs/cost-and-capacity.md) §6 has the measurement that settles it.
