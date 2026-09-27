# Design decisions

Ten decisions, each with the alternative that was rejected and what rejecting it costs. They are
numbered because the rest of the repo cites them by number — `#1` from `tools/fabric_tsql_lint.py`
and `docs/gold-execution.md`, `#3` from `src/warehouse/ddl/05_security.sql`, `#10` from
`src/warehouse/ddl/03_facts.sql`, `docs/data-contracts.md` and `src/notebooks/nb_02_silver_transform.py`
— so the numbers are a stable interface and do not get reordered.

Decisions 11–14 came out of building rather than out of planning. They are kept separate for that
reason: a decision you made before writing any code and a decision the code forced on you are
different kinds of evidence, and pretending the second kind was foresight is the sort of thing that
falls apart under one follow-up question.

A note on what "decision" means here. Several of these had one defensible answer and the interesting
content is the *reason*, not the choice. Others were genuinely close, and those say so. The ones that
were close: #2, #3, #6 and #10.

---

## 1. Gold is a Warehouse, not a second Lakehouse

**Decision.** Bronze and silver are Lakehouses (Delta on OneLake, written by Spark). Gold is a
Fabric **Warehouse** (`wh_gold`), written in T-SQL, loaded by stored procedures.

**Why.** The Lakehouse SQL analytics endpoint is **read-only**. It will serve `SELECT` over the Delta
tables all day, but there is no `CREATE TABLE`, no `MERGE`, no stored procedure and no multi-table
transaction. A star schema loaded by ten procedures, where a dimension and the facts that reference it
have to move together, needs write T-SQL and needs a transaction boundary. That is a Warehouse.

**The alternative.** Keep gold in the lakehouse and build the star with Spark, serving it through the
endpoint. This works, and for a Spark-only team it is the lower-friction answer. It costs two things:
the dimensional loads become PySpark rather than SQL, which narrows who on a typical analytics team
can review them; and there is no transaction, so a partial failure mid-load leaves the star
internally inconsistent until the next successful run rather than rolling back.

**What it cost here.** Everything in `docs/gold-execution.md`. Choosing T-SQL for gold on a machine
with no Fabric tenant and (as it turned out) no working SQL Server container is what created the
whole two-claims problem — logic proven by a Spark harness, dialect proven by a linter, and nothing
proving both together. A Spark gold layer would have been fully executed and fully verified locally.
The decision still stands, because it is the right shape for the platform and the wrong shape only
for my laptop, but the honest accounting is that this repo's single largest verification gap is
downstream of decision #1.

---

## 2. Notebooks, not Dataflow Gen2

**Decision.** Every transformation is a notebook (`src/notebooks/*.py`, percent-cell format).
No Dataflow Gen2 anywhere.

**Why.** Three reasons, in order of how much they actually matter:

1. **Testability.** `nb_02_silver_transform.py` is a Python module with importable functions, so
   `tests/test_scd2.py` can hand `scd2.merge` a synthetic feed with two changes at the same instant
   and assert what comes out. A Gen2 dataflow is a mashup document; the unit of testing is "run it and
   look".
2. **Reviewability.** A diff on a notebook cell is a diff a reviewer can read. A diff on a dataflow's
   JSON is not, and the parts that matter most (the actual M expressions) are the parts least legible
   in a pull request.
3. **Cost.** Gen2 runs the mashup engine and stages intermediate results into an implicit staging
   lakehouse, so a unit of work generally consumes more capacity than the equivalent Spark job.
   I have not measured this — measuring it needs a capacity, which is the thing this project does not
   have — so it is stated as a directional claim with a mechanism behind it, not a multiplier. The
   first two reasons stand on their own without it.

**The alternative.** Gen2 is genuinely good at the thing it is for: a connector-heavy ingest from a
source Spark has no clean reader for, authored by someone who is not going to write PySpark. This
project's sources are files on a filesystem, which is exactly the case where the advantage disappears.

**Where the line actually is.** If a source needed an ODBC-only connector or an OAuth flow, Gen2
landing into bronze and notebooks from there would be a better design than writing a reader by hand.
The decision is "not for transformation", not "never".

---

## 3. Direct Lake, not Import

**Decision.** The semantic model is Direct Lake over the gold Warehouse, with its source kept as
text — TMDL plus `measures.dax`, which is the format Fabric's own git integration stores, rather than
a binary `.pbix`. `semantic-model/` now holds it: 10 tables, 14 relationships and 36 measures,
checked by 23 tests against the warehouse DDL and **never loaded by Fabric or any DAX engine** — see
[`semantic-model/README.md`](../semantic-model/README.md) for what that does and does not establish.

**Why.** Import mode would mean a third copy of the data (Delta in OneLake, columnstore in the
Warehouse, vertipaq in the model) and a refresh window that has to be scheduled after gold finishes
and sized so it fits. Direct Lake reads the Delta files directly into the same vertipaq structures at
query time, so there is no refresh to schedule and no second copy to keep current. For a star that is
already sitting in OneLake, it removes a whole moving part.

**The guardrails, because "no refresh" is not free.** Direct Lake silently falls back to DirectQuery
under conditions that are documented and capacity-dependent — table size and row-count limits per
SKU, unsupported column types, views, and calculated columns that cannot be folded. Fallback is not
an error; it is a quiet change in performance characteristics, which is worse. The design rule that
follows: gold tables must be plain physical tables with supported types, which is one more reason the
gold DDL has no computed columns even where one would be convenient.

**The conflict this creates with row-level security, and how it is resolved.**
`src/warehouse/ddl/05_security.sql` puts RLS on the star, and enabling RLS **gives up Direct Lake** —
Power BI queries against a Warehouse with row-level security fall back to DirectQuery in order to
honour the predicate. So #3 and the RLS design are in direct tension, and on the same tables both
cannot hold.

They are not on the same tables. `sec.account_region_policy` carries filter predicates on
`dim_account`, `fact_transaction` and `fact_dispute`, and **`agg_merchant_daily` is deliberately
absent from it** — the aggregate has no region column, so there is nothing to filter and nothing to
force fallback. The executive report reads the aggregate in Direct Lake; the regional drill-down reads
the detail in DirectQuery and pays for it per visual rather than per refresh. The security requirement
therefore shapes which table each report reads, which is a real architectural consequence rather than
a checkbox, and it is the sort of thing better discovered in the documentation than in a capacity bill.

**What is still a judgement call.** Whether the drill-down's DirectQuery latency is acceptable to the
people who use it is a fact about the organisation, not about the platform. If it is not, the answer is
a second aggregate at the grain that audience actually needs — not turning the filter off.

---

## 4. Metadata-driven, not one pipeline per source

**Decision.** One bronze notebook and one silver notebook, for all seven feeds. Every difference
between them is a row in `meta_source_config`. There is no `if entity == "transactions"` anywhere in
`src/notebooks/`.

**Why.** The eighth feed should be a config row. That is the whole claim, and it is testable: adding
a feed touches `nb_99_seed_metadata.py` and nothing else. Per-source pipelines start faster and then
diverge — seven notebooks with the same watermark logic copied seven times means a bug in that logic
gets fixed between one and seven times, and nobody knows which.

**What goes in config, and what deliberately does not.** Config holds: format and read options,
target table, load type, merge keys, SCD type, effective-from column, CDC operation column, partition
column, DQ rule set, priority wave, enabled flag. No entry in `meta_source_config` is code: the
moment a *movement* config column contains something that has to be `eval`'d, the control plane has
become a programming language with no tests and no debugger, and "which feeds load in what order"
stops being answerable by reading a table.

**There is one bounded exception, and it is in the other table.** `meta_dq_rules` has two columns
that hold SQL: `condition`, which restricts a rule to a subset of rows (`channel <> 'ATM'`, because
ATM withdrawals legitimately have no merchant), and the `expression` rule type's `predicate`, which
is an arbitrary boolean that must hold for every row. Drawing the line here rather than nowhere is a
judgement, so it is worth being precise about what makes it defensible: both are **boolean, row-local
and side-effect-free** — they cannot write, cannot aggregate, and cannot change what the pipeline
loads, only which rows a rule judges. A cross-field invariant like "a decline reason code exists if
and only if the status is `DECLINED`" has no non-code expression, and the alternative is a bespoke
rule type per invariant, which moves the same logic into Python where a reviewer who does not read
Python can no longer see it.

The discipline that keeps it an exception rather than a habit is that it stays **rare and visible**:
`expression` is used exactly twice in the whole control plane — `transactions.decline_reason_code`
and `disputes.resolved_date` — both at `warn` severity, and both with a `description` stating what
the invariant means. A third would be fine. A tenth would mean the rule engine is missing a rule
type, and the right response then is to add the type, not another predicate.

**The priority waves encode a real dependency, not a preference.** Reference data (5) → dimensions
(10) → facts (20) → late-arriving facts (30). `transactions` carries a `referential` DQ rule against
`dim_merchant`; run the fact before the dimension and that rule checks against a stale dimension and
quarantines valid rows — a failure that presents as bad source data and is actually an ordering bug.
That is the kind of defect metadata-driven orchestration is supposed to make impossible to introduce
by accident.

**The cost, stated plainly.** Indirection. Reading `nb_01_bronze_ingest.py` does not tell you what
happens to `transactions` — you have to read the notebook *and* the config row. For seven similar
feeds that trade is clearly worth it. For three feeds that genuinely share nothing it would not be,
and building a framework for three feeds is its own mistake.

---

## 5. Quarantine with severity, not fail-fast everywhere

**Decision.** `src/lib/dq.py` runs config-driven rules over silver. Each rule carries a severity:
`error` fails the run, `warn` records and continues. Rejected rows go to `quarantine_<entity>`
carrying the rule that caught them; every outcome lands in `meta_dq_results`.

**Why severity is per-rule and not global.** A payments feed where 0.02% of rows have a negative
amount should not stop — those rows are quarantined, the other 99.98% reach the star, and someone
looks at the quarantine table. A payments feed where `currency_code` has become an integer *should*
stop, because the shape of the data changed and every downstream number is now suspect. "Bad row" and
"broken feed" need different responses, and the only way to get both is to let the rule declare which
it is.

**Quarantine, not drop.** A dropped row is a row nobody will ever investigate. The quarantine table
carries the offending row plus the rule that rejected it, which makes "why is today's volume down"
answerable in one query instead of by re-reading bronze and guessing.

**The negative test is the one that matters.** It is easy to write a DQ framework that logs
violations and passes anyway — it looks identical to a working one on green data, and every other DQ
test in the suite would still be green. So `tests/test_silver.py` flips
`transactions.amount_minor.range` from `warn` to `error` **in the control plane** (not by patching
the engine, which would test the patch), runs the load, and asserts four things: a `DQFailure` naming
that rule, the offending rows present in `q_transactions` tagged with it, a `failed` row in
`meta_run_log`, and a `failed_run` outcome in `meta_dq_results`.

The fourth assertion is the one worth arguing for. It checks that the silver **watermark did not
advance** — because a gate that raises *after* the watermark has moved has protected nothing: the run
is red, silver is incomplete, and the next run skips the window that was never processed. That is a
worse failure than no gate at all, because it is silent. `nb_02` advances the watermark once, after
the whole batch loop, precisely so it cannot happen, and this is the test that holds it to that.

A gate that cannot be shown to fail is not a gate.

**What this does not cover.** Batch-level rules (`continuity`, `freshness`) detect a row that does not
*exist*, and a missing row cannot be quarantined. Those fail or warn at the batch, which is the only
option available — and it is worth knowing that the DQ layer's coverage of absence is structurally
weaker than its coverage of malformed presence.

---

## 6. SCD2 on account, customer and merchant — and SCD1 on card products

**Decision.** Three dimensions keep full history with half-open `[valid_from, valid_to)` intervals,
`is_current`, and hash-based change detection. `dim_card_product` is overwritten. `dim_date`,
`dim_currency` and `dim_decline_reason` are conformed reference data.

**Why those three.** `risk_band` on an account and `risk_score` on a merchant both move, and a fraud
or decline trend computed against *today's* risk band is meaningless — it attributes last quarter's
transactions to this quarter's assessment. `segment` on a customer moves for the same reason. A
transaction has to join to the dimension version that was current when it was authorised, which is a
point-in-time join and needs the history to exist.

**Why card products are not SCD2.** They change rarely and nothing analytical depends on what a
product's fee was last year. History you will never query is cost without benefit — and the honest
reason to state that out loud is that "SCD2 everything" is a common way to look thorough while adding
rows nobody reads.

**The resolution limit, which is a property of the feed and not the code.** `accounts` is a daily CDC
extract carrying `_change_ts`, so `dim_account` is accurate to the change. `customers` and `merchants`
are *monthly* snapshots, so every `valid_from` on those two is a snapshot boundary, not a real change
instant: a customer who changed on the 3rd and changed back on the 20th appears never to have changed.
A report that joins all three inherits the coarsest of them. `src/lib/scd2.py` is not the limitation —
hand it a daily snapshot and it produces daily versions — and `docs/data-contracts.md` states the
resolution per dimension so a query author can find it before a trend looks suspicious.

**The other limit.** Two changes to one key can carry the same `_change_ts`. A half-open interval
chain cannot represent two states at one instant without a zero-width version, and a zero-width
version is unreachable by any point-in-time join, so `scd2.py` collapses them deterministically rather
than writing a row no query can return. The dimension therefore preserves every *observable* state,
which is fewer than every state in the feed. That is a real loss, stated in `tests/test_scd2.py` where
someone will actually read it.

---

## 7. A seeded synthetic generator, not a public dataset

**Decision.** `nb_00_generate_landing_data.py` generates all seven feeds from a fixed seed, with
`--scale tiny|demo`, and injects specific defects at specific rates.

**Why.** The pipeline's interesting behaviours are all reactions to specific dirt: late-arriving
disputes, exact duplicates, CDC `U`/`D` operations, FX gaps, invalid enum values, and a column that
appears at month 10. No public payments dataset contains that set on demand, and a clean dataset makes
every mechanism in the repo an assertion instead of a demonstration. The generator is a feature, not
fixture setup — it is what lets the DQ gate, the SCD2 close-out and the schema-evolution path be
*shown* rather than described.

**Determinism is the load-bearing part.** Fixed seed means the same 50k rows every time, which is what
makes a test able to assert "every injected defect is quarantined" rather than "some rows were
quarantined". Defects are configured as **rates**, not counts, so the same assertions hold at `tiny`
and at `demo`.

**The schema drift is real, not simulated.** Transactions are written in two passes split at the
month-10 boundary, so files before that date genuinely lack `wallet_type`. JSON carries schema per
file, so bronze has to absorb the new column rather than being handed a null-filled one. Faking it by
writing nulls would test nothing.

**What synthetic data cannot give you.** Real distributions, real correlations, and the specific kind
of weirdness that shows up in production and that nobody would think to generate. Any performance
claim from this dataset is a claim about 50k or 2M well-behaved rows — see #10.

---

## 8. One codebase behind a context shim, not a local rewrite

**Decision.** `src/runtime/context.py` provides `get_spark()`, path/table resolution per layer, and
SQL execution. Locally these resolve to a configured `SparkSession`, `./_onelake/` paths and the gold
harness. In Fabric they resolve to the ambient `spark`, lakehouse tables and the Warehouse endpoint.
The transformation code above the shim is unmodified.

**Why.** The alternative that was actually tempting: rewrite the pipeline in DuckDB or pandas so it
runs fast on a laptop, and keep the Fabric version as a parallel set of files. That produces a repo
that demos beautifully and proves nothing, because the code that ran is not the code that would ship,
and the two drift the moment either is touched. The shim means there is exactly one implementation of
every transformation, and the substrate is a resolution detail.

**The same argument, applied to gold, is why `src/lib/gold.py` restates none of the SQL.** It reads
statements out of the deliverable `.sql` files and transpiles them. A second implementation of the
gold logic in Python would be a way for the local numbers to be right while the shipped T-SQL is
wrong, which is the exact failure the shim exists to prevent.

**What the shim does not fix.** It makes the code portable; it does not make it *verified*. Spark 3.5
locally and Spark 3.5 in a Fabric Environment are the same engine, so bronze and silver are strong
claims. Gold is not: see #1 and `docs/gold-execution.md`.

---

## 9. Spark 3.5 and Delta 3.2, pinned to match the Fabric runtime

**Decision.** `pyproject.toml` pins `pyspark==3.5.*`, `delta-spark==3.2.*`, and Python **3.11**.

**Why.** Parity with the deployment target, by choice rather than by coincidence. If the local run is
evidence about what happens on Fabric, the engine has to be the engine. Delta's version is pinned
alongside Spark's because the pairing is strict — a Delta major built against a different Spark major
fails at the protocol level, not with a helpful message.

**Why 3.11 specifically.** PySpark 3.5 does not support Python 3.12+, and the driver and workers must
agree on the minor version. This machine's default is 3.14, so `uv venv --python 3.11` is not a
preference; without it nothing runs at all.

**The cost.** A pinned old-ish Spark means no newer Spark features, and the pins go stale when Fabric
moves its runtime. That is the right trade for a repo whose entire argument is "this is what would
run there", and the staleness is visible — a version bump is a one-line diff with a reason — rather
than silent.

---

## 10. 50k and 2M rows, not 200M

**Decision.** `tiny` is 50k transactions over 120 days (the CI profile); `demo` is 2M over 18 months
(the committed dataset). Nothing in silver or gold is partitioned, `fact_transaction` included.

**Why the sizes.** `tiny` has to run the full suite in minutes on a cold CI runner. It spans 120 days
rather than 30 because *span*, not row count, is what makes the hard mechanisms reachable: a 30-day
span gives only one monthly master-data snapshot (so SCD2 has nothing to close out) and truncates the
90-day dispute lag to ~26 days (so late arrivals are never actually late). A CI profile that cannot
exercise the two hardest paths in the pipeline is not worth having. `demo` is sized to run on a laptop
in a sitting.

**Why nothing is partitioned.** At these volumes partition pruning buys less than the small-file
pressure it creates — Delta with 120 daily partitions over 50k rows is a few hundred rows per file,
and the metadata and task-scheduling overhead exceeds the scan it saves. Partitioning here would be a
scheme chosen to look thorough.

**And in gold, partitioning is not available to choose.** Fabric Warehouse does not expose
`DISTRIBUTION` or `PARTITION` in `CREATE TABLE` at all; the engine picks physical layout itself. That
is why `tools/fabric_tsql_lint.py` rejects the Synapse syntax (FB101) that a reader arriving from
dedicated SQL pools would expect to find in `03_facts.sql`. At 100× the partition question comes back
as a question about the engine's choices and the statistics it has, not about DDL you can write.

**What breaks first at 100×, in order.** This is the part worth being specific about, because "it
would scale" is not an answer:

1. **Small-file pressure in bronze — and the fix is not the one I first wrote here.** Seven feeds ×
   daily partitions × one write per batch gives 120 files of 5–28 KiB in `br_transactions` at 50k
   rows. At 100× the window it is 12,000, and the first symptom is planning time on silver reads
   growing faster than the data does.

   This item used to say the fix was *"a scheduled `OPTIMIZE` with V-Order per bronze table"*. Both
   halves were wrong, and `nb_03_table_maintenance` was written partly to find that out. `OPTIMIZE`
   bin-packs **within** a partition and never across one, so a table holding exactly one file per
   partition — which is every bronze table here — is already at the end state of bin-packing.
   Measured over the real lake, compaction removed 0 files and touched 0 partitions on all seven,
   while `meta_run_log` in the same sweep went 48 files → 1 and 200 KB → 6 KB. And V-Order is a
   Fabric write optimisation that OSS Delta rejects outright: setting
   `delta.parquet.vorder.enabled` locally raises `DELTA_UNKNOWN_CONFIGURATION`, so the notebook
   reads and reports the property rather than writing it. It would also buy nothing here, because
   no lakehouse table in this design is read by Direct Lake — gold is a Warehouse, which is #1.

   The real fix is **`partition_column` in `meta_source_config`** — one value in one config row,
   changing the grain from day to month. That is a design decision rather than an operational one,
   which is the interesting part: a maintenance schedule cannot reach it, and a maintenance job that
   reported success on `br_disputes` would have retired the problem from somebody's list while
   changing nothing. So the notebook reports those tables as an advisory naming the config column
   instead. [`docs/cost-and-capacity.md`](cost-and-capacity.md) §6 has the before/after numbers.
2. **Shuffle on the silver SCD2 merges.** The merges are keyed joins over the full dimension, so cost
   grows with dimension size and not just with the increment. The fix is to narrow the merge target
   before reaching for a bigger pool: restrict the match to open versions (`is_current = true`, or
   equivalently the `9999-12-31` sentinel — note that a `valid_to IS NULL` predicate, which is the
   reflex, would match nothing here, because #6 deliberately uses a sentinel rather than NULL), and
   Z-order the dimension files on the business key so the join can prune at file level.
3. **The decision not to partition `fact_transaction`.** Third, not first. It only starts to hurt once
   the fact table is large enough that a date-ranged query scans materially more than it needs, and by
   then the answer is about the Warehouse engine's clustering rather than about a partition column.

Stating the order matters more than the items: the instinct is to reach for partitioning first, and it
is the last of the three to actually bite.

---

Decisions 11–14 were forced by building. They are listed after the ten because that is what they are.

## 11. Idempotency is two mechanisms, not one

**Decision.** The watermark decides **what to read**. A deterministic `_batch_id`, derived from the
window being loaded rather than from the clock, decides **what a retry replaces** — the bronze write
deletes that batch id and reinserts it.

**Why both.** The watermark alone makes the happy path a no-op but cannot repair a run that died after
writing half its rows: the watermark never advanced, so the next attempt re-reads the same window and,
without the second mechanism, double-counts. A clock-based batch id would make the second mechanism
useless for the exact case it exists for, because the retry would carry a different id and append
alongside the partial write instead of replacing it.

**Why the assertion is over bronze.** `make idempotency` runs the pipeline, runs it again with
`--force-reload`, and asserts bronze is unchanged. Bronze is where a double-load is *irreparable* —
silver and gold are both rebuilt by `MERGE` from whatever bronze holds, so a duplicate there is
transient and a duplicate in bronze is permanent. All three stages still run twice, and gold
re-running unconditionally is the point rather than waste: it has no watermark, so every run reloads
the star from silver and must arrive at the same counts. A proc that was not re-runnable from the top
would show up as a row count that moved.

**The order that makes it work.** The watermark advances only after the write commits. A watermark
ahead of its data is worse than no watermark: it makes missing rows invisible rather than merely
absent.

---

## 12. No `IDENTITY` in gold, although Fabric supports it

**Decision.** Surrogate keys are assigned with `ROW_NUMBER() OVER (...) + <current max>`. No
`IDENTITY` column anywhere, and `tools/fabric_tsql_lint.py` enforces that.

**Why this is decision 12 and not decision 1.** The original plan said Fabric Warehouse has no
`IDENTITY` at all. Reading the Learn page showed that is wrong — `IDENTITY` **is** supported, with two
caveats: the `(seed, increment)` arguments are not, and the column must be `bigint`. So the constraint
I had designed around did not exist, and the decision had to be re-made on its merits.

**It came out the same way, for a better reason.** Fabric's identity allocation is not guaranteed
gapless or ordered — it is allocated per-distribution across an MPP engine, so a reload can produce
different keys for the same rows. Combine that with foreign keys being `NOT ENFORCED` metadata and the
failure mode is specific and silent: a dimension reload shifts a surrogate key, the facts still point
at the old value, nothing errors, and a fact row now resolves to a different dimension member.
Deterministic keys derived from the business key mean a reload produces the same keys, and the
reconciliation test can therefore assert something meaningful about them.

**The general lesson, which is the reason this entry exists at all.** Three other things in the
original plan were also wrong about Fabric Warehouse — `UPDATE ... FROM`, recursive CTEs, and
`@@ROWCOUNT` — and all four were found by reading the documentation while writing the linter rather
than by writing code and watching it fail. Without a tenant there is nothing to watch fail, which
makes reading the surface-area docs the only available verification.
`docs/fabric-tsql-subset.md` has the full accounting.

---

## 13. A snapshot feed's deletions are invisible to SCD2, and that is not fixed here

**Decision.** `dim_account` closes rows on a CDC `_op = D` — a logical close-out, never a hard delete.
`dim_customer` and `dim_merchant` cannot do this, because a snapshot expresses a deletion by *omitting
the row*, and nothing in the config tells silver that an absent key means a deleted one.

**Why it is not fixed.** It could be: diff each snapshot against the current dimension and close
whatever disappeared. That is a real design, and it is wrong to add by reflex, because "absent from
this snapshot" and "deleted" are not the same claim. A truncated extract, a filtered export, or a
partial upload all present as absence, and a diff-based close-out would silently retire live customers
on the strength of a broken file. Doing it safely needs a completeness signal from the source — an
expected row count, or a vendor contract that the snapshot is total — and this project has neither.

**So it is documented instead**, here and in `nb_99_seed_metadata.py`, where `op_column` is null for
every feed that has no CDC flag. The dimensions overstate what is live, by exactly the set of keys
that vanished from a snapshot. That is a known, bounded, stated inaccuracy, which is a different thing
from a bug.

---

## 14. Money is an integer in minor units, everywhere

**Decision.** Every monetary column is `*_minor` and integral — pence, cents. No floating point in a
money column at any layer, and currency is always carried alongside the amount.

**Why.** Binary floating point cannot represent 0.01, so a float pipeline accumulates error in
proportion to how much aggregation it does — which means the largest, most-reported numbers are the
least accurate, and the discrepancy appears at the end, in a total someone is looking at. The
alternative is `decimal`, which is correct but invites a silent precision change at every cast
boundary between Spark, Delta and T-SQL. An integer count of the smallest unit has neither problem and
is what payment systems actually use.

**The cost.** Every display and every rate has to divide, so a forgotten division is a number wrong by
100 — loud and immediately visible, which is the right failure mode. FX conversion is the one place
real care is needed: the rate is `decimal(18,8)`, the multiply happens in decimal, and the result is
rounded back to integer minor units **once**, at the end
(`ROUND(amount_minor * fx_rate * minor_unit_scale, 0)` in `07_sp_load_fact_transaction.sql`), rather
than at each step.

**The trap inside the decision, which is worth knowing about.** Minor units are not universal. JPY has
none, so ¥1000 is `amount_minor = 1000` and so is $10.00 — the same integer meaning two different
amounts. Multiplying either by a GBP-per-unit rate and calling the result pence is wrong for one of
them by a factor of 100, silently, in the most-reported column in the warehouse. That is why
`dim_currency` carries `minor_unit_digits` and the conversion scales by `10^(2 - digits)`, written as a
`CASE` over the three digit counts that actually exist rather than `POWER(10, ...)` — because `POWER`
returns a float, and a float has no business anywhere in the arithmetic that produces a money column.
An unexpected digit count yields `NULL` and a reconciliation failure, not a plausible wrong number.
