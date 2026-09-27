# Cost and capacity: the arithmetic, and the one number this page does not have

Three pages defer to this one, and one of them says in advance that it will be disappointed:

| Deferred here | From |
|---|---|
| The capacity arithmetic behind "100× the data" | [`docs/architecture.md`](architecture.md) §4 |
| Whether an F2 is the right capacity, which §3 there calls a guess | [`docs/fabric-deployment.md`](fabric-deployment.md) §3 |
| *"`docs/cost-and-capacity.md` cannot fix it without a capacity metrics app and a real run"* | [`docs/fabric-deployment.md`](fabric-deployment.md) §9 item 5 |

That third row is correct, so it goes first. The shape of this page follows from admitting it.

---

## 1. The one number this page does not have

**CU-seconds per operation.** Nothing published maps a notebook run or a stored procedure to a
number of Capacity Units. The Fabric Capacity Metrics app is the source, and it reports on a real
capacity after a real run — neither of which this project has. So every statement here about *money*
would be a guess, and dressing one up as arithmetic would be worse than leaving the gap visible.

What is left is not a guess at all. **Every published guardrail is a number, and this repo's actual
footprint is a number, so the ratio between them is arithmetic.** That ratio answers a more useful
question than a CU estimate would: *which limit binds first, and at what multiple of today's data.*
It turns out to answer `architecture.md` §4 directly, and to contradict the ordering I would have
given from instinct.

**What this page does not restate.** The *mechanics* of Fabric consumption — 30-second timepoints,
interactive versus background smoothing, the four throttling stages, carryforward and burndown, the
3× overage rate — are in [`docs/databricks-to-fabric.md`](databricks-to-fabric.md) §9, written from
the Databricks side as "a DBU is not a CU". Repeating them here would create two copies of a set of
numbers that can drift apart. §9 owns the mechanism; this page owns the measurements and the
division. The one place they meet is §9 of this page.

---

## 2. The measured baseline

Numbers below are from the local lake after `make run` at `--scale tiny`, read out of the Delta
transaction logs — the **live snapshot**, not the files on disk. That distinction is not pedantry;
it is §6. The lake holds 40 Delta tables; this is the subset the rest of the page uses — all of
bronze, two of silver for comparison, and gold as its fact plus two totals.

| Layer | Table | Rows | Live files | Size | Rows/file |
|---|---|---|---|---|---|
| bronze | `br_transactions` | 50,158 | 120 | 3.26 MiB | 418 |
| bronze | `br_accounts` | 10,507 | 120 | 1.18 MiB | 88 |
| bronze | `br_fx_rates` | 920 | 115 | 0.45 MiB | 8.0 |
| bronze | `br_disputes` | 256 | 95 | 0.48 MiB | **2.7** |
| bronze | `br_customers` | 8,000 | 4 | 0.30 MiB | 2,000 |
| bronze | `br_merchants` | 2,000 | 4 | 0.07 MiB | 500 |
| bronze | `br_card_products` | 12 | 1 | 0.00 MiB | 12 |
| silver | `fact_transaction` | 49,708 | 1 | 1.93 MiB | 49,708 |
| silver | `dim_account` | 6,996 | 1 | 0.26 MiB | 6,996 |
| gold | `fact_transaction` | 49,708 | 1 | 1.85 MiB | 49,708 |
| gold | *all 11 tables* | 93,572 | 16 | 2.67 MiB | — |
| gold | *the 10 in the model* | 92,652 | 15 | **2.66 MiB** | — |

The last two rows are separate because the difference matters to §3. `gold/dbo` holds eleven tables
and the semantic model has ten: `dim_fx_rate` is joined inside the Warehouse by
`sp_load_fact_transaction` and never becomes a model table, which is also why it is the one gold
table with no surrogate key. The guardrails in §3 apply to the model, so the model's number is the
one they divide.

Two numbers from that table do the work in every section below:

- **39 bytes per row**, compressed, for the gold fact (1.85 MiB / 49,708 rows).
- **2.66 MiB** for the semantic model, across 10 tables and 15 live files, with a maximum of
  **2 live files in any one table**.

`tools/lake_footprint.py --all` prints every table on this page from the Delta logs — the baseline
above, the headroom in §3, the partition profile in §5 and the ratio in §6. The numbers here are
regenerable rather than asserted, and a reader who doubts one can run it. It derives which gold
tables are modelled from the TMDL filenames rather than holding its own list, so the split above
cannot drift from the model.

---

## 3. Which Direct Lake guardrail binds first

The model is **Direct Lake on SQL**: `semantic-model/definition/expressions.tmdl` reads
`Sql.Database("<sql-endpoint>", "wh_gold")`, the Warehouse's SQL analytics endpoint. With
`directLakeBehavior: DirectLakeOnly` in `model.tmdl`, DirectQuery fallback is off, so a breached
guardrail is an error rather than a slow query. `model.tmdl` item 1 argues for that choice; this
section prices it.

F2, F4 and F8 share one row of the guardrail table: **1,000 parquet files per table, 1,000 row
groups per table, 300 million rows per table, 10 GB maximum model size on disk.**

| Guardrail | F2 limit | Measured today | Scales with | Binds at |
|---|---|---|---|---|
| Max model size on disk | 10 GB | 2.66 MiB | data volume | **3,855×** |
| Rows per table | 300M | 49,708 (gold fact) | data volume | ~6,000× |
| Parquet files per table | 1,000 | 2 (worst table) | writes, not volume | not on this trajectory |
| Row groups per table | 1,000 | ~1 per file | row-group *size* | after the row cap, at default sizing |

The last two rows are the reason this is a table and not a list of multiples. File count does not
track data volume here: the gold load rewrites whole tables and nothing partitions them, so 11
tables stay at 1–2 files whether they hold 50 thousand rows or 50 million. And a Parquet row group
defaults to roughly a million rows, so 1,000 row groups is about a billion rows — comfortably past
the 300M row cap, meaning the row-group guardrail is never the first to bind *while row groups stay
near default size*. Both of those are assumptions with a name, and both are in §10.

**So max model size on disk binds first, at 3,855× today's data.** It is also the only one of the
four evaluated at *model* level rather than per query, so when it goes, it goes for everything
rather than for one visual.

**What that means for `architecture.md` §4's question.** At 100× the data, this model is 267 MiB of
a 10 GB allowance and 5.0M rows of a 300M allowance: **2.6% and 1.7%**. Direct Lake is not the
constraint at 100×, and it is not within two orders of magnitude of being the constraint. Go a
further order of magnitude to **1,000×** — 50 million fact rows, which is a real payments warehouse —
and the model is 2.6 GiB, still only **26%** of an F2's allowance. The SKU that cannot hold this
model is one nobody would be discussing by then.

That is worth stating plainly because it is the opposite of what I expected before doing the
arithmetic. Direct Lake guardrails are the most-discussed limit in Fabric and the first thing I
would have reached for. For a model this shape they are three orders of magnitude away, and the
thing that actually binds is compute — §7.

---

## 4. Max Memory is not a guardrail

F2's row also carries **Max memory: 3 GB**, and I read that as a fourth wall. It is not one, and
the page says so directly:

> *"For Direct Lake semantic models, **Max Memory** represents the upper memory resource limit for
> how much data can be paged in. For this reason, **it's not a guardrail**; however, it can have a
> performance impact if the amount of data is large enough to cause excessive paging in and out of
> the model data from the OneLake data."*

So memory *degrades*, and the four guardrails *fail*. That matters here more than it looks, because
it is the one place in this design where `DirectLakeOnly` does **not** convert a quiet failure into
a loud one. `model.tmdl` item 1 names the cost of `DirectLakeOnly` as *"on a capacity under memory
pressure, a report that would have degraded now breaks"* — the worry is right, but the mechanism is
paging, and paging is not fallback. Disabling fallback does nothing to paging: a memory-pressured
report on `DirectLakeOnly` still degrades quietly, exactly as it would on `Automatic`.

The two failure modes want different fixes, which is why keeping them apart is worth a section: a
guardrail breach needs smaller or better-organised tables, and paging pressure needs a larger SKU or
a narrower model. Reaching for `OPTIMIZE` when the problem is paging is a wasted afternoon.

---

## 5. The small-file finding, and a correction to `architecture.md` §4

Bronze is partitioned by `ingest_date` and writes one file per partition per run. Measured, the
live snapshot has 120 partitions — 120 days — and file *count* therefore tracks **days ingested,
not rows**:

| Table | Rows | Live files | Partitions | Rows/file | KiB/file |
|---|---|---|---|---|---|
| `br_disputes` | 256 | 95 | 95 | **2.7** | 5 |
| `br_fx_rates` | 920 | 115 | 115 | 8.0 | 4 |
| `br_accounts` | 10,507 | 120 | 120 | 87.6 | 10 |
| `br_transactions` | 50,158 | 120 | 120 | 418.0 | 28 |
| `br_customers` | 8,000 | 4 | 4 | 2,000.0 | 78 |
| `br_merchants` | 2,000 | 4 | 4 | 500.0 | 18 |
| `br_card_products` | 12 | 1 | 1 | 12.0 | 5 |

Live files equals partitions in all seven rows, which is the fact the rest of this section turns on.

`architecture.md` §4 predicts that at 100× the data, the first thing to break is small-file pressure
in bronze. The measurement says that ordering rests on something the phrase "100× the data" hides,
and that in one of its two readings the prediction points the wrong way:

- **100× the rows, same window.** Every file grows about 100×. `br_transactions` goes from 28 KiB to
  roughly 2.8 MiB per file — *closer* to a healthy Parquet file, not further. Small-file pressure
  improves.
- **100× the window, same daily rate.** 120 partitions become 12,000, each still about 28 KiB. This
  is the failure §4 means, and it arrives from the calendar rather than from volume.

Both are "100× the data" and they move bronze's file profile in opposite directions. §4 is right
about *which component* gives first and wrong about what drives it — and because the driver is
elapsed time, the pressure is not a future problem at all. **2.7 rows per file in `br_disputes` is
visible today, at 50 thousand rows.** It is not a scale symptom; it is a design consequence that was
there on day one.

This sharpens [`docs/fabric-deployment.md`](fabric-deployment.md) §9 item 6, which names the missing
`OPTIMIZE`, `VACUUM` and `nb_03_table_maintenance` as the honest first gap. The refinement: the
trigger for compaction in this design is **calendar time, not row count**, and the number to watch
is partitions per table rather than gigabytes. A pipeline that ingests daily needs compaction on a
schedule even if its volume never grows at all.

---

## 6. What `VACUUM` would reclaim, measured

Every bronze table holds **exactly twice** the parquet files its current snapshot references:

```
br_transactions   240 on disk / 120 live   = 2.0x
br_accounts       240 on disk / 120 live   = 2.0x
br_fx_rates       230 on disk / 115 live   = 2.0x
br_disputes       190 on disk /  95 live   = 2.0x
br_customers        8 on disk /   4 live   = 2.0x
br_merchants        8 on disk /   4 live   = 2.0x
br_card_products    2 on disk /   1 live   = 2.0x
```

Across the whole lake it is 617 live files against 1,179 on disk, or 1.91×.

Seven tables, the same ratio to one decimal place in all seven, and the cause is knowable rather
than mysterious: `make run` was executed twice, the batch-level rerun in `nb_01_bronze_ingest`
deletes and reinserts the batch it is replacing, and the removed files stay on disk until a `VACUUM`
this repo never runs. The idempotency guarantee is real, and this is its storage cost.

So the missing maintenance notebook currently carries **100% storage overhead on bronze**, and that
number is measured rather than inferred. What it *costs* is a different question: OneLake storage
and capacity compute are billed on different bases, and rather than state a billing treatment from
memory I will say that this is the one figure on this page that a pricing page could settle without
a tenant — and that I have not read it, so the overhead is quantified and its price is not.

---

## 7. The compute wall: an F2 is one node

`fabric-deployment.md` §3 guesses an F2 and labels the guess. The arithmetic says the guess is wrong
for a reason that has nothing to do with the semantic model. Starter pools use **Medium nodes only**:

| SKU | Capacity units | Spark VCores | Default max nodes | Max nodes |
|---|---|---|---|---|
| F2 | 2 | 4 | 1 | **1** |
| F4 | 4 | 8 | 1 | 1 |
| F8 | 8 | 16 | 2 | 2 |
| F16 | 16 | 32 | 3 | 4 |
| F32 | 32 | 64 | 8 | 8 |
| F64 | 64 | 128 | 10 | 16 |

An F2 is **one Medium node and four Spark VCores.** Autoscale and dynamic executor allocation are
both on by default, and on a one-node ceiling they have nothing to do. Seven entities through bronze
and three SCD2 merges run on that one node or they do not run.

At `tiny` scale that is genuinely fine, and an F2 is the right *CI* capacity. At `demo` scale — 2M
transactions across 18 months — one Medium node is the thing to worry about, not a 10 GB model
allowance with 3,855× of headroom. **The binding constraint on this repo is Spark compute, and the
semantic model is nowhere near a limit.** That inverts the order I would have put those two in
before measuring, and it is the most useful sentence on this page.

One more thing the same page corrects. Starter pools are *best-effort*:

> *"Starter pools are a Microsoft-managed, **best-effort** optimization that reduces Spark startup
> time by using pre-warmed capacity. Starter pool capacity isn't guaranteed for every run."*

The famous ~5-second start is a **custom live pool**, which keeps dedicated clusters warm during an
active window you control — not the starter pool. For a scheduled nightly run that is the difference
between a predictable window and a variable one, and it is a provisioning decision rather than a
tuning one.

---

## 8. What I would provision, and which half stays a guess

| Scenario | Capacity | Basis |
|---|---|---|
| CI, `tiny` scale | **F2** | One node is enough; the build needs to be green, not fast |
| Demo, `demo` scale | **F8–F16** | 2–4 nodes. Which of the two is a CU question — see below |
| The semantic model | not a factor | 3,855× headroom; it does not enter the decision at either scale |

The middle row is where arithmetic runs out. Choosing between an F8 and an F16 is a question about
CU-seconds per run, and CU-seconds per run is §1 — the number this page does not have. What I can
say is which direction the uncertainty points: the Spark stage is the spiky one, so the risk is
under-provisioning compute, not storage or model size.

The bottom row is the one worth having ready in conversation, because the instinct — mine included,
before §3 — is to size the capacity on the semantic model. For this workload that instinct is wrong
by three orders of magnitude.

---

## 9. Where smoothing changes the answer

The mechanism is [`docs/databricks-to-fabric.md`](databricks-to-fabric.md) §9. The single fact that
matters to the sizing above: Fabric reports almost all **Warehouse** operations as *background*, so
they smooth over 24 hours, while Spark notebook work smooths over minutes. Gold here is a Warehouse,
so the ten stored procedures are the flattest part of this system's consumption profile, and the
Spark notebooks are the spiky part.

Which is unhelpful in a specific way: the spiky component is also the one behind the one-node ceiling
in §7. Both constraints point at the same place. At least it is one component to fix rather than two.

---

## 10. What would falsify this page

**1. A real run on a real capacity with the Capacity Metrics app.** Everything in §8's demo row is a
guess and would be replaced rather than refined. This is the item that makes the rest provisional.

**2. 39 bytes per row not holding.** It is measured on 50k rows of *synthetic* data with deliberately
limited cardinality. Real payments data — more distinct merchants, free-text descriptors, wider
currency coverage — compresses worse, and every multiple in §3 moves with it. The direction is known
and the magnitude is not, which is the honest form of that caveat.

**3. Gold gaining partitions.** §3's claim that file count does not scale with volume holds only
while the gold load rewrites whole tables. Partition gold by month and files per table start tracking
the calendar exactly as bronze's do in §5, which would move the file guardrail from "not on this
trajectory" into the ranking.

**4. Row-group sizing drifting from the default.** §3 dismisses the 1,000 row-group guardrail on the
assumption of roughly million-row groups. Write small row groups and it becomes the *first* guardrail
to bind rather than the last — a reordering caused by a writer setting, not by data.

**5. The guardrail tables changing.** They are per-SKU and dated. The numbers in §3 and §7 are what
the pages below said on the `ms.date` recorded there, and a capacity table is exactly the sort of
thing that gets revised upward quietly.

---

## Sources

| Page | `ms.date` | Used for |
|---|---|---|
| `learn.microsoft.com/fabric/fundamentals/direct-lake-overview` | 2026-09-02 | §3 guardrails, §4 Max Memory |
| `learn.microsoft.com/fabric/data-engineering/configure-starter-pools` | 2026-06-15 | §7 nodes, VCores, best-effort |
| `learn.microsoft.com/fabric/enterprise/throttling` | 2026-08-14 | §9, via `databricks-to-fabric.md` §9 |

Measurements in §2, §3, §5 and §6 were read from the Delta transaction logs in `./_onelake/` after
`make run` at `--scale tiny`; `tools/lake_footprint.py --all` regenerates all four. Where a Learn
page and a measurement disagree, the measurement is about this repo and the page is about the
platform; §5 is the one section where that distinction does real work.

*No figure on this page came from a live Fabric capacity. §1 says which number is missing because of
that, and §10 item 1 says what would replace it.*
