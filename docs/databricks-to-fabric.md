# Databricks → Fabric: what actually changes

I have built medallion lakehouses on Databricks and Spark. I have never run a Fabric tenant — the
README status box says so and this page does not pretend otherwise. What follows is the translation I
worked out while writing this repo, and it exists because *"you've only used Databricks"* is a fair
question that deserves a detailed answer rather than a reassuring one.

The unhelpful version of that answer is "they're both Spark and Delta, so it transfers." They are
both Spark and Delta, that part does transfer for free, and it is therefore the part worth the least.
What is worth talking about is the handful of places where a Databricks habit produces a design or a
line of code that is **wrong** on Fabric — and there are more of them than a feature comparison
suggests, because they cluster in exactly the areas a senior engineer owns: where the governance
boundary sits, where compute comes from, how it is billed, and what the serving layer demands of the
tables underneath it.

Every platform fact below cites the Microsoft Learn page it was read out of, with that page's
`ms.date`, so a fact with a shelf life can be found when it goes stale instead of defended from
memory. That is the convention [`docs/fabric-tsql-subset.md`](fabric-tsql-subset.md) already uses for
the linter's rules, and §10 — *five things I would have got wrong from memory* — is the section that
justifies it.

---

## 1. The table

| Databricks | Fabric | What actually changes |
|---|---|---|
| Unity Catalog | Workspace + item, with domains alongside | The access boundary is the **workspace and the item**. A domain is a label and a settings scope, not a permission. §2 |
| DBFS / Volumes / mounts | OneLake `Files/`, plus shortcuts | One logical lake per tenant. A shortcut is a symlink, not a copy — with limits and one identity surprise. §3 |
| Delta Lake | Delta Lake + V-Order | Same format, same readers. V-Order is a Fabric-specific write optimisation, and it is **off** by default in new workspaces. §4 |
| Delta Live Tables | Pipelines + notebooks, or Materialized Lake Views | MLV is the real declarative analogue, and its default on a failed constraint is the opposite of the one you'd guess. §5 |
| Workflows / Jobs | Data Factory pipelines | `Lookup → ForEach → Notebook` is how metadata-driven orchestration is expressed. §6 |
| Job clusters / instance pools | Starter pools + Environments | Starter pools are *best-effort*, not guaranteed. Library pinning and cold start trade off against each other. §7 |
| DBSQL Warehouse | Warehouse, vs. the lakehouse SQL analytics endpoint | The lakehouse endpoint is **read-only**. That single fact is why gold here is a Warehouse. `design-decisions.md` #1 |
| DBSQL dashboards | Power BI + Direct Lake | No refresh window — but the tables must satisfy guardrails, and a duplicate key fails queries outright. §8 |
| DBU | Capacity Unit, smoothed and burstable | A capacity is **shared**, throttling is per-capacity, and almost every Warehouse operation is billed as *background*. §9 |

---

## 2. Unity Catalog → the workspace, and the domain that isn't what it looks like

Unity Catalog gives you `catalog.schema.table` and a permission model attached to it. Fabric splits
those two things apart, and the split is the first thing to get straight.

**The namespace** is the item. Inside one workspace a warehouse can read
`lh_silver.dbo.dim_account` and write `wh_gold.dbo.dim_account`, and that cross-database read is what
step 6 of the deploy runbook depends on. It is also why all eleven items in
[`docs/fabric-deployment.md`](fabric-deployment.md) §3 live in **one workspace per stage** rather
than one workspace per medallion layer: a warehouse transaction may span warehouses in the same
workspace, and splitting the layers across workspaces would turn a supported cross-database query
into an unsupported one. A Unity Catalog habit pushes you straight into that mistake, because there
`bronze`/`silver`/`gold` as three catalogs is the obvious shape.

**The permission model** is the workspace role plus item permissions. Nothing else.

And **the domain is not part of it**, which is the correction worth making out loud, because the name
invites the opposite reading. A domain groups workspaces so that content can be filtered by domain in
the OneLake catalog, and it is a scope to which a few tenant settings can be delegated — currently a
default sensitivity label and the certification settings. What it does not do, in the words of the
Learn page: *domain assignment doesn't affect item visibility or accessibility*. Every user in the
tenant can see every domain defined in the tenant, whatever their domain role. So a domain is a
**label plus a settings-delegation scope**, and if you reach for one expecting a Unity Catalog
catalog's blast radius you will have built nothing.

> [Domains](https://learn.microsoft.com/en-us/fabric/governance/domains), ms.date **2025-05-01**.
> This is the oldest page cited here by ten months, which is worth knowing: of everything on this
> page, this is the claim likeliest to have moved since it was written. Subdomains are one level and
> have no admins of their own — a subdomain's admins are its parent's.

[`docs/fabric-deployment.md`](fabric-deployment.md) §3 already says the domain "carries endorsement
and discovery, not access", so the two pages agree; this one says *why* the wording was chosen that
carefully.

## 3. DBFS and Volumes → OneLake, and what a shortcut really costs

OneLake is one logical lake for the tenant, and the mount-point habit mostly evaporates: there is no
`dbutils.fs.mount`, and the thing that replaces `abfss://` juggling is a **shortcut**.

A shortcut behaves like a symbolic link and is an independent object from its target. Delete the
shortcut and the target is untouched. Delete a file *inside* a shortcut and — if you hold write
permission at the target — you have deleted it at the target. Both halves of that are worth knowing
before someone treats a shortcut as a safe read-only view; it is not one.

The specifics that constrain a design:

- Shortcuts can be created in **lakehouses and KQL databases**, not warehouses. In the `Tables`
  folder they may only sit at the **top level**; in `Files` they may sit anywhere.
- **Limits**: up to 100,000 shortcuts per item, up to 10 shortcuts per single OneLake path, and at
  most **5** direct shortcut-to-shortcut links. Names cannot contain `%` or `+`, and non-Latin
  characters are unsupported.
- **Schemas sync automatically** for shortcut tables, new and existing.
- **Caching** exists to cut cross-cloud egress, applies to GCS, S3, S3-compatible and on-premises
  gateway shortcuts, is configured per workspace with a 1–28 day retention, and skips any individual
  file over **1 GB**.
- **Identity**: an internal OneLake shortcut authorises with *the calling user's* identity, so that
  user needs read permission at the target. The exception is the one to remember — under **Direct
  Lake on SQL**, or T-SQL in **delegated identity mode**, the caller's identity is *not* passed to
  the shortcut target; the calling item owner's identity is. Direct Lake on OneLake, or T-SQL in user
  identity mode, restores passthrough.

> [Unify data sources with OneLake shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts),
> ms.date **2026-07-13**.

This repo has no shortcuts, deliberately: silver reaches gold through a cross-database read against
the lakehouse SQL analytics endpoint, not through a shortcut into the warehouse — warehouses cannot
hold shortcuts anyway. The identity note is recorded because it is the piece that would matter the
moment a real deployment reached for one, and because it interacts with §8 in a way that is not
obvious from either page alone.

## 4. Delta Lake → Delta Lake, plus the V-Order default that flipped

The format is the same, the readers are the same, and Delta written by this repo's local Spark 3.5 /
Delta 3.2 is readable by Fabric without conversion. That parity is the whole basis of the claim in
[`docs/design-decisions.md`](design-decisions.md) #9.

What is Fabric-specific is **V-Order**: a write-time optimisation — sorting, row-group sizing,
compression — inside the parquet files, producing a layout the Verti-Scan engines behind Power BI
read faster. It is spec-compliant parquet, so nothing else notices it.

The part I would have stated wrongly: **V-Order is disabled by default in all newly created
workspaces** (`spark.sql.parquet.vorder.default = false`), because the default now favours
write-heavy engineering over read-heavy serving. It costs roughly **15%** more on write, which is
why the default moved. So enabling it is a per-table decision — a read-heavy resource profile
(`readHeavyforSpark`, `ReadHeavy`), the `delta.parquet.vorder.enabled` table property, or the
`parquet.vorder.enabled` write option — and because a setting change affects only *future* writes,
changing an existing table's layout means rewriting it with `OPTIMIZE`. The older
`spark.sql.parquet.vorder.enable` session setting was **removed** in runtime 1.3, which is the exact
form I would have typed from memory.

To be straight about provenance: [`docs/architecture.md`](architecture.md) §6 already states that
default and already records it as a fact it would have got wrong, because the maintenance-gap item
there needed it. This page did not discover it. What it adds is the write cost, the two remaining
switches, and the reason the default moved — and the fact that it is worth repeating in two places is
itself the argument for §10.

> [Optimize Delta Lake tables with V-Order in Fabric](https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order),
> ms.date **2026-03-01** — the same page [`docs/architecture.md`](architecture.md) §6 cites, under
> the title that page uses.

## 5. Delta Live Tables → Materialized Lake Views

This is the row where "the closest analogue" is genuinely close, and it is the row I researched
hardest, because if MLVs did what this repo does by hand then the repo's design would need a defence.

**Materialized Lake Views** are declarative, hosted in a **Lakehouse** (not a Warehouse), authored in
Spark SQL or — in preview, and full-refresh only — PySpark. Fabric builds the dependency DAG across
lakehouses itself and orders the refresh. Quality rules are declared inline:

```sql
CONSTRAINT valid_amount CHECK (amount > 0) ON MISMATCH DROP
```

That is the nearest thing Fabric has to `src/lib/dq.py`'s per-rule **severity**, and the mapping is
worth stating precisely because it is *not* one-to-one. `ON MISMATCH FAIL` — **the default**, and it
stops the refresh — lines up with this repo's `error`. `ON MISMATCH DROP` excludes the violating
rows, counting each once, which looks like `warn` and is not: in `dq.py` a failing row is
quarantined **regardless of severity**, and severity decides only whether the *run* fails. So `DROP`
is `warn` with the quarantine deleted, and that missing half is reason 1 below. Run metrics land in
system tables under the `_mlv_system` schema — `sys_run_metrics`, `sys_node_metrics`,
`sys_error_metrics` — which is the declarative counterpart of `meta_run_log` and `meta_dq_results`.

MLVs can even read files directly, which is the declarative version of what `nb_01` hand-builds:

```sql
USING OneLake_Files OPTIONS ('format'='csv', 'path'='Files/landing/accounts', 'header'='true')
TBLPROPERTIES ('schema_mode'='DYNAMIC', 'refresh_mode'='APPEND_ONLY')
```

Each row then carries a `__filepath__` column, and `schema_mode DYNAMIC` absorbs new source columns.

**So why is this repo not built out of MLVs?** Three reasons, in order of how much they decide it:

1. **Quarantine with attribution is not what `DROP` gives you.** `dq.py` writes rejected rows to
   `quarantine_<entity>` carrying `_dq_rule_ids` — the *complete* set of rules that caught each row,
   not the first — and `meta_dq_results` records the outcome per rule per run. `DROP` drops. The DQ
   design in [`docs/design-decisions.md`](design-decisions.md) #5 exists precisely so a bad row is
   inspectable rather than merely absent, and a payments feed is the case where that distinction has
   an auditor attached to it.
2. **File-backed MLVs support CSV and Parquet only.** This repo's transactions land as **JSONL** on
   purpose — multi-format landing is part of what the generator demonstrates — so the landing ingest
   could not be expressed this way regardless.
3. **Incremental refresh requires Delta change data feed on the source tables.** Without CDF the
   refresh engine can only choose between skip and full. That is a real dependency to design in, not
   a switch, and it is the kind of thing that turns "declarative is simpler" into "declarative is
   simpler once you have enabled the right table property everywhere upstream".

The honest summary: for a greenfield silver layer with tolerable row loss, MLVs would be less code
than `nb_02` and I would use them. For a payments quarantine with rule attribution and a JSONL
source, they would not have reached the requirement. Knowing which of those two situations you are in
is the actual skill, and `ON MISMATCH FAIL` being the default is the detail that decides whether your
first MLV pipeline stops or silently thins your data.

> [Materialized lake views overview](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view),
> ms.date **2026-07-20**. Also: no DML against an MLV, `ALTER MATERIALIZED LAKE VIEW` supports rename
> only, all-uppercase schema names unsupported, and managed refresh discovers new files at a folder
> shortcut's root but not under nested shortcut folders.

## 6. Workflows → Data Factory pipelines

The shortest row, because the shape is familiar and this repo already documents the mapping in
detail. A Databricks job with a task graph becomes a Data Factory pipeline; metadata-driven
orchestration is expressed as `Lookup → ForEach → Notebook → Stored procedure`, which is exactly the
graph `orchestration/run.py` mirrors, activity for activity. The mapping is item-by-item in
[`docs/fabric-deployment.md`](fabric-deployment.md) §2, and §7 describes `pl_master` by pointing at
the file rather than restating it.

The one difference that bites: a notebook calling a notebook is `mssparkutils.notebook.run`, which
means a second Spark session and a string-serialised return value. That is why every notebook here is
one Fabric Notebook item with a single entry point and a parameter list, shared code lives in
`src/lib/`, and `tests/test_import_graph.py` fails if a notebook ever imports another.

## 7. Job clusters → starter pools and Environments

On Databricks, a job cluster and an instance pool are the two levers: one for isolation, one for
warm start. Fabric splits the same problem differently, and the split is worth getting right because
it is where cold-start latency and library reproducibility trade against each other.

**Starter pools** are the pre-warmed default: Medium nodes, autoscale and dynamic executor allocation
both on. The sentence to internalise is Learn's own description — *a Microsoft-managed, best-effort
optimization… Starter pool capacity isn't guaranteed for every run*. When pre-warmed capacity is
unavailable Fabric falls back to on-demand provisioning and the session takes longer. So "sessions
start in seconds" is a tendency, not an SLA. Node ceilings are set by SKU: **F2 and F4 cap at one
node**, F8 at 2, F16 at 4, F32 at 8, F64 at 16, and upward to 200 at F1024. An F2 is one Medium node,
which is the relevant fact for the capacity guess in
[`docs/fabric-deployment.md`](fabric-deployment.md) §3.

**Environments** are the library and runtime lever: a workspace item bundling Spark compute
configuration, libraries and resources, attachable per workspace or per notebook. Two things about
them decide a deployment:

- **Publish modes trade startup for reproducibility.** *Quick mode* publishes in about 5 seconds and
  installs libraries at session start — good for iteration. *Full mode* builds a reproducible
  snapshot, takes **3–6 minutes** to publish, and adds **1–3 minutes** to every session start for
  dependency deployment. Full mode is the one for scheduled pipelines, and the way to get both
  reproducibility and a fast start is Full mode paired with a **custom live pool**, which keeps
  dedicated clusters warm in an active window for roughly **5 second** starts. That pairing — not the
  starter pool — is where the "sessions start instantly" impression actually comes from.
- **Cross-workspace attachment is constrained.** Attaching an environment from another workspace
  requires the *same capacity and network security settings*, or the session fails to start, and the
  remote environment's compute configuration is ignored in favour of the current workspace's. Only
  the workspace admin can edit an environment that is the workspace default, and switching
  environments mid-session takes effect on the next session, never the current one.

> [Configure starter pools](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-starter-pools),
> ms.date **2026-06-15**; [Create, configure and use an
> environment](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment),
> ms.date **2026-03-25**.

## 8. DBSQL dashboards → Direct Lake, and the duplicate key that fails a query

Direct Lake is the row where Fabric genuinely offers something Databricks does not: a semantic model
that reads the Delta files directly into Verti-Scan structures with **no import and no refresh
window**. Refresh becomes *framing* — a metadata operation taking seconds.

The first correction is that "Direct Lake falls back to DirectQuery" is not one fact, it is two
modes:

- **Direct Lake on OneLake** reads one or more Fabric items' Delta tables and **does not fall back**
  at all. Exceed a guardrail and the refresh fails and the model is unqueryable.
- **Direct Lake on SQL** goes through a single source's SQL analytics endpoint for discovery and
  permission checks, and **does** fall back to DirectQuery — for SQL views, endpoint RLS, SQL-based
  granular access control, unprocessed XMLA-created tables, or an exceeded guardrail. The
  `Direct Lake behavior` model property controls it, and with fallback disabled those queries
  **fail** instead.

This repo's model is Direct Lake over `wh_gold`, so it is Direct Lake on SQL, which is why
`semantic-model/definition/model.tmdl` can set `directLakeBehavior: DirectLakeOnly` at all — and
gap 6 of [`semantic-model/README.md`](../semantic-model/README.md) chose that deliberately, on the
grounds that a performance cliff which announces itself beats one that silently doubles a report's
cost. It also explains the RLS tension documented in `05_security.sql` and
[`docs/design-decisions.md`](design-decisions.md) #3: endpoint RLS is on the fallback list, so
turning RLS on is *how* a Direct Lake query becomes a DirectQuery one.

**The second correction is the one that reaches back into the warehouse DDL.** Direct Lake requires
that **the one-side column of every relationship contain unique values, and queries fail when
duplicates are detected.** Fabric Warehouse cannot enforce a primary key —
`src/warehouse/ddl/04_constraints.sql` declares every key `NOT ENFORCED` because that is the only
form Fabric accepts. Put those two facts next to each other and the consequence is concrete: the
`unique` rule in `src/lib/dq.py`, plus
`tests/test_gold_recon.py::test_every_dimension_surrogate_key_is_unique`, are the *only* things
standing between a duplicated surrogate key and a report that errors for every user. Reading the
Direct Lake page is what added that test: the DDL header had named one cost of `NOT ENFORCED` — join
elimination going silently wrong — and the count tests could not see a duplicate that arrived
alongside a dropped row. The second cost is the louder of the two, which for once is the better
failure mode.

Other guardrails worth carrying: the parquet-file, row-group, row-count and model-size ceilings scale
by SKU (F2–F8: 1,000 files, 1,000 row groups, 300M rows, 10 GB model, 3 GB memory; F64: 5,000 /
5,000 / 1,500M / unlimited model / 25 GB; F512: 10,000 / 10,000 / 12,000M / 200 GB). Model size is
evaluated at model level, the rest per query. Complex Delta types, binary and GUID are unsupported,
strings cap at 32,764 characters, NaN is unsupported, and model-level partitions do not exist —
partition at the Delta level instead. Cross-region model creation is unsupported, with a shortcut as
the workaround, and there is no gateway support.

Note finally what Learn does **not** say: it describes well-tuned Delta tables *including V-Order
and row-group sizing* as a performance dependency of Direct Lake, not a precondition. V-Order makes
Direct Lake faster; its absence does not make Direct Lake unavailable.
[`docs/architecture.md`](architecture.md) §6 already draws that line — V-Order is what "read-heavy
Direct Lake queries benefit from" — and the overstatement is the kind that is easy to repeat
confidently, which is why both pages are careful with it.

> [Direct Lake overview](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview),
> ms.date **2026-09-02**.

## 9. DBU → Capacity Units, and why the Warehouse gets 24 hours of smoothing

A DBU is consumed by a cluster you sized. A Capacity Unit is consumed from a **capacity that
everything in the workspace shares**, and the mechanics of that sharing are the part a Databricks
background does not prepare you for.

Consumption is evaluated in **30-second timepoints** — 2,880 of them in a day — and smoothed:
interactive operations over a minimum of 5 minutes and up to 64 minutes, **background operations over
24 hours**. Bursting lets an operation exceed the SKU's rate and smoothing spreads the cost forward.
Then throttling arrives in stages, by how much future capacity has already been consumed:

| Future capacity consumed | What happens |
|---|---|
| ≤ 10 minutes | Nothing — this is *overage protection* |
| 10–60 minutes | **Interactive delay**: 20 seconds added at submission |
| 60 minutes – 24 hours | **Interactive rejection** |
| > 24 hours | **Background rejection** — everything is refused |

Overages become **carryforward**, which is paid down by **burndown** in later timepoints.

**The fact that matters most for this repo: Fabric reports almost all Warehouse operations as
*background*,** specifically so they get the 24-hour smoothing window rather than the interactive
one. Gold here is a Warehouse. So the ten stored procedures' cost is spread across a day instead of
spiking a timepoint — which is a genuine argument in favour of
[`docs/design-decisions.md`](design-decisions.md) #1 that I had not considered when making that
decision, and would not have been able to make up.

Three more with teeth. Throttling is evaluated **per capacity**, so another workspace's runaway job
throttles yours — the noisy-neighbour concern is structural, not hypothetical, and compound
throttling protection (opt-in) exists to stop one chain being throttled repeatedly across a
capacity. Capacity **overage billing costs 3× the normal rate**, which makes "just let it burst" an
expensive default.
And bursting and smoothing **do not apply at all when Autoscale Billing for Spark is enabled** — a
setting that changes the cost model rather than tuning it.

> [Throttling in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/enterprise/throttling),
> ms.date **2026-08-14**. Error signatures worth recognising: status `CapacityLimitExceeded`, *"Your
> organization's Fabric compute capacity has exceeded its limits"*, and *"Cannot load model due to
> reaching capacity limits"*.

---

## 10. Five things I would have got wrong from memory

This is the section I would most want to be asked about, because it is the actual evidence that the
pages above were read rather than recalled. Each of these is something I believed, wrote down, or
would have said in an interview, and each was wrong.

1. **"V-Order is on by default."** It is **off** in all newly created workspaces, and the session
   setting I would have reached for (`spark.sql.parquet.vorder.enable`) was removed in runtime 1.3.
   This one was already caught in [`docs/architecture.md`](architecture.md) §6 while writing the
   maintenance-gap item; it is listed here because the list would be dishonest without the one entry
   that had already cost me a correction elsewhere.
2. **"Direct Lake falls back to DirectQuery."** Only *Direct Lake on SQL* does. *Direct Lake on
   OneLake* has no fallback: a guardrail breach fails the refresh and leaves the model unqueryable.
3. **"Starter pools give you a session in seconds."** Learn calls them **best-effort** and says
   capacity isn't guaranteed for every run. The ~5-second start comes from a **custom live pool**,
   and the reproducible-library option (Full mode) *adds* 1–3 minutes to session start on its own.
4. **"A domain is a governance boundary like a Unity Catalog catalog."** Domain assignment doesn't
   affect item visibility or accessibility at all, and every user in the tenant can see every domain.
   It is a label and a settings-delegation scope.
5. **"A failed MLV constraint drops the row."** `FAIL` is the **default**, and it stops the refresh.
   `DROP` is the opt-in. I had the polarity backwards, which is the difference between a pipeline
   that halts and one that quietly thins your data.

Nothing in this list is obscure. All five are one page-read away, and all five would have been said
with confidence. That is the argument for the citation convention rather than for a better memory.

---

## 11. Sources

Every page below was read while writing this document. Dates are each page's own `ms.date`.

```
domains          .../fabric/governance/domains                              (ms.date 2025-05-01)
shortcuts        .../fabric/onelake/onelake-shortcuts                       (ms.date 2026-07-13)
v-order          .../fabric/data-engineering/delta-optimization-and-v-order (ms.date 2026-03-01)
environments     .../fabric/data-engineering/create-and-use-environment     (ms.date 2026-03-25)
starter-pools    .../fabric/data-engineering/configure-starter-pools        (ms.date 2026-06-15)
mlv              .../fabric/data-engineering/materialized-lake-views/
                    overview-materialized-lake-view                         (ms.date 2026-07-20)
throttling       .../fabric/enterprise/throttling                           (ms.date 2026-08-14)
direct-lake      .../fabric/fundamentals/direct-lake-overview               (ms.date 2026-09-02)
```

The T-SQL surface-area pages behind the Warehouse row are listed separately, next to the linter rules
they produced, in [`docs/fabric-tsql-subset.md`](fabric-tsql-subset.md).

**The standing caveat.** None of this has been executed. Every claim here is read from documentation
and reasoned about against a local implementation; none of it has been confirmed against a tenant,
and the runbook in [`docs/fabric-deployment.md`](fabric-deployment.md) §1 lists the questions I would
answer first with one. The spread of `ms.date` values above — sixteen months between the oldest and
the newest — is itself the reason the dates are recorded rather than dropped.
