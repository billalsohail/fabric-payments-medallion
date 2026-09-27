# Deploying this repo to Microsoft Fabric

> ## UNVALIDATED
>
> **No part of this document has been executed against a Fabric tenant.** There was no tenant: the
> Fabric trial requires a work or school account and the Gmail signup path is closed, which is the
> constraint the whole repo is built around ([`README.md`](../README.md)). Every platform statement
> below is sourced from current Microsoft Learn pages, cited inline with the page it came from, and
> **none of it is sourced from experience**. Where a Learn page left a question genuinely open I have
> said so and named the question rather than resolved it by guessing — see §9, which is the section
> to read if you only read one.
>
> Treat this as a design and a runbook, reviewable as code. The distinction between "designed" and
> "verified" is the only thing this repo asks to be trusted on, and it is not worth spending here.

---

## 1. The runbook: the first two hours on a real tenant

### The two questions this runbook exists to answer

Everything else here is provisioning. These two are the only things in the repo that **cannot** be
answered off-tenant, and they are the reason the first two hours are worth planning in advance:

1. **Does any gold statement spill?** `tools/fabric_tsql_lint.py` proves the T-SQL is inside the
   Fabric Warehouse surface area and `src/lib/gold.py` proves the logic produces the right numbers,
   but neither can see a query plan. Fabric Warehouse is a distributed MPP engine; a join that is
   fine on one node is a shuffle on eight, and a plan that exceeds memory spills to disk rather than
   failing, so the symptom is a load that gets slower with volume and never errors. The statement
   most likely to show it is `07_sp_load_fact_transaction.sql`'s surrogate-key resolution — six
   dimension lookups against the staged fact in one statement.
2. **Is `MERGE` distributed the way the procs assume?** Six of the ten procs MERGE, and a `MERGE`
   against a table distributed on a column other than its merge key is a full data movement per
   statement. Fabric Warehouse does not expose `DISTRIBUTION` in `CREATE TABLE` the way dedicated
   SQL pools did — it decides, and `tools/fabric_tsql_lint.py` rejects the syntax (FB104) for that
   reason. So this is not something the DDL can assert; it is something the plan has to be read for.

Both are answered by the same technique and the technique is the first thing to learn on the tenant:
run the statement, then read the actual plan and the distributed statistics, not the estimate.

**What "answered" looks like.** Question 1 is answered when I can point at the plan for the fact
load and say whether it spilled. Question 2 is answered when I can say which column Fabric chose to
distribute `fact_transaction` on and whether `07_sp_load_fact_transaction.sql`'s merge key matches
it. If the answer to either is bad, the fix is a DDL or a proc change in this repo, reviewed and
tested locally the same way everything else was — which is the point of having the local harness at
all.

### Why they are questions 1 and 2 but not steps 1 and 2

You cannot read a query plan for a warehouse that does not exist yet. Steps 1–6 below are the
minimum provisioning that makes those two questions *askable*, and they are ordered so that the
answering happens as early as it mechanically can — step 7, roughly forty minutes in, before any
semantic model, report, pipeline or second workspace exists. Nothing before step 7 is there for its
own sake. If the two hours run out at step 8, the trip was still worth it.

### The steps

**Step 1 — capacity and a single workspace (5 min).** One F2 (or a trial capacity) and one workspace,
`ws-payments-dev`. Do **not** create the dev/test/prod set yet: the number and names of deployment
pipeline stages are permanent once the pipeline is created
([intro-to-deployment-pipelines](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)),
so that decision is worth making after §5 has been read on a real tenant rather than before.

**Step 2 — the four lakehouses (5 min).** `lh_bronze`, `lh_silver`, `lh_meta`, `lh_quarantine`. Names
matter and are not cosmetic: `src/runtime/context.py` composes them as
`f"{cfg.lakehouse_prefix}{layer.value}.{name}"`, so the item names *are* `lh_` + the `Layer` enum
value. A lakehouse named anything else means `FABRIC_LAKEHOUSE_PREFIX` has to change, which is a
Variable Library entry (§6), not a code change — but check the names first, because getting them
right is free and renaming a lakehouse later is not.

**Step 3 — `wh_gold` (5 min).** One warehouse. **Check its collation before anything else touches
it**: all four Fabric CI/CD workflows are unsupported across warehouses with different collations
([data-warehouse/git-integration](https://learn.microsoft.com/fabric/data-warehouse/git-integration)),
so a dev warehouse and a prod warehouse that disagree on collation is a dead end discovered late.
Microsoft ships a remedy script for the case where it has already happened
(`scripts/dw-collation-error-update-tmsl/pbi_interactive.py` in `microsoft/fabric-toolbox`), which is
a good indication of how often it does.

**Step 4 — upload landing data, then run bronze and silver (15 min).** Upload
`_onelake/files/landing/` into `lh_bronze`'s `Files/` area as `Files/landing/`. Import
`src/notebooks/*.py` as notebooks; **attach `lh_bronze` as each notebook's default lakehouse**. That
attachment is load-bearing and is the subject of the one deployment rule Fabric offers for notebooks
(§5): `landing_path()` returns a *relative* `Files/landing/<entity>` on Fabric, and a relative Files
path resolves against the notebook's default lakehouse. Wrong default lakehouse, no landing data,
and the error will be a file-not-found rather than anything that names the cause.

Then set `FABRIC_ENV` — actually, do not: `detect_env()` finds `notebookutils` and resolves to Fabric
on its own inside a Fabric kernel. Run `nb_99_seed_metadata`, then bronze and silver for all seven
feeds. If they run, the shim's claim is true, and that is the second-most-interesting result of the
whole trip.

**Step 5 — DDL into `wh_gold` (5 min).** Run the files in `src/warehouse/ddl/` in filename order
from the Fabric SQL query editor, with one exception: **not `05_security.sql`** — see step 10, where
it is deliberately last and deliberately separate. This is the first time any of it has met a T-SQL
engine, so expect to learn something here; whatever fails, fails in a way
`tools/fabric_tsql_lint.py` should have caught, and the linter gets a new rule and a new test the
same day.

**Step 6 — the staging fill, which is the only code that changes (5 min).** `stg.*` is the single
seam between substrates and `src/warehouse/ddl/06_staging.sql` says so in its header. Locally it is
filled by the bridge in `src/lib/gold.py`; on Fabric it is filled by cross-database query against the
lakehouse SQL analytics endpoint:

```sql
INSERT INTO stg.dim_account_silver SELECT * FROM lh_silver.dbo.dim_account;
```

**This is the only step in the repo that disappears on Fabric**, and `06_staging.sql`'s three
arguments for staging at all — snapshot stability, one seam rather than seven, and a diagnosable
intermediate state — are the reason the seam was put there instead of having each proc read
`lh_silver.dbo.*` directly under a conditional. Verify the cross-warehouse read actually works
before trusting the rest: a transaction may span warehouses in the same workspace including reads
from a lakehouse endpoint, which is what makes this a supported read and not a copy job, and it is
the single Learn claim in this document that the whole gold layer's portability rests on.

**Step 7 — run the ten procs, then answer questions 1 and 2 (20 min).** `00`–`09` in order, which is
the order `orchestration/run.py`'s `run_gold_stage` already encodes. Then read the plans. This is the
point of the exercise.

**Step 8 — reconcile (10 min).** `tests/test_gold_recon.py`'s assertions, run as SQL against
`wh_gold`: bronze = silver + quarantined + deduped, silver ties to gold, and
`agg_merchant_daily` ties to `fact_transaction` at day grain. If the numbers match the local run,
the two substrates agree on every transformation in the repo and the `gold.py` harness was honest.
If they do not, the difference is the most valuable single output of the day.

**Step 9 — the semantic model (20 min).** Import `semantic-model/` as a Direct Lake model over
`wh_gold`'s SQL analytics endpoint, then paste `semantic-model/measures.dax` into DAX query view and
run it. That file exists for this moment: it is a complete `DEFINE` + `EVALUATE` returning all 36
measures at the grand total, generated from the TMDL by `tools/extract_dax.py`, so the first measure
that fails to resolve names itself. Several measures return BLANK correctly and
`semantic-model/measures.dax` says which — a guarded measure returning BLANK is not a failure.

**Step 10 — security, last and by itself (15 min).** `05_security.sql` needs a step of its own for a
documented reason: **SQL permissions are not carried by Fabric git integration.** The Learn page is
explicit that "SQL security features such as permissions require a script-based approach for export
and migration", and that pre- and post-deployment scripts added to the project through Git are not
preserved
([data-warehouse/git-integration](https://learn.microsoft.com/fabric/data-warehouse/git-integration)).
So the grants cannot ride along with the schema and there is no way to make them; they are a script
someone runs, and the runbook is where that fact has to live.

Replace the `@bank.example` placeholders with real Entra principals first — the file ships with
none, deliberately (see its header). Then read the header again before enabling RLS, because
**enabling RLS on the detail star gives up Direct Lake**: Power BI queries against a warehouse fall
back from Direct Lake to DirectQuery to honour row-level security. `05_security.sql` argues the
middle path — RLS on the detail star, Direct Lake preserved on `agg_merchant_daily`, which has no
region column and therefore nothing to filter. That trade is a decision to make on the tenant with
the audience in the room, not one to have already made in a file.

**Step 11 — git integration, once something works (10 min).** Connect `ws-payments-dev` to this
repository. Deliberately last: an item that does not work yet, committed, is a wrong definition in
source control, and §4 is where the reasons this step is more complicated than it sounds are set out.

---

## 2. Item inventory

Every Fabric item this repo implies, and the files it comes from. **This lists what exists.** The two
rows that say "does not exist" mean it, and nothing else in the table is aspirational — a row is here
because a file in this repo would become that item.

| Fabric item | Type | Source in this repo |
|---|---|---|
| `lh_bronze` | Lakehouse | `Layer.BRONZE`; also holds `Files/landing/` |
| `lh_silver` | Lakehouse | `Layer.SILVER` |
| `lh_meta` | Lakehouse | `Layer.META` — the 6 control-plane tables |
| `lh_quarantine` | Lakehouse | `Layer.QUARANTINE` — `q_<entity>`, `src/lib/dq.py` |
| `wh_gold` | Warehouse | `src/warehouse/ddl/` + `src/warehouse/procs/` |
| `nb_00_generate_landing_data` | Notebook | `src/notebooks/nb_00_generate_landing_data.py` |
| `nb_01_bronze_ingest` | Notebook | `src/notebooks/nb_01_bronze_ingest.py` |
| `nb_02_silver_transform` | Notebook | `src/notebooks/nb_02_silver_transform.py` |
| `nb_03_table_maintenance` | Notebook | `src/notebooks/nb_03_table_maintenance.py` — scheduled, **not** in `pl_master` |
| `nb_99_seed_metadata` | Notebook | `src/notebooks/nb_99_seed_metadata.py` |
| `pl_master` | Data pipeline | `orchestration/run.py` — see §7 |
| `sm_payments` | Semantic model | `semantic-model/` (TMDL, Direct Lake) |
| `rpt_payments` | Report | **Does not exist.** `dashboard/` is a static stand-in |
| `env_payments` | Environment | **Does not exist.** See below |
| `vl_payments` | Variable library | Designed in §6; no item file |

Fifteen rows, twelve of them real, which is well inside the 300-item-per-deployment limit
([understand-the-deployment-process](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/understand-the-deployment-process)).

**Two absences worth naming rather than leaving as gaps in a table.**

`env_payments` — a Fabric Environment pins Spark libraries and configuration per workspace. This
repo needs none: `src/lib/` is imported from the repo itself and the notebooks use nothing outside
PySpark, Delta and the standard library. `pyproject.toml` pins `pyspark==3.5.*` / `delta-spark==3.2.*`
specifically to match the Fabric runtime pairing, which is the version parity an Environment would
otherwise have to enforce. If `src/lib/` were ever packaged as a wheel instead of being imported as
source, an Environment is where the wheel would be attached, and that is the version of this repo
where the item becomes necessary.

`rpt_payments` — there is no `.pbir`, no page layout and no visual, because Power BI Desktop is
Windows-only and this is an arm64 Mac. `dashboard/build_dashboard.py` reads gold and emits static
HTML covering 34 of the 36 measures, and it is labelled a stand-in everywhere it is mentioned. Worth
knowing before attempting one: **PBIR-format reports are not supported by deployment pipelines**
([understand-the-deployment-process](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/understand-the-deployment-process)),
so the report format a new build would default to is the one that does not deploy.

---

## 3. Workspace and domain layout

```
Domain: Payments                       ← a label and a settings scope, not a boundary
  └── ws-payments-dev      (F2 / trial)   all 11 items, git-connected
  └── ws-payments-test     (F2)           deployment pipeline stage 2
  └── ws-payments-prod     (F64)          deployment pipeline stage 3
```

**One workspace per stage holding all eleven items, not a workspace per layer.** The alternative —
`ws-bronze`, `ws-silver`, `ws-gold` — is the shape a Databricks or Unity Catalog habit suggests, and
it is wrong here for a specific mechanical reason rather than a stylistic one: a warehouse
transaction may span warehouses **in the same workspace**, including reads from a lakehouse SQL
analytics endpoint. Split the layers across workspaces and step 6's `INSERT INTO stg.* SELECT ...
FROM lh_silver.dbo.*` stops being a supported cross-database read. The gold layer's portability
claim depends on the workspace layout, which is not obvious and is the kind of thing worth having
found in the docs rather than in a failed deploy.

The domain is where a Unity Catalog habit misleads hardest, and it is the entry in
[`docs/databricks-to-fabric.md`](databricks-to-fabric.md) §2 most likely to be asked about. It
carries endorsement and discovery, not access; access is per-workspace and per-item. Note what that
rules out: Learn is explicit that *domain assignment doesn't affect item visibility or
accessibility*, and every tenant user sees every domain whatever their role, so it is not a
boundary in any sense a Unity Catalog catalog would suggest. It groups workspaces for discovery, and
gives two tenant settings — a default sensitivity label and certification — somewhere narrower to be
set from. It is a label plus a settings-delegation scope, and nothing in it is a permission.

Capacity sizes are a guess. An F2 is enough for the `tiny` scale CI runs on and almost certainly not
enough for the `demo` scale at 2M transactions; sizing needs a real run and a real capacity metrics
app, which is exactly the observation `docs/cost-and-capacity.md` exists to make and cannot make
without a tenant either.

---

## 4. Git integration, and the one place it does not fit

Fabric git integration stores each item as a directory named `{display name}.{type}` containing a
`.platform` descriptor plus the item's definition files
([intro-to-git-integration](https://learn.microsoft.com/fabric/cicd/git-integration/intro-to-git-integration)).
Warehouse, Lakehouse, Notebook, Environment, Spark Job Definition, Data pipeline and Variable library
are all supported; **semantic model and report are still (preview)**. Providers are Azure DevOps,
GitHub and GitHub Enterprise, cloud-hosted only.

This layout now exists, and it is split in two. `fabric/items/` holds what is **committed**;
`make fabric-build` renders the full git-integration tree into `fabric/build/`, which is
**gitignored**. [`fabric/README.md`](../fabric/README.md) is that directory's own account of itself
and goes further than this section does.

```
fabric/items/                        committed — only what cannot be derived
  lh_bronze.Lakehouse/.platform
  lh_silver.Lakehouse/.platform
  lh_meta.Lakehouse/.platform
  lh_quarantine.Lakehouse/.platform
  nb_00_generate_landing_data.Notebook/.platform
  nb_01_bronze_ingest.Notebook/.platform
  nb_02_silver_transform.Notebook/.platform
  nb_03_table_maintenance.Notebook/.platform
  nb_99_seed_metadata.Notebook/.platform
  sm_payments.SemanticModel/.platform
  pl_master.DataPipeline/.platform
  vl_payments.VariableLibrary/{.platform, variables.json, settings.json, valueSets/{test,prod}.json}

fabric/build/                        generated by `make fabric-build`, gitignored
  <the same twelve directories>
  + notebook-content.py per notebook       rendered from src/notebooks/*.py
  + definition.pbism, definition/          rendered from semantic-model/
  - no pipeline-content.json               synthesising one is refused; §7 points at run.py instead
```

Twelve items, and **no `wh_gold.Warehouse/` at all** — that absence is the subject of the next
subsection and it is enacted, not merely argued: `tests/test_fabric_items.py` fails if the directory
appears.

Only two things are committed, on the rule that a file belongs in git here only if it cannot be
derived from something the repo already holds. A `logicalId` cannot: it is a GUID that must stay
stable across deploys. The Variable Library cannot: §6 below is a table in prose, and prose is not
what a tenant reads. Notebooks and TMDL both *can* be derived — they already exist in
`src/notebooks/` and `semantic-model/definition/` — so committing a second copy under `fabric/items/`
would create five notebook pairs and fourteen TMDL pairs with nothing keeping them in agreement. The
git-integration layout is therefore treated as a deployment artefact, on the same argument that makes
`semantic-model/measures.dax` generated and drift-checked rather than hand-maintained.

The cost of that, stated plainly: you cannot point Fabric git integration at this repository, because
the layout it wants is not in git. This repo takes the code-first path in §7 instead.

`.platform` is schema version 2.0, carrying `config.logicalId` (a GUID) and `metadata.type`,
`displayName`, `description`. **`type` is case-sensitive**, and copying an item directory to make a
second item requires changing both the `logicalId` and the `displayName` — a duplicated logical id is
the failure mode most likely to be produced by hand-authoring, which is one more reason `fabric/`
carries the UNVALIDATED label.

`semantic-model/` still has no `.platform` of its own, and that remains correct rather than
outstanding: the descriptor belongs to the deployment layer, so it lives in
`fabric/items/sm_payments.SemanticModel/` and `make fabric-build` assembles the two into an item.
[`semantic-model/README.md`](../semantic-model/README.md) raised this as gap 2 and now records where
it went.

### The warehouse does not fit, and this is the most important paragraph in the document

**Fabric git integration does not store hand-authored SQL scripts for a warehouse.** It commits the
warehouse as a **SQL database project**: the schema is extracted into individual `.sql` files and
synchronized by DacFx-based incremental schema deployment
([data-warehouse/git-integration](https://learn.microsoft.com/fabric/data-warehouse/git-integration)).
So `src/warehouse/ddl/*.sql` and `src/warehouse/procs/*.sql` — hand-written, ordered by filename,
readable top to bottom, linted by `tools/fabric_tsql_lint.py` — are **not** the git-integration
format and cannot be dropped into `wh_gold.Warehouse/` and synced.

That is a genuine fork in the road and the repo picks a side:

**The repo's scripts stay authoritative.** They are executed against the warehouse (step 5), and
whatever DacFx extracts afterwards is a *derived* artefact — the same relationship
`semantic-model/measures.dax` has to the TMDL, and the same argument: the copy a human edits is the
copy that has to win, and a generated copy that can drift is only safe when something fails on
drift. The alternative — maintain a `.sqlproj`, deploy with SQLPackage or the DacFx pipeline tasks
([development-deployment](https://learn.microsoft.com/fabric/data-warehouse/development-deployment))
— is the right answer for a team with a warehouse and no prior scripts, and the wrong answer here,
because it would make the ten procs a generated view of a project file and destroy the thing this
repo is actually for: SQL a reviewer can read in order.

The cost of that choice, stated: `wh_gold` is deployed by **executing scripts**, not by syncing git,
and so the warehouse is the one item in the inventory whose deployment is imperative rather than
declarative. Three further limitations from the same page are worth knowing before relying on any of
it: there are no selective commits below item level; SQL analytics endpoints have no version control
at all; and an out-of-date `.sqlproj` pinning an old `Microsoft.Build.Sql` SDK fails on newer syntax
including `IDENTITY` and `CLUSTER BY`.

### The lakehouses fit, but only their metadata

**"Only metadata is tracked — git and deployment operations never overwrite data in tables or
files"**
([lakehouse-git-deployment-pipelines](https://learn.microsoft.com/fabric/data-engineering/lakehouse-git-deployment-pipelines)).
Tables, Spark views and `Files/` folders are neither tracked nor overwritten. What *is* tracked: the
display name, description and logical GUID, SQL analytics endpoint metadata, `shortcuts.metadata.json`
and `data-access-roles.json` (preview), opt-in per object type through `alm.settings.json`.

Two consequences that matter for this repo:

- **A deployed lakehouse arrives empty.** "A new empty lakehouse with the same name is created in the
  target workspace. Notebooks and Spark Job Definitions are remapped to reference the new lakehouse."
  So promoting dev to test does not promote the data, and `Files/landing/` has to be uploaded again
  per stage — or, better, the landing feed becomes a shortcut and the shortcut is what deploys.
- **A deployment overrides the target's shortcut state**, and internal OneLake shortcuts are
  auto-remapped to the target workspace while external ones keep their original targets unless a
  Variable Library remaps them. Auto-remapping does not *create* the target, so a remapped shortcut
  can point at something that does not exist yet.

---

## 5. The deployment pipeline: dev → test → prod

Three stages, which is the default, chosen mostly because **the number and names of stages are
permanent after creation** and 2-to-10 is the range
([intro-to-deployment-pipelines](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)).
Three is the fewest that lets prod be promoted from something that was tested rather than from
something that was written.

**Pairing is by item name and type, and unpaired items do not merge.** Two items that look identical
but were never paired produce a duplicate on deploy rather than an overwrite, and items added after a
workspace is assigned are not paired automatically. Pairing survives renames, which is the useful
half of the same rule. Autobinding across workspaces matches by **numeric stage index, not display
name**, and both pipelines must have the same number of stages — so a stage called "test" in one
pipeline binds to whatever is second in another, whatever it is called.

### Deployment rules

Rules are defined **in the target stage** and cannot be created in the development stage, and you
must own the item
([create-rules](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/create-rules)). Only
five item types accept rules at all, and of those, this repo's inventory intersects two:

| Item | Rule available | Used here for |
|---|---|---|
| Notebook | default lakehouse | Repointing `Files/landing/` per stage — step 4's attachment |
| Semantic model | data source, parameter | Rebinding Direct Lake — see below, and §9 |

The notebook rule is the one that carries real weight, and it is the mechanical reason step 4 insists
on getting the default lakehouse right: `landing_path()` returns a relative `Files/landing/<entity>`,
so the notebook's default lakehouse *is* the landing location, and the default-lakehouse rule is the
only supported way to make that differ per stage.

Three ways a rule fails that are worth knowing in advance: if a rule's data source or parameter is
changed or removed in the **source** stage, the deployment **fails**; rules are lost if a workspace is
unassigned and reassigned; and a rule is deleted with its item. Parameter rules additionally require
the parameter to be of type `Text`.

### The Direct Lake rebinding trap

This is the gotcha to lead with, because it applies to this repo exactly and its symptom is not an
error:

> "When you deploy a Direct Lake semantic model, it doesn't automatically bind to items in the target
> stage. For example, if a lakehouse is a source for a Direct Lake semantic model and they're both
> deployed to the next stage, the Direct Lake semantic model in the target stage still binds to the
> lakehouse in the source stage. Use datasource rules to bind it to an item in the target stage.
> Other types of semantic models automatically bind to the paired item in the target stage."
> — [understand-the-deployment-process](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/understand-the-deployment-process)

Every partition in `semantic-model/definition/tables/*.tmdl` is `mode: directLake`, so this is not a
hypothetical. Deploy dev to test and the test model reads **dev's** warehouse: the report works, the
numbers are plausible, and they are the wrong environment's numbers. A test stage silently reporting
production-candidate figures from dev is worse than a test stage that is broken, because nothing
announces it.

The open question this leaves is in §9, and it is a real one rather than a rhetorical one.

---

## 6. Variable library

`src/runtime/context.py`'s `RuntimeConfig` docstring already says it "mirrors a Fabric Variable
Library entry set", and this is that mirror made explicit. A Variable Library holds one value set per
stage with one active at a time; the active set is item **configuration** rather than **definition**,
so it does not appear in the deployment comparison and is not overwritten on each deploy.

| Variable | dev | test | prod | Read by |
|---|---|---|---|---|
| `FABRIC_LAKEHOUSE_PREFIX` | `lh_` | `lh_` | `lh_` | `RuntimeConfig.lakehouse_prefix` |
| `SilverLakehouse` | `lh_silver` | `lh_silver` | `lh_silver` | step 6's staging fill |
| `GoldWarehouse` | `wh_gold` | `wh_gold` | `wh_gold` | `pl_master` stored-procedure activities |
| `LandingRoot` | `Files/landing` | `Files/landing` | `Files/landing` | `landing_path()` |
| `Parallelism` | `4` | `4` | `8` | `pl_master` ForEach `batchCount` |
| `Scale` | `tiny` | `demo` | `""` | `nb_00` — dev and test only |

Most of those columns are identical across stages, and that is the point rather than an oversight:
the names are stage-invariant by design, so the variables exist to make the binding **explicit and
relocatable** rather than to differ. The two that genuinely differ — `Parallelism` and `Scale` — are
the two that should, because `nb_00_generate_landing_data` fabricates data and a generator that can
run in production is a generator that will.

This table is now implemented, in `fabric/items/vl_payments.VariableLibrary/`, and
`tests/test_fabric_items.py` fails if the two stop agreeing. Implementing it corrected the last row.

### The row this table used to have, and why it could not be built

The `prod` cell above said ***(unset)*** until the JSON was written, and **the Variable Library
format cannot express that.** Every variable requires a default `value`, and a value set can only
*override* a variable that already exists
([variable-library-cicd](https://learn.microsoft.com/fabric/cicd/variable-library/variable-library-cicd),
ms.date 2025-12-15); there is no way to declare a variable absent in one stage. "Unset in prod" was a
thing this page had asserted and the format had no way to hold.

The replacement is better than what it replaced, which is why it is recorded here rather than quietly
patched. `valueSets/prod.json` sets `Scale` to the empty string, and the empty string is not a scale:

```python
scale = str(PARAMS["scale"])
if scale not in SCALES:
    raise ValueError(f"unknown scale {scale!r}; expected one of {sorted(SCALES)}")
```

That guard in `nb_00_generate_landing_data.py` was written for typos, so no notebook needed changing.
An *unset* variable would make the generator's behaviour in prod depend on how whatever reads it
handles a missing key — a question with a different answer per consumer. An empty string makes prod
the stage where the generator **cannot start**: a loud failure rather than an absence. The design
intent was "the generator must not run in production", and the format pushed the implementation
towards a stronger version of it than the one originally written down.

A Data Factory pipeline consumes one as `@pipeline().libraryVariables.SilverLakehouse`, which is how
`pl_master` can name the silver lakehouse without hard-coding it.

The rest of the format, for the record: `variables.json` and `settings.json` are required and
`valueSets/` is optional; there is deliberately **no `dev.json`**, because a value set holds only
non-default values and the default set therefore already *is* dev. Supported types are `Boolean`,
`DateTime`, `Number`, `Integer`, `String` and `ItemReference`. `SilverLakehouse` and `GoldWarehouse`
are `String` rather than the more idiomatic `ItemReference` for the reason everything in `fabric/` is
conservative: an `ItemReference` value needs a workspace GUID and an item GUID that do not exist until
the items do.

---

## 7. `pl_master`, described by pointing at a file

`orchestration/run.py`'s module docstring carries the mapping table, and it is a 1:1 mirror rather
than a convenience script — which is what lets this section point at the file instead of hand-waving
about the pipeline JSON:

| `orchestration/run.py` | `pl_master` |
|---|---|
| `read_table(META, "meta_source_config")` filtered on `enabled` | **Lookup** activity |
| grouping by `priority` into sequential waves | chained **ForEach** activities |
| `ThreadPoolExecutor` inside a wave | ForEach, `isSequential = false`, `batchCount = N` |
| `nb_01_bronze_ingest.ingest(entity=...)` | **Notebook** activity, parameterised |
| `nb_02_silver_transform.transform(entity=...)` | **Notebook** activity, parameterised |
| retry with backoff | activity `retry` / `retryIntervalInSeconds` |
| `gold.load()` after the silver stage | a chain of **Stored procedure** activities on `wh_gold` |

The waves are four, from `meta_source_config.priority`: `5` (card_products, customers), `10`
(accounts, merchants), `20` (transactions), `30` (disputes, fx_rates). The ordering is a real
dependency and not a preference — `transactions` carries a `referential` DQ rule against
`dim_merchant`, so running the fact concurrently with its dimension would quarantine valid rows and
present as bad source data. Expressed as chained ForEach activities, one per priority, each waiting
on the previous.

Two behaviours the JSON has to preserve and which are easy to lose in translation:

- **A failed entity does not stop its wave**, but a failed wave stops the next one. In Data Factory
  that is per-activity failure tolerance inside the ForEach plus a dependency condition between the
  ForEach activities — not a single `Completion` dependency, which would let a broken wave's
  successors run.
- **Gold is not entity-shaped.** It is ten stored procedures in a fixed dependency order —
  reference data, `dim_date`, dimensions, facts, then the aggregate that reads the facts — and none
  of that is expressible as a priority on a source feed, because gold tables are not source feeds.
  So it is a flat chain of Stored procedure activities, in the order `run_gold_stage` already
  encodes, stopping at the first failure. Retry wraps the whole chain rather than one proc, because
  every proc is idempotent and resuming from the middle would require knowing which earlier ones
  committed.

Gold also logs differently, and the pipeline cannot paper over it: bronze and silver write
`meta_run_log` in `lh_meta`, while the procs write `stg.load_log` inside `wh_gold`, because a stored
procedure cannot write to a lakehouse table and `TRY...CATCH` is the only thing that can record its
own failure. `run_gold_stage` ties the two by deriving the gold batch id from the run id, so
`meta_run_log.run_id` and `stg.load_log.load_batch_id` name the same run.

### If the pipeline JSON is authored instead of clicked

`fabric-cicd` ([microsoft.github.io/fabric-cicd](https://microsoft.github.io/fabric-cicd/)) is the
Python library for code-first deployment: `FabricWorkspace(workspace_id=..., environment=...,
repository_directory=...)` then `publish_all_items()` / `unpublish_all_orphan_items()`, authenticating
with `AzureCliCredential`, parameterised through a `parameter.yml` keyed by `environment`. All
`FabricWorkspace` arguments must be keyword arguments. `fabric/deploy.py` is the wrapper, and it has
**never been executed** — not once, against anything.

Two properties to know before relying on it: it **deploys fully every time without checking commit
diffs**, and it deploys into the tenant of the executing identity. It supports only item types that
have both Source Control and public Create/Update APIs, which covers every item in §2's inventory.

Both properties shape the script's interface. `--workspace-id` is required and never defaulted,
because there is no tenant argument — whoever is logged in to `az` is who the deploy runs as, and a
script that guesses a workspace eventually overwrites the wrong one. `unpublish_all_orphan_items()`
**deletes** workspace items absent from the repository, so it sits behind `--unpublish-orphans` and
defaults off. `--dry-run` prints the item list and touches neither the workspace nor your credentials.
There is no `parameter.yml`, and [`fabric/README.md`](../fabric/README.md) §4 explains why its absence
is the design rather than an omission.

---

## 8. What a deployment does not carry

Not copied between stages
([understand-the-deployment-process](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/understand-the-deployment-process)):
**data** (metadata only), item URL and ID, permissions, workspace settings, app content and
settings, and personal bookmarks. For semantic models additionally: role assignments, refresh
schedule, **data source credentials**, query caching and endorsement.

Three of those are operationally sharp for this repo:

- **Data source credentials.** A deployed semantic model has none, so the first query against a
  freshly promoted model fails on authentication rather than on anything interesting. This is the
  step most likely to be mistaken for a broken deployment.
- **Gateways are not automatically mapped after the initial deployment.** Configure once manually;
  subsequent deployments do not reset it.
- **Permissions.** Which is §1 step 10 again from the other direction: `05_security.sql` is a script
  someone runs per stage, because neither git integration nor deployment pipelines will carry it.

Hard limits worth remembering: 300 items per deployment; circular and self dependencies fail; PBIR
reports are unsupported. And a dated one that will expire: **from 12 February 2026, deployment
pipelines no longer support semantic models that have not been upgraded to Enhanced Metadata.**
`semantic-model/` is authored in TMDL against a current schema, so this should not apply — but "should
not" is doing real work in that sentence and it belongs on the step-9 checklist.

Unsupported item types in a git-connected workspace "are ignored. They aren't saved or synced, but
they're not deleted either", which is a safer failure than it sounds and is why connecting the
workspace in step 11 cannot destroy anything.

---

## 9. What would falsify this document

The honest list, most likely first. Each is a thing I expect to be corrected on, and being corrected
on a written prediction is a better outcome than having predicted nothing.

**1. The Direct Lake rebinding rule may have no rule to use.**
[understand-the-deployment-process](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/understand-the-deployment-process)
says to use datasource rules to rebind a Direct Lake model to the target stage.
[create-rules](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/create-rules) lists the
data sources a semantic model rule accepts — Analysis Services, Azure Synapse, SQL Server Analysis
Services, Azure SQL Server, SQL Server, OData, Oracle, SAP HANA (import only), SharePoint, Teradata
— and **a Fabric Warehouse or SQL analytics endpoint is not on it**, while the same page recommends
parameters "for other data sources". This model's Direct Lake partitions source from `wh_gold`'s
endpoint via `DatabaseQuery`. So either the endpoint is accepted as one of the SQL types in practice,
or the rebinding is done with a parameter rule, or it is done some third way the docs describe
elsewhere. **I do not know which, and I have written it as an open question rather than picked the
plausible-sounding one.** It is the first thing I would test in step 9, and it is a good example of
what reading documentation can and cannot settle.

**2. The cross-warehouse staging read.** Step 6 and the whole portability claim of the gold layer
rest on `INSERT INTO stg.* SELECT ... FROM lh_silver.dbo.*` being a supported cross-database read
inside one workspace. If it is not — or if it is supported but not transactional with the rest of
the load — then `06_staging.sql`'s snapshot-stability argument weakens and the staging design needs
revisiting, though the seam itself stays useful.

**3. The warehouse deployment story is the least settled part of this.** §4 picks "scripts are
authoritative, DacFx extraction is derived", which is coherent but has never been round-tripped.
The specific thing I expect to discover: whether an extracted project and hand-authored scripts can
coexist in one repository without one of them becoming stale in a way nothing detects. If they
cannot, the honest resolution is a `.sqlproj` plus a generated-file check in `make lint`, which is
the shape `tools/extract_dax.py` already uses for the semantic model.

**4. Every `.platform` file in `fabric/` is hand-authored** and none has round-tripped through a
tenant. Version 2.0, case-sensitive `type`, one GUID per item — each of which is a thing to get
wrong silently. The likeliest first failure is a duplicated `logicalId` from copying a directory.

**5. Capacity sizing in §3 is a guess**, and `docs/cost-and-capacity.md` cannot fix it without a
capacity metrics app and a real run.

**6. Delta maintenance exists now, but its schedule is untested and one of its numbers is a
guess.** `nb_03_table_maintenance` runs `OPTIMIZE` then `VACUUM` over the 21 lakehouse tables and
logs every action, and running it over the real lake took the whole lake from 1.91× to 1.15×
live-to-on-disk. Two things about it remain unvalidated here. **The schedule is a claim, not a
configuration:** nothing in `fabric/` expresses "weekly, after the last load of the week", so on a
tenant it is a scheduler setting somebody has to make, and the right cadence depends on partition
growth this repo has not observed for eighteen months. **And the `--small-file-mib 16` threshold
that triggers the partition advisory is a convention rather than a measurement** — it is the right
order of magnitude for Parquet and it is not a number I read off a Learn page. Both would be
settled by one month of `meta_maintenance_log` rows on a real capacity, which is a shorter list of
unknowns than this item used to carry and a more specific one.

---

*Every Learn page cited here was read while writing this document. Where a page's own `ms.date`
mattered to a claim — the T-SQL surface area in particular — the date is recorded next to the rule it
produced in [`docs/fabric-tsql-subset.md`](fabric-tsql-subset.md) and in
[`tools/fabric_tsql_lint.py`](../tools/fabric_tsql_lint.py), which is the pattern this repo uses for
platform facts with a shelf life.*
