# `fabric/` — the deployment layer, UNVALIDATED

**Nothing in this directory has been run against a Microsoft Fabric tenant.** No item here has ever
been created, synced, published or read back. The Fabric trial requires a work or school account and
one could not be provisioned inside this project's window, so every file below is authored from
Microsoft Learn's documentation of the on-disk formats and from nothing else.

That makes this directory the one part of the repository whose correctness is **argued rather than
demonstrated**, and the rest of this page exists to say exactly where the argument is strong, where
it is weak, and what is deliberately missing. Everything else in the repo is verified by `make
test`; this is not, and the honest version of that is more useful to you than a confident one.

What follows is organised by how much I can defend:

1. [What is committed, and why only this](#1-what-is-committed-and-why-only-this)
2. [What is generated](#2-what-is-generated)
3. [The one format I could not read off a page](#3-the-one-format-i-could-not-read-off-a-page)
4. [Three things deliberately absent](#4-three-things-deliberately-absent)
5. [What building this refuted](#5-what-building-this-refuted)
6. [Every Learn page this directory rests on](#6-every-learn-page-this-directory-rests-on)
7. [How you would find out whether any of it works](#7-how-you-would-find-out-whether-any-of-it-works)

---

## 1. What is committed, and why only this

```text
fabric/
├── README.md                 ← this file
├── build_items.py            ← renders items/ into the git-integration layout
├── deploy.py                 ← wraps fabric-cicd; has never been executed
└── items/
    ├── lh_bronze.Lakehouse/.platform
    ├── lh_silver.Lakehouse/.platform
    ├── lh_meta.Lakehouse/.platform
    ├── lh_quarantine.Lakehouse/.platform
    ├── nb_00_generate_landing_data.Notebook/.platform
    ├── nb_01_bronze_ingest.Notebook/.platform
    ├── nb_02_silver_transform.Notebook/.platform
    ├── nb_03_table_maintenance.Notebook/.platform
    ├── nb_99_seed_metadata.Notebook/.platform
    ├── sm_payments.SemanticModel/.platform
    ├── pl_master.DataPipeline/.platform
    └── vl_payments.VariableLibrary/
        ├── .platform
        ├── variables.json
        ├── settings.json
        └── valueSets/{test,prod}.json
```

Twelve items, and the committed set is small on purpose: **a file is committed here only if it
cannot be derived from something the repo already holds.**

Two things meet that test. The `.platform` descriptors do, because a `logicalId` is a GUID that must
stay stable across every deploy — a workspace cannot hold two items with the same one, and copying a
descriptor to make a second item means changing both the `logicalId` and the `displayName`. A GUID
is not computable from anything, so it is generated once and committed.

The Variable Library does, because it is the **only** executable expression of
[`docs/fabric-deployment.md` §6](../docs/fabric-deployment.md). That section is a table in prose,
and a table in prose is documentation; these four JSON files are what a tenant would actually read.
The default set in `variables.json` holds the dev values and there is deliberately **no `dev.json`**
— a value set contains only the variables whose values are *non-default*, so the default set already
*is* dev, and adding a `dev.json` full of values identical to the defaults would be a second copy of
the same fact. `tests/test_fabric_items.py` asserts §6's table and this JSON still agree.

Everything else is generated, which is the next section.

## 2. What is generated

`make fabric-build` runs `build_items.py`, which writes `fabric/build/` — **gitignored**. It
contains the same twelve item directories, each with its `.platform` plus whatever definition files
that item type takes:

| Item type | Definition in `fabric/build/` | Rendered from |
|---|---|---|
| `Lakehouse` | none — a lakehouse's contents are data, not definition | — |
| `Notebook` | `notebook-content.py` | `src/notebooks/*.py`, jupytext percent cells |
| `SemanticModel` | `definition.pbism` + `definition/` (14 TMDL files) | `semantic-model/definition/` |
| `DataPipeline` | none — refused, see §4 | — |
| `VariableLibrary` | the four JSON files, copied verbatim | `items/vl_payments.VariableLibrary/` |

**Why generate rather than commit.** Fabric git integration stores a notebook as
`notebook-content.py` and a semantic model as a folder of TMDL. This repo already holds both of
those things, in `src/notebooks/` and `semantic-model/definition/`. Committing a second copy under
`fabric/items/` would create five notebook pairs and fourteen TMDL pairs with nothing keeping them
in agreement, and a fact stated twice is a fact that can disagree with itself. So the
git-integration layout is treated as what it is — a **deployment artefact** — by the same argument
that makes `semantic-model/measures.dax` generated from the TMDL and checked for drift by `make
lint` rather than maintained by hand beside it.

The cost of that choice is real and worth naming: you cannot point Fabric git integration at this
repository directly. Its layout lives in `fabric/build/`, which is not in git, so a real git-sync
setup would either commit the build output on a deployment branch or use `deploy.py`'s code-first
path instead. This repo picks code-first, and §7 of the deployment doc is where that is argued.

## 3. The one format I could not read off a page

Learn documents that a PySpark notebook is stored as `notebook-content.py` rather than `.ipynb`,
that cell output is not committed, and that the file keeps notebook metadata, markdown cells and
code cells "as separate sections". It does **not** print the syntax that delimits those sections. It
shows it only inside a screenshot.

So these five strings, the whole of `build_items.py`'s `_FABRIC_MARKERS`, are the single part of
this deployment layer that is not traceable to a documented format:

```text
# Fabric notebook source
# METADATA ********************
# CELL ********************
# MARKDOWN ********************
# META                            (line prefix, inside a METADATA section)
```

They are isolated in one constant for exactly that reason: the first real export
from a tenant corrects them in one place, and every rendered notebook follows.

`tests/test_fabric_items.py` asserts that the render round-trips — that parsing a generated
`notebook-content.py` back out recovers the cells that went in, and that every notebook's cell count
matches its source. That proves the renderer is self-consistent. It does not prove Fabric would
accept the result, and no test in this repo can.

## 4. Three things deliberately absent

**No `wh_gold.Warehouse/`.** This is the most consequential absence and it is not an oversight.
Fabric git integration represents a warehouse as a SQL database project extracted by DacFx, so
`src/warehouse/ddl/*.sql` and `src/warehouse/procs/*.sql` cannot be dropped into an item directory
and synced — the sync would want to own the schema, and the repo's scripts would stop being the
source of truth. §4 of the deployment doc picks the other side: the scripts stay authoritative and
`wh_gold` is deployed by **executing** them, which makes it the one item in the inventory whose
deployment is imperative rather than declarative. `deploy.py` prints that step rather than
performing it. The alternative — an item directory containing DacFx output nobody in this repo
generated — would look more complete and be less true.

**No `pipeline-content.json`.** `build_items.py` builds `pl_master.DataPipeline/` with its
descriptor and refuses to synthesise its body. A Data Factory pipeline definition is several hundred
lines of activity JSON with typed dependency edges, and hand-writing one would produce the single
most misleading file in the repository: authoritative-looking, never parsed, wrong in ways nobody
could see by reading it. `orchestration/run.py` is the honest statement of what `pl_master` does —
its module docstring carries the activity-by-activity mapping — and §7 points at that file for this
exact reason. The *item* is real; only its body is missing, and it is missing on purpose.

**No `parameter.yml`.** `fabric-cicd` parameterises by find-and-replace inside item definitions, and
the canonical thing people replace is a notebook's default-lakehouse GUID. `build_items.py` binds no
default lakehouse, because `src/runtime/context.py` names tables two-part (`lh_bronze.transactions`)
so that no notebook depends on which attached lakehouse is default. Every value that genuinely
differs by stage lives in the Variable Library instead. That leaves nothing for a parameter file to
substitute, so there is no parameter file — and if a future item needs one, that is the moment to
add it.

Attaching the lakehouses to the notebooks is still a tenant-side binding no file here can express.
It is step 4 of the §1 runbook.

## 5. What building this refuted

`docs/fabric-deployment.md` §6 shipped a table saying `Scale` is ***unset*** in prod, so that
`nb_00_generate_landing_data` — which fabricates data — cannot run there. Authoring the Variable
Library established that **this is not expressible in the format.** Every variable requires a
default `value`, and a value set can only *override* an existing variable; there is no way to
declare a variable absent in one stage. "Unset in prod" was a thing I had written down and could not
build.

The fix turns out to be better than the thing it replaces. `valueSets/prod.json` sets `Scale` to the
empty string, and `nb_00` already raises on it:

```python
scale = str(PARAMS["scale"])
if scale not in SCALES:
    raise ValueError(f"unknown scale {scale!r}; expected one of {sorted(SCALES)}")
```

That guard was written for typos and no notebook change was needed. An unset variable would make the
generator's behaviour in prod depend on what reads it; an empty string makes prod the stage where
the data generator **cannot start** — a loud failure instead of an absence. §6 has been corrected to
say so.

This is the second time in this repo that building an artefact refuted a line the docs had already
shipped; the first was `nb_03_table_maintenance` discovering that `OPTIMIZE` cannot compact bronze.
Both are recorded rather than quietly fixed, because the pattern is the useful part.

## 6. Every Learn page this directory rests on

Each with the `ms.date` it carried when read, so a rule that has gone stale can be found rather than
guessed at.

| Page | `ms.date` | What it settles here |
|---|---|---|
| `fabric/cicd/git-integration/source-code-format` | 2025-12-15 | `.platform` schema 2.0, the `{name}.{type}` directory pattern, `type` being case-sensitive, `logicalId` uniqueness |
| `fabric/data-engineering/notebook-source-control-deployment` | 2026-03-05 | a notebook is `notebook-content.py`, not `.ipynb`; output is not committed; `notebook-settings.json` and `fs-settings.json` are Fabric-generated and must not be hand-edited |
| `fabric/cicd/variable-library/variable-library-cicd` | 2025-12-15 | the four-file layout; a value set holds only non-default values; the active set is configuration, not definition |
| `rest/api/fabric/articles/item-management/definitions/variable-library-definition` | 2025-01-14 | the exact `variables.json`, `valueSet` and `settings.json` shapes and the supported variable types |
| `power-bi/developer/projects/projects-dataset` | 2025-12-15 | `definition.pbism` is required, and its `version` must be 4.0 or above for a TMDL `definition/` folder to be the definition |

One caveat on the last row: Power BI Desktop projects are documented as **preview**, and
`projects-dataset` describes a Desktop project rather than a Fabric git export. The two formats
overlap but this repo has verified neither, and the semantic model is the item here I would expect
to need the most correction on first contact with a tenant.

There is also a distinction worth knowing, because it bit me while reading. `source-code-format`
lists the item types with a documented *definition-file format* — mirrored databases, notebook,
paginated report, report, semantic model, user data functions. `intro-to-git-integration` lists the
types *supported for source control* — warehouse, lakehouse, notebook, environment, Spark job
definition, data pipeline, variable library. Those are different lists, and an item can be on the
second without being on the first. `pl_master` is exactly that case, which is part of why §4 refuses to invent its
body.

## 7. How you would find out whether any of it works

In order, and the first two are quick:

```bash
make fabric-build                                     # renders fabric/build/, no tenant needed
python -m pytest tests/test_fabric_items.py            # descriptors, parity, round trip
az login
python fabric/deploy.py --workspace-id <guid> --environment dev --dry-run
python fabric/deploy.py --workspace-id <guid> --environment dev
```

`--dry-run` prints the item list and sends nothing; it does not touch the workspace or your
credentials. The real run publishes every item in full without consulting commit history, deploys
into the tenant of whichever identity `az` is logged in as, and leaves orphaned workspace items
alone unless you pass `--unpublish-orphans`, which **deletes**.

The first thing likely to fail is §3's marker syntax, and the tell would be a notebook that imports
as one enormous cell, or as a cell of commented-out code. The second is the semantic model, for the
reason in §6. If both survive, the interesting failures start: whether `MERGE` is distributed the
way the gold procs assume, and whether the six-dimension surrogate-key resolution in
`07_sp_load_fact_transaction.sql` spills. Those two questions are §1 of the deployment doc and they
are the whole reason a tenant is worth having.
