# The semantic model

A Direct Lake semantic model over `wh_gold`, authored directly in **TMDL** — the text format
Fabric's own git integration stores a semantic model in. There is no `.pbix` here and there was
never going to be: Power BI Desktop is Windows-only, this is an arm64 Mac, and a binary that cannot
be diffed is not the artefact this repo is trying to produce anyway.

Authoring it as text is the point. Every design decision below is a line in a file a reviewer can
read, and the ones that matter are argued for in a `///` comment next to the thing they affect.

```
semantic-model/
├── definition/
│   ├── database.tmdl          compatibility level
│   ├── model.tmdl             model-level settings + the table manifest
│   ├── expressions.tmdl       the M expression Direct Lake reads through
│   ├── relationships.tmdl     14 relationships, 1 of them inactive
│   └── tables/*.tmdl          10 tables, 36 measures, 152 columns
└── measures.dax               GENERATED from the above — see below
```

---

## UNVALIDATED

**Nothing in this directory has been loaded by Fabric, by Analysis Services, or by any DAX engine.**

It is authored against the TMDL format and the Direct Lake constraints as the Microsoft Learn docs
describe them. It has never round-tripped through a tenant. That is the one gap in this repo that
tests cannot close, and it is worth being precise about what it does and does not mean:

**What is checked.** `tests/test_semantic_model.py` (23 tests) parses every file with
`tools/tmdl.py` and asserts the model is internally consistent and consistent with the warehouse:
every column maps to a real `wh_gold` column of a compatible type, every relationship joins a
declared foreign key to the primary key it references, every measure's column and measure
references resolve, the lineage tags are unique and derivable, and the design rules the comments
argue for are enforced — no measure divides with `/`, every resolved-date dispute measure also
excludes open disputes, the non-additive aggregate column is reachable only through a guarded
measure.

**What is not.** That Fabric accepts the files. TMDL that `tools/tmdl.py` parses is not TMDL that
Fabric imports — the parser reads the subset this model uses and is explicitly not a validator, so
a schema error it has never heard of sails through every test. A property could be misspelled, or
spelled correctly for a compatibility level other than 1604, and nothing here would notice.

**What that costs, concretely.** The first import on a real tenant is the step that would find it,
and it is a minutes-long step, not a days-long one: the failure mode for a bad TMDL property is a
named error on import, not a subtly wrong number. The numbers themselves are the part that has been
checked another way — `tests/test_gold_recon.py` ties the aggregate to the detail fact in SQL, which
is the same arithmetic the measures perform.

`measures.dax` exists partly for this reason. It is a runnable DAX query, and pasting it into DAX
query view against a deployed model is the fastest available answer to "does every measure
resolve?" — the error names the measure that does not.

---

## measures.dax is generated

`semantic-model/measures.dax` is **derived**. The TMDL is the source; `tools/extract_dax.py` writes
the `.dax` file from it, `make lint` runs that script with `--check`, and CI fails on drift. Edit
the TMDL.

The derived copy earns its place for three reasons: the 36 measures are spread across six table
files interleaved with 152 column declarations, so there is no single place to read them all; it is
a complete DAX query rather than a listing, with a generated `EVALUATE` over every measure; and
`src/warehouse/procs/09_sp_load_agg_merchant_daily.sql` cites it by name as the place rates live.

It carries the first paragraph of each measure's documentation and nothing more, so it stays an
index rather than a second copy of the reasoning. `formatString` and `displayFolder` travel as
comments because `DEFINE MEASURE` has no syntax for them — and they stay in view because a rate
measure with a count's format string is a real defect that is invisible from the expression.

---

## How lineage tags were generated

A lineage tag is how Fabric tracks a model object across a rename. Power BI Desktop mints them as
random UUIDv4s, which is correct when a tool owns the file and wrong for a model a person edits:
the obvious way to add an eleventh table here is to copy the tenth, and the obvious thing to forget
is the tag. Two objects then share one identity, which is not a syntax error and which Fabric will
accept.

So every tag in this model is derived from the object's path:

```python
LINEAGE_NAMESPACE = uuid.UUID("92c82230-3e6b-43b7-8a11-d3ef203de007")

def lineage_tag(path: str) -> str:
    return str(uuid.uuid5(LINEAGE_NAMESPACE, path))
```

`path` is the object's location as the directory lays it out — `tables/fact_transaction`,
`expressions/DatabaseQuery` — rather than the bare name, so a future column-level or measure-level
tag cannot collide with a table of the same name. The namespace is a fixed UUIDv4, minted once for
this repo and never regenerated; RFC 4122 wants a namespace for name-based UUIDs, and having our
own keeps these tags from colliding with anything else's `uuid5` of the same short string.

It is a constant, not a seed. Changing it rewrites all eleven tags, which to Fabric reads as eleven
new objects rather than eleven renamed ones.

`tests/test_semantic_model.py::test_every_lineage_tag_is_derived_from_its_object_path` re-derives
every tag and compares, so a copied-and-not-edited file fails with the file named and the tag it
should have. The uniqueness test is still there and still worth having: it is what catches a tag
that is missing entirely rather than wrong.

The honest caveat: tags are only *on* tables and on the one expression. Columns, measures and
relationships have none. Fabric will mint its own on first import, and those will be random —
meaning the guarantee above holds for the objects most likely to be copy-pasted and not for the
rest.

---

## Why agg_merchant_daily is not an aggregation

`relationships.tmdl` points here, because this is the decision most likely to be questioned: the
merchant-day aggregate is exposed as **its own table**, not registered as a Power BI user-defined
aggregation over `fact_transaction`.

A user-defined aggregation lets the engine silently substitute the aggregate for the detail fact
whenever a query's grain permits. For the seven additive columns that substitution is a pure win and
is always correct. For `distinct_account_count` it is not correct at any grain
coarser than a day, and **the engine cannot tell the difference** — a distinct count is not an
aggregation of distinct counts. The substitution would therefore be right for the seven additive
columns and wrong for the eighth, silently, at query time, with no error and a plausible number.

The size of "wrong" is measured rather than asserted. In `tests/test_gold_recon.py`:

    test_distinct_account_count_is_not_additive_and_is_only_right_per_day

sums the daily counts and asserts the total strictly exceeds the true distinct count. At this data's
shape it overstates by roughly an order of magnitude.

Exposing the table explicitly moves the choice to a report author, and then fences it:

* `[Distinct Accounts (per day)]` returns BLANK unless `HASONEVALUE(dim_date[date_sk])`. A monthly
  card reads blank rather than reading the sum of thirty daily distinct counts.
* `[Distinct Accounts]` is the correct answer at any grain, computed over the detail fact via
  `dim_account[account_id]` — the business key, so it does not double-count an account whose SCD2
  version changed inside the period.
* Every aggregate measure sits in the `Merchant daily (agg)` display folder, away from the detail
  measures it superficially duplicates. The aggregate relates only to `dim_date` and
  `dim_merchant`; slicing one of its measures by `fact_transaction[channel]` does not error, it
  repeats the unfiltered total in every channel row.

The cost of the decision, stated plainly: the engine will never use the aggregate automatically, so
a detail-fact query that *could* have been served from one row per merchant-day is not. That is the
price of the eighth column, and it is the right trade because the alternative is a wrong number
nobody can see.

---

## Named gaps

The things a production version of this model has and this one does not. Each is absent for a
reason, and the reason is never that it was forgotten.

**1. RLS and OLS roles.** There are no `role` blocks. Row-level and object-level security for a
Direct Lake model must be **restated at the model level** — warehouse RLS and dynamic data masking
in `src/warehouse/ddl/05_security.sql` do not reach a Direct Lake consumer. The mechanism a role
would use is already here: `account_region` is denormalised onto both facts by the loaders
precisely so a security predicate can filter the fact directly, and `dim_customer[date_of_birth]`
is hidden with `birth_year` provided as the usable alternative. What is missing is the role
definitions themselves, and they are missing deliberately: writing DAX filter expressions and an
OLS column list I cannot test against a tenant, for the one feature whose failure mode is *showing
someone data they may not see*, would be exactly the unverifiable claim this project is built to
avoid. `05_security.sql` takes the same line — its `GRANT UNMASK` statements are commented
templates with `@bank.example` placeholders and no real principal names.

**2. No `.platform` file.** Fabric git integration identifies each item with a `.platform`
descriptor holding its type, display name and logical id. This directory has none, so it is TMDL
that is *shaped* like a Fabric semantic model rather than a Fabric item you could sync. The
descriptor belongs to the deployment layer, not to the model, and will live in `fabric/items/`
alongside the lakehouse, warehouse and pipeline descriptors — labelled UNVALIDATED like everything
else in there, because a hand-authored item descriptor that has never round-tripped through a
tenant is exactly the kind of file worth being loud about.

**3. No report.** No `.pbir` definition, no page layout, no visual. `dashboard/build_dashboard.py`
reads gold directly and emits static HTML as an explicit stand-in — its job is to show the measures
resolve to sensible numbers, not to be a Power BI report.

**4. No calculated columns or calculated tables, anywhere.** This one is a constraint rather than an
omission, and it is load-bearing enough to be worth restating here: Direct Lake does not support
them. It is *why* `dim_date` is generated by `01_sp_load_dim_date.sql` rather than computed in DAX,
and why the additive flags are materialised as `smallint` by `07_sp_load_fact_transaction.sql`
rather than derived — `SUM(is_declined)` is the decline count, and a `bit` is not summable. The
modelling constraint reached back and changed the warehouse DDL.

**5. No perspectives, no translations, no field parameters, no calculation groups.** Nothing here
needs them, and a model that ships features nobody asked for is a model with more surface than
anyone has read.

**6. Direct Lake fallback is disabled, not tuned.** `directLakeBehavior: DirectLakeOnly` means a
query that exceeds a Direct Lake guardrail **fails** rather than silently falling back to
DirectQuery. That is the deliberate choice — a performance cliff that announces itself is worth
more than one that hides — but it is a choice made without ever having watched the guardrails bind
on real capacity. The tuning question, which row-group and parquet-file layout keeps the model
inside them, needs a tenant to answer.
