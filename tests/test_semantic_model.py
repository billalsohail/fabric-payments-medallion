"""The semantic model, checked against the warehouse it reads from.

`semantic-model/` is TMDL — the text format Fabric's git integration itself stores — and it has
never been opened by Power BI, because this project had no tenant (see the status box in README.md).
That makes it the least verified thing in the repo, and it is also the layer where a mistake is
least visible: a wrong relationship or a measure over the wrong column does not fail, it produces a
number. Someone reads that number and makes a decision with it.

So these assertions exist in place of the import that never happened. They divide into three kinds,
and the distinction is worth keeping straight because only the first kind is a real guarantee:

1. **Cross-layer agreement.** Every relationship has a `NOT ENFORCED` foreign key behind it and
   vice versa; every `sourceColumn` exists in the warehouse DDL. These are genuine — both sides are
   in this repo, so the check is complete. `semantic-model/definition/relationships.tmdl` promises
   this check by name in its own header, and this file is what discharges that promise.

2. **Internal consistency.** Every `Table[column]` and `[Measure]` reference in a measure resolves;
   measure names are unique model-wide; no `lineageTag` is reused. Real, and enough to catch the
   most common edit mistake, which is renaming a column and missing a measure that used it.

3. **Encoded design decisions.** No measure divides with `/`; every resolved-date measure also
   excludes open disputes; the non-additive aggregate column is only reachable through a guarded
   measure; the aggregate is not declared as a user-defined aggregation. These are the tests worth
   the most, because each one is a specific way of being wrong that the TMDL comments argue against
   in prose — and a comment does not fail a build.

**What none of this establishes: that the model imports.** TMDL that parses is not TMDL that Fabric
accepts. `tools/tmdl.py` reads the subset this model uses; it is not a validator, and a schema error
it does not know about would sail through every test here. That gap is stated in the tooling and in
`semantic-model/README.md` rather than papered over, and it is the first thing that would be closed
with tenant access.
"""
from __future__ import annotations

import re

import pytest

from tests.ddl_parse import parse_foreign_keys, parse_tables, read_ddl
from tools import tmdl

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------
# Both sides are parsed once per module. `tmdl.load()` is also the only place measure-name
# collisions are detected — it raises on a duplicate, because DAX measure names are model-wide and
# not table-scoped, so two tables each defining `[Attempts]` is a model that cannot be opened. That
# check therefore runs implicitly in every test below rather than needing one of its own.


@pytest.fixture(scope="module")
def model() -> tmdl.Model:
    return tmdl.load()


@pytest.fixture(scope="module")
def ddl() -> str:
    return read_ddl()


@pytest.fixture(scope="module")
def wh_tables(ddl: str) -> dict[str, dict[str, bool]]:
    return parse_tables(ddl)


@pytest.fixture(scope="module")
def wh_fks(ddl: str) -> list[dict]:
    return parse_foreign_keys(ddl)


def _split(ref: str) -> tuple[str, str]:
    """`fact_transaction.date_sk` -> `("fact_transaction", "date_sk")`.

    TMDL writes a relationship endpoint as one dotted string, quoting either half only when it
    contains a space. Split from the right: a table name cannot contain a dot, but this keeps the
    parse honest if one day it is quoted.
    """
    table, _, column = ref.rpartition(".")
    return table.strip("'"), column.strip("'")


def _relationship_endpoints(model: tmdl.Model) -> set[tuple[str, str, str, str]]:
    return {
        (*_split(r.props["fromColumn"]), *_split(r.props["toColumn"]))
        for r in model.relationships
    }


def _fk_endpoints(wh_fks: list[dict]) -> set[tuple[str, str, str, str]]:
    """The same shape as `_relationship_endpoints`, with schemas dropped.

    Single-column only, which holds for every FK in this warehouse: each one is a surrogate key.
    A composite FK would need a different comparison, and asserting the single-column shape here
    means it fails loudly rather than silently comparing only the first column.
    """
    for fk in wh_fks:
        assert len(fk["cols"]) == 1 and len(fk["ref_cols"]) == 1, (
            f"{fk['name']} is composite; the comparison below would only look at its first column"
        )
    return {
        (fk["table"].split(".")[-1], fk["cols"][0],
         fk["ref_table"].split(".")[-1], fk["ref_cols"][0])
        for fk in wh_fks
    }


# --------------------------------------------------------------------------------------
# Neither side of the comparison may be vacuous
# --------------------------------------------------------------------------------------
# `tests/test_warehouse_ddl.py` carries the same three tests for the same reason, and the reason is
# worth repeating: every assertion below iterates over a parsed collection, so a parse that quietly
# returns nothing turns this whole file green while checking nothing at all. That is a worse outcome
# than having no tests, because the CI badge then asserts something false.

def test_the_model_was_parsed(model):
    assert set(model.tables) == {
        "dim_date", "dim_account", "dim_customer", "dim_merchant", "dim_card_product",
        "dim_currency", "dim_decline_reason", "fact_transaction", "fact_dispute",
        "agg_merchant_daily",
    }, sorted(model.tables)
    assert len(model.relationships) == 14, [r.name for r in model.relationships]
    assert len(model.measures) >= 30, sorted(model.measures)


def test_the_warehouse_ddl_was_parsed(wh_tables, wh_fks):
    assert "dbo.fact_transaction" in wh_tables, sorted(wh_tables)
    assert len(wh_tables["dbo.fact_transaction"]) >= 25
    assert len(wh_fks) == 14, [fk["name"] for fk in wh_fks]


def test_every_measure_has_an_expression(model):
    """Guards the DAX assertions specifically.

    Several tests below search a measure's expression for something it must or must not contain.
    Every one of them passes on an empty string, so a parser that returned measures without their
    bodies — the multi-line ``` fence is the fiddly part of reading TMDL — would disable them all
    at once while `test_the_model_was_parsed` still counted 36 measures.
    """
    for name, (table, block) in model.measures.items():
        assert (block.value or "").strip(), f"{table}.[{name}] parsed with no expression"


# --------------------------------------------------------------------------------------
# Cross-layer agreement: the model and the warehouse describe the same star
# --------------------------------------------------------------------------------------

def test_every_relationship_has_a_foreign_key_behind_it(model, wh_fks):
    """One half of the promise `relationships.tmdl` makes in its own header.

    A relationship without an FK is a join the optimiser does not know about and a reader of the
    DDL cannot see. It still works in the model, which is what makes it easy to leave behind.
    """
    missing = sorted(_relationship_endpoints(model) - _fk_endpoints(wh_fks))
    assert not missing, (
        "relationships with no NOT ENFORCED foreign key in 04_constraints.sql: "
        f"{missing}"
    )


def test_every_foreign_key_has_a_relationship(model, wh_fks):
    """The other half, and the more likely direction to break.

    Adding a dimension means writing the FK and then remembering the relationship. Forgetting the
    second gives a model where the new dimension silently filters nothing — every visual sliced by
    it repeats the unfiltered total, which looks like data rather than like an error.
    """
    missing = sorted(_fk_endpoints(wh_fks) - _relationship_endpoints(model))
    assert not missing, (
        f"foreign keys with no relationship in the semantic model: {missing}"
    )


def test_every_modelled_column_exists_in_the_warehouse(model, wh_tables):
    """A `sourceColumn` that does not exist is the failure this catches, and on Fabric it is a
    refresh error rather than a blank — but only once someone opens the model. Until then a renamed
    warehouse column leaves TMDL that reads perfectly well and cannot load.
    """
    missing = []
    for table in sorted(model.tables):
        wh = wh_tables.get(f"dbo.{table}")
        assert wh, f"model table {table} has no CREATE TABLE in the warehouse DDL"
        for name, column in model.columns(table).items():
            source = column.props.get("sourceColumn")
            assert source, f"{table}[{name}] has no sourceColumn"
            if source.lower() not in wh:
                missing.append(f"{table}[{name}] -> dbo.{table}.{source}")
    assert not missing, f"columns with no matching warehouse column: {missing}"


def test_the_only_star_table_left_out_of_the_model_is_dim_fx_rate(model, wh_tables):
    """The model exposing fewer tables than the warehouse is correct here, and worth pinning.

    `dim_fx_rate` is deliberately absent. FX conversion happens in the gold load — every fact
    carries `amount_gbp_minor` alongside its original amount, and `fx_rate_is_carried` records when
    no rate was found and the previous one was reused. By the time the model reads the fact, the
    rate has done its work; exposing the rate table would invite someone to convert twice.

    Pinned as an equality rather than a subset so that a *new* dimension omitted by accident fails
    here, instead of being quietly tolerated by the same rule that legitimately excuses this one.
    """
    star = {
        t for t in wh_tables
        if t.startswith(("dbo.dim_", "dbo.fact_", "dbo.agg_"))
    }
    assert star - {f"dbo.{t}" for t in model.tables} == {"dbo.dim_fx_rate"}


# --------------------------------------------------------------------------------------
# The role-playing date dimension, and the -1 trap underneath it
# --------------------------------------------------------------------------------------

def test_fact_dispute_has_two_date_relationships_and_exactly_one_is_active(model):
    """The textbook role-playing case, and the reason it is the raised date that stays active:
    raised-date counts are final the moment a dispute is raised, while resolved-date counts restate
    as disputes close. The default relationship should be the one whose history does not move.
    """
    date_rels = [
        r for r in model.relationships
        if _split(r.props["fromColumn"])[0] == "fact_dispute"
        and _split(r.props["toColumn"])[0] == "dim_date"
    ]
    assert len(date_rels) == 2, [r.name for r in date_rels]
    inactive = [r for r in date_rels if r.props.get("isActive") == "false"]
    assert len(inactive) == 1, (
        "exactly one of the two fact_dispute date relationships must be inactive; two active "
        "relationships between the same pair of tables is a model Fabric will refuse to open"
    )
    assert _split(inactive[0].props["fromColumn"])[1] == "resolved_date_sk"


def test_every_resolved_date_measure_also_excludes_open_disputes(model):
    """The single most valuable assertion in this file, because the bug it prevents is invisible.

    `resolved_date_sk` is `NOT NULL` and is **-1 while the dispute is open** — the unknown member,
    which carries a real date rather than a sentinel one. So a measure that switches to the resolved
    date without also filtering `is_open = 0` does not produce a blank or an obviously wrong day. It
    lands on a real date and inflates it by the entire open backlog, and the chart still looks like
    a chart.

    `USERELATIONSHIP` and the `is_open = 0` filter are therefore one idea, not two, and neither is
    correct alone. This test is what makes that non-optional for the next measure someone adds.
    """
    checked, offenders = [], []
    for name, (table, block) in model.measures.items():
        expr = block.value or ""
        if "USERELATIONSHIP" not in expr or "resolved_date_sk" not in expr:
            continue
        checked.append(f"{table}.[{name}]")
        if not re.search(r"is_open\s*\]\s*=\s*0", expr):
            offenders.append(f"{table}.[{name}]")
    assert checked, (
        "no measure switches to the resolved-date relationship, so this test checked nothing — "
        "either the measures were renamed or the expressions were not parsed"
    )
    assert not offenders, (
        "resolved-date measures that do not exclude open disputes, and so count the entire open "
        f"backlog against whatever real date row -1 holds: {offenders}"
    )


# --------------------------------------------------------------------------------------
# The aggregate table, and the one column in it that does not add up
# --------------------------------------------------------------------------------------

def test_the_aggregate_is_not_a_user_defined_aggregation(model):
    """`agg_merchant_daily` is exposed as an ordinary table, and that is the design decision.

    A user-defined aggregation (`alternateOf` in TMDL) lets the engine silently substitute the
    aggregate for the detail fact when it judges the query answerable from it. That substitution is
    always right for the six additive columns and never right for `distinct_account_count` above day
    grain — and the engine cannot tell the two cases apart, because a distinct count is not an
    aggregation of distinct counts. Silent and sometimes-wrong is the worst combination available,
    so the substitution is not offered: the `(agg)` measures say in their own names which table they
    read.
    """
    for table, block in sorted(model.tables.items()):
        for name, column in model.columns(table).items():
            assert not column.of_kind("alternateOf"), (
                f"{table}[{name}] declares alternateOf, which re-enables the silent substitution "
                "the aggregate is deliberately not set up for"
            )


def test_the_non_additive_aggregate_column_is_only_reachable_through_a_guarded_measure(model):
    """`distinct_account_count` sums to nonsense above day grain, and the guard is the fix.

    Summing a per-day distinct count over a month counts an account once per day it transacted. The
    number that comes out is larger than the truth, plausible in size, and monotonic in the way a
    real distinct count would be — there is nothing about it that looks wrong.

    Two defences, and the test asserts both. `summarizeBy: none` stops the column being dragged onto
    a visual and implicitly summed. `IF(HASONEVALUE(...))` makes the wrong number *unobtainable*
    rather than merely documented as wrong: above day grain the measure returns BLANK. Blank rather
    than an error string, deliberately — a text value would change the measure's data type and break
    every numeric visual that uses it.
    """
    column = model.columns("agg_merchant_daily")["distinct_account_count"]
    assert column.props.get("summarizeBy") == "none", column.props

    referencing = [
        (f"{table}.[{name}]", block.value or "")
        for name, (table, block) in model.measures.items()
        if ("agg_merchant_daily", "distinct_account_count") in tmdl.column_refs(block.value or "")
    ]
    assert referencing, "no measure reads distinct_account_count, so this test checked nothing"
    for label, expr in referencing:
        assert "HASONEVALUE" in expr, (
            f"{label} sums distinct_account_count without a HASONEVALUE guard, so it returns a "
            "plausible over-count at any grain coarser than a day"
        )


# --------------------------------------------------------------------------------------
# Internal consistency of the DAX
# --------------------------------------------------------------------------------------

def test_every_column_reference_in_a_measure_resolves(model):
    """Catches the ordinary edit mistake: a column renamed in the DDL and in the TMDL column list,
    but missed in a measure that used it. On Fabric that is an error at model load, which is to say
    it is found by whoever opens the report next rather than by whoever made the change.
    """
    known = {(t, c) for t in model.tables for c in model.columns(t)}
    broken = []
    for name, (table, block) in model.measures.items():
        for ref_table, ref_column in sorted(tmdl.column_refs(block.value or "")):
            if (ref_table, ref_column) not in known:
                broken.append(f"{table}.[{name}] -> {ref_table}[{ref_column}]")
    assert not broken, f"measures referencing columns that do not exist: {broken}"


def test_every_measure_reference_in_a_measure_resolves(model):
    """The measures build on each other deliberately — `[Authorisation Rate]` is
    `DIVIDE([Approved Transactions], [Attempts])`, not a second copy of the same SUMs — so a
    renamed base measure breaks several dependants at once.
    """
    broken = []
    for name, (table, block) in model.measures.items():
        for ref in sorted(tmdl.measure_refs(block.value or "")):
            if ref not in model.measures:
                broken.append(f"{table}.[{name}] -> [{ref}]")
    assert not broken, f"measures referencing measures that do not exist: {broken}"


def test_no_measure_divides_with_a_slash(model):
    """`DIVIDE` everywhere, `/` nowhere, and the difference shows up on exactly the rows that matter.

    `/` on a zero denominator returns Infinity, which formats happily as a percentage and renders in
    a visual. `DIVIDE` returns BLANK, and a blank row drops out of a chart. A merchant with no
    attempts should vanish from an authorisation-rate ranking, not top it — and a merchant with no
    attempts is the normal state of a merchant on any given day.

    String literals and DAX comments are stripped first: a `//` comment contains a slash, and so
    does a format string, and neither is division.
    """
    offenders = []
    for name, (table, block) in model.measures.items():
        expr = re.sub(r'"(?:[^"]|"")*"', '""', block.value or "")
        expr = re.sub(r"//[^\n]*", "", expr)
        expr = re.sub(r"--[^\n]*", "", expr)
        expr = re.sub(r"/\*.*?\*/", "", expr, flags=re.S)
        if "/" in expr:
            offenders.append(f"{table}.[{name}]")
    assert not offenders, (
        f"measures dividing with `/` rather than DIVIDE, so a zero denominator yields Infinity "
        f"instead of BLANK: {offenders}"
    )


# --------------------------------------------------------------------------------------
# Direct Lake: the constraints that make the storage mode what it claims to be
# --------------------------------------------------------------------------------------

def test_every_table_is_a_single_direct_lake_partition(model):
    """Direct Lake reads the Delta files the Warehouse writes — no import, no refresh window, no
    second copy of the data. The properties below are what express that, and each one is a way the
    model could quietly stop being Direct Lake: a second partition, an `import` mode, or a source
    pointing somewhere other than the warehouse.
    """
    for table, block in sorted(model.tables.items()):
        partitions = block.of_kind("partition")
        assert len(partitions) == 1, f"{table} has {len(partitions)} partitions"
        partition = partitions[0]
        assert partition.props.get("mode") == "directLake", partition.props
        sources = partition.of_kind("source")
        assert len(sources) == 1, f"{table} partition has {len(sources)} sources"
        source = sources[0].props
        assert source.get("schemaName") == "dbo", source
        assert source.get("expressionSource") == "DatabaseQuery", source
        assert source.get("entityName") == table, (
            f"{table} reads warehouse table {source.get('entityName')!r}; the model name and the "
            "warehouse name are deliberately identical here, so a mismatch is a mistake rather "
            "than a rename"
        )


def test_the_model_forbids_falling_back_to_directquery(model):
    """`DirectLakeOnly` turns the fallback into an error, and that is the point.

    The default (`Automatic`) silently switches a query to DirectQuery against the SQL endpoint when
    it exceeds a guardrail — row counts, model size, an unsupported construct. The report keeps
    working and gets slower, which means the guardrail is discovered in production as a performance
    complaint. Failing instead moves that discovery to whoever wrote the query.
    """
    assert model.model.props.get("directLakeBehavior") == "DirectLakeOnly", model.model.props


def test_there_are_no_calculated_columns(model):
    """Direct Lake does not support them, and the reason is worth knowing rather than just obeying:
    there is no engine pass in which a calculated column could be materialised, because the columns
    are read straight from Delta. Anything derived belongs in the gold load, where it is computed
    once in T-SQL and is visible to everything downstream rather than only to the model.

    In TMDL a calculated column is a `column X = <expression>`, so the test is that no column block
    carries an expression at all.
    """
    offenders = [
        f"{table}[{name}]"
        for table in sorted(model.tables)
        for name, column in model.columns(table).items()
        if column.value
    ]
    assert not offenders, f"calculated columns, which Direct Lake cannot evaluate: {offenders}"


# --------------------------------------------------------------------------------------
# Model-level hygiene
# --------------------------------------------------------------------------------------

def test_the_model_references_exactly_the_table_files_on_disk(model):
    """`model.tmdl` lists its tables by `ref table`, and a table file not listed there is a file
    Fabric ignores. The symptom is a table simply missing from the model, with nothing anywhere
    reporting that a file was skipped — which is why this is worth a test rather than a glance.
    """
    referenced = {c.name for c in model.model.children if c.kind == "ref" and c.value == "table"}
    on_disk = {p.stem for p in (tmdl.MODEL_DIR / "tables").glob("*.tmdl")}
    assert referenced == on_disk, {
        "listed in model.tmdl but no file": sorted(referenced - on_disk),
        "file exists but not listed": sorted(on_disk - referenced),
    }


def test_every_lineage_tag_is_unique(model):
    """Lineage tags are how Fabric tracks an object across renames, so two objects sharing one is a
    model with an ambiguous identity. These were generated deterministically from the object's name
    — see `semantic-model/README.md` — which makes a collision unlikely but also makes it the kind
    of mistake a copy-pasted table file would produce.
    """
    seen: dict[str, str] = {}
    duplicates = []

    def walk(block: tmdl.Block, path: str) -> None:
        tag = block.props.get("lineageTag")
        if tag:
            if tag in seen:
                duplicates.append(f"{tag}: {seen[tag]} and {path}")
            seen[tag] = path
        for child in block.children:
            walk(child, f"{path}/{child.kind} {child.name}")

    for table, block in sorted(model.tables.items()):
        walk(block, table)
    for relationship in model.relationships:
        walk(relationship, f"relationship {relationship.name}")
    assert seen, "no lineageTags were parsed at all"
    assert not duplicates, f"lineageTags used more than once: {duplicates}"


def test_every_relationship_joins_surrogate_keys(model):
    """Never on the business key, and for the SCD2 dimensions that is the whole point: joining
    `fact_transaction` to `dim_account` on `account_id` would match every version of the account and
    fan the fact out. The surrogate key names one version, chosen at load time by comparing the
    authorisation timestamp to the version's validity interval — so the join freezes the
    point-in-time decision the gold load already made rather than re-deciding it at query time.
    """
    for relationship in model.relationships:
        for end in ("fromColumn", "toColumn"):
            _, column = _split(relationship.props[end])
            assert column.endswith("_sk"), (
                f"{relationship.name}.{end} joins on {column!r}, which is not a surrogate key"
            )


def test_implicit_measures_are_discouraged(model):
    """Stops a report author dragging a column onto a visual and getting an implicit SUM.

    It matters most for the four `smallint` flags on `fact_transaction`. They are additive on
    purpose — `SUM(is_declined)` *is* the decline count, which is why they are not `bit` — so each
    one carries `summarizeBy: sum` and is genuinely summable. That makes the measures correct and
    also makes an accidental implicit aggregation look entirely reasonable. This flag is what keeps
    the named measures the only supported entry point.
    """
    assert model.model.has_flag("discourageImplicitMeasures"), model.model.props


def test_dim_date_is_marked_as_a_date_table(model):
    """`dataCategory: Time` plus `isKey` on the date column is what makes the DAX time-intelligence
    functions work. Without it they do not error — they return wrong answers for any period that is
    not a contiguous run of days present in the fact, which is most periods.
    """
    assert model.tables["dim_date"].props.get("dataCategory") == "Time"
    assert model.columns("dim_date")["full_date"].has_flag("isKey")
