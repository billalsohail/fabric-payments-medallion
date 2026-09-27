"""Structural consistency of the warehouse DDL — the check the linter cannot do.

`tools/fabric_tsql_lint.py` is a *per-file* check: does this statement use only constructs Fabric
Warehouse accepts? It says nothing about whether the objects those statements reference actually
exist, because it never sees more than one file at a time.

That gap is not theoretical. Writing `04_constraints.sql` produced
``UNIQUE (from_currency_code, to_currency_code, valid_from_date)`` against a table whose columns
are ``from_currency`` and ``to_currency``. The linter passed it — correctly, it is valid Fabric
T-SQL — and on a real Warehouse it would have failed at deployment, after the CREATE TABLEs in
the same batch had already committed. This file is the check that catches that class of error,
and it exists because that error happened rather than because it was anticipated.

It matters more here than it would in a repo whose SQL runs: nothing under `src/warehouse/` has
ever been executed (see docs/gold-execution.md), so a deployment-time failure would not be found
until the first real deployment. These assertions are the substitute for that first deployment,
and they are deliberately about the things a deployment would object to.

**On parsing.** `CREATE TABLE` is read with sqlglot, which models columns properly. Constraints
are read with regular expressions, which is a downgrade and worth justifying: sqlglot falls back
to an opaque `Command` node for ``ALTER TABLE ... ADD CONSTRAINT ... NOT ENFORCED`` — and
``NOT ENFORCED`` is the *only* form Fabric accepts, so every constraint in the repo is exactly
the form sqlglot cannot parse. An AST-only version of this file would silently check nothing,
which is the failure mode these tests exist to prevent.

Two sqlglot quirks are handled explicitly rather than swallowed, because both were first met as
a crash here:

- ``CREATE SCHEMA stg`` raises ``AttributeError`` inside sqlglot's TSQL dialect, not
  ``ParseError``. Filtering to ``CREATE TABLE`` before parsing sidesteps it; a blanket
  ``except Exception`` would have hidden it.
- ``MASKED WITH (FUNCTION = '...')`` raises ``ParseError``. The clause is stripped before parsing
  — masking is irrelevant to every assertion here, and the alternative is losing ``dim_customer``,
  the only table in the warehouse with PII, from the checks entirely.

Crucially, an unparseable ``CREATE TABLE`` **fails** rather than being skipped. Skipping is what
the linter did until FB019 was added, and the result was an AST pass that reported a clean bill of
health on DDL it had never looked at. A table this file cannot parse is a table exempt from every
assertion below, which is worse than a red test.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

DDL_DIR = Path(__file__).resolve().parents[1] / "src" / "warehouse" / "ddl"

# Facts and the aggregate. Kept as an explicit list rather than a `startswith("fact_")` test so
# that adding a fact table is a deliberate decision about which invariants apply to it.
FACT_TABLES = {"dbo.fact_transaction", "dbo.fact_dispute", "dbo.agg_merchant_daily"}


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Remove `--` comments. Needed before splitting on `;`, and before matching constraint
    syntax — this repo's DDL carries more prose than SQL, and several comments contain the word
    `CONSTRAINT` while describing one.
    """
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _statements(sql: str) -> list[str]:
    return [s.strip() for s in _strip_comments(sql).split(";") if s.strip()]


def _qualify(name: str) -> str:
    """`dbo.t` unchanged; bare `t` assumed to be in `dbo`, matching T-SQL's default schema."""
    name = name.replace("[", "").replace("]", "").lower()
    return name if "." in name else f"dbo.{name}"


def _is_nullable(col: exp.ColumnDef) -> bool:
    """Whether a parsed column accepts NULL.

    The `allow_null` check is the whole point. sqlglot represents an **explicit** ``NULL`` as
    ``NotNullColumnConstraint(allow_null=True)`` — the same node as ``NOT NULL``, differing only in
    that argument. An earlier version of this file tested only the node type, and so read every
    column in the DDL as NOT NULL. Every nullability assertion below asserts that something is *not*
    nullable, which meant all of them passed unconditionally and none of them could ever have caught
    the thing they exist to catch.

    It was found by `src/lib/gold.py` creating the staging tables from this same parse and failing on
    a NOT NULL violation for a column the DDL declares ``NULL``. Worth recording as the pattern it
    is: a static check that agreed with itself for months, corrected the moment something executed
    against it.
    """
    return not any(
        isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null")
        for c in col.constraints
    )


@pytest.fixture(scope="module")
def ddl() -> str:
    files = sorted(DDL_DIR.glob("*.sql"))
    assert files, f"no DDL found under {DDL_DIR}"
    return "\n".join(f.read_text() for f in files)


_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
_MASK_CLAUSE = re.compile(
    r"\s+MASKED\s+WITH\s*\(\s*FUNCTION\s*=\s*'(?:[^']|'')*'\s*\)", re.IGNORECASE
)


@pytest.fixture(scope="module")
def tables(ddl: str) -> dict[str, dict[str, bool]]:
    """`{qualified table: {column: is_nullable}}`, from every CREATE TABLE in the DDL.

    Parsed one statement at a time and filtered to ``CREATE TABLE`` first — see the module
    docstring for why a whole-file ``sqlglot.parse`` does not survive this DDL.
    """
    out: dict[str, dict[str, bool]] = {}
    for stmt in _statements(ddl):
        if not _CREATE_TABLE_RE.match(stmt):
            continue
        try:
            tree = sqlglot.parse_one(_MASK_CLAUSE.sub("", stmt), dialect="tsql")
        except Exception as exc:  # noqa: BLE001 — re-raised immediately with context
            raise AssertionError(
                f"a CREATE TABLE statement could not be parsed, so it would have been exempt "
                f"from every check in this file ({type(exc).__name__}: {exc}):\n{stmt[:400]}"
            ) from exc
        if not isinstance(tree, exp.Create) or (tree.kind or "").upper() != "TABLE":
            continue
        schema = tree.this
        name = _qualify(schema.this.sql(dialect="tsql"))
        cols: dict[str, bool] = {}
        for col in schema.expressions:
            if not isinstance(col, exp.ColumnDef):
                continue
            cols[col.name.lower()] = _is_nullable(col)
        out[name] = cols
    return out


# `ALTER TABLE <t> ADD CONSTRAINT <n> PRIMARY KEY|UNIQUE ... (<cols>) NOT ENFORCED`
_KEY_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[\w\[\]\.]+)\s+ADD\s+CONSTRAINT\s+(?P<name>\w+)\s+"
    r"(?P<kind>PRIMARY\s+KEY|UNIQUE)\b[^(]*\((?P<cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
# `... FOREIGN KEY (<cols>) REFERENCES <t2> (<cols2>) NOT ENFORCED`
_FK_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[\w\[\]\.]+)\s+ADD\s+CONSTRAINT\s+(?P<name>\w+)\s+"
    r"FOREIGN\s+KEY\s*\((?P<cols>[^)]*)\)\s*REFERENCES\s+(?P<ref_table>[\w\[\]\.]+)\s*"
    r"\((?P<ref_cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
_STATS_RE = re.compile(
    r"CREATE\s+STATISTICS\s+(?P<name>\w+)\s+ON\s+(?P<table>[\w\[\]\.]+)\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)


def _cols(raw: str) -> list[str]:
    return [c.strip().replace("[", "").replace("]", "").lower() for c in raw.split(",") if c.strip()]


@pytest.fixture(scope="module")
def keys(ddl: str) -> list[dict]:
    return [
        {"table": _qualify(m["table"]), "name": m["name"], "cols": _cols(m["cols"]),
         "kind": " ".join(m["kind"].upper().split())}
        for stmt in _statements(ddl)
        for m in [_KEY_RE.search(stmt)] if m
    ]


@pytest.fixture(scope="module")
def foreign_keys(ddl: str) -> list[dict]:
    return [
        {"table": _qualify(m["table"]), "name": m["name"], "cols": _cols(m["cols"]),
         "ref_table": _qualify(m["ref_table"]), "ref_cols": _cols(m["ref_cols"])}
        for stmt in _statements(ddl)
        for m in [_FK_RE.search(stmt)] if m
    ]


# --------------------------------------------------------------------------------------
# The fixtures themselves must not be vacuous
# --------------------------------------------------------------------------------------
# Every assertion below iterates over a parsed collection, so all of them pass trivially if the
# parsing silently returns nothing — which is exactly what happens the moment the DDL is
# reformatted in a way the regexes do not anticipate. These three tests are what stop this file
# from degrading into a suite that always passes.

def test_create_tables_were_parsed(tables):
    assert len(tables) >= 12, sorted(tables)
    assert "dbo.fact_transaction" in tables
    assert len(tables["dbo.fact_transaction"]) >= 25


def test_constraints_were_parsed(keys, foreign_keys):
    assert len(keys) >= 15, [k["name"] for k in keys]
    assert len(foreign_keys) >= 12, [f["name"] for f in foreign_keys]


def test_every_add_constraint_statement_was_recognised(ddl, keys, foreign_keys):
    """No `ADD CONSTRAINT` may be skipped by both regexes.

    Without this, a constraint written in a shape the patterns miss is not a failure — it is an
    invisible exemption from every check in this file.
    """
    total = sum(
        1 for stmt in _statements(ddl) if re.search(r"ADD\s+CONSTRAINT", stmt, re.IGNORECASE)
    )
    assert total == len(keys) + len(foreign_keys), (
        f"{total} ADD CONSTRAINT statements but only {len(keys) + len(foreign_keys)} parsed"
    )


# --------------------------------------------------------------------------------------
# Referential consistency of the DDL itself
# --------------------------------------------------------------------------------------

def test_key_constraints_reference_columns_that_exist(tables, keys):
    """The check that catches the `from_currency_code` bug described in the module docstring."""
    for k in keys:
        assert k["table"] in tables, f"{k['name']}: unknown table {k['table']}"
        for col in k["cols"]:
            assert col in tables[k["table"]], f"{k['name']}: {k['table']} has no column {col!r}"


def test_foreign_keys_reference_columns_that_exist(tables, foreign_keys):
    for fk in foreign_keys:
        assert fk["table"] in tables, f"{fk['name']}: unknown table {fk['table']}"
        assert fk["ref_table"] in tables, f"{fk['name']}: unknown referenced table {fk['ref_table']}"
        for col in fk["cols"]:
            assert col in tables[fk["table"]], f"{fk['name']}: {fk['table']} has no column {col!r}"
        for col in fk["ref_cols"]:
            assert col in tables[fk["ref_table"]], (
                f"{fk['name']}: {fk['ref_table']} has no column {col!r}"
            )
        assert len(fk["cols"]) == len(fk["ref_cols"]), fk["name"]


def test_statistics_reference_columns_that_exist(tables, ddl):
    found = 0
    for stmt in _statements(ddl):
        m = _STATS_RE.search(stmt)
        if not m:
            continue
        found += 1
        table = _qualify(m["table"])
        assert table in tables, f"{m['name']}: unknown table {table}"
        for col in _cols(m["cols"]):
            assert col in tables[table], f"{m['name']}: {table} has no column {col!r}"
    assert found >= 4, "statistics statements were not parsed"


def test_every_foreign_key_points_at_a_declared_key(foreign_keys, keys):
    """A FK to an unconstrained column is a relationship the optimiser and Power BI will not trust.

    Fabric does not enforce this (it does not enforce anything here), so a FK referencing an
    arbitrary column deploys cleanly and then quietly fails to do the two jobs FKs are declared
    for in this repo — join elimination and relationship detection. That silence is the reason to
    assert it.
    """
    declared = {(k["table"], tuple(k["cols"])) for k in keys}
    for fk in foreign_keys:
        assert (fk["ref_table"], tuple(fk["ref_cols"])) in declared, (
            f"{fk['name']} references {fk['ref_table']}{tuple(fk['ref_cols'])}, "
            "which is not a declared PRIMARY KEY or UNIQUE constraint"
        )


# --------------------------------------------------------------------------------------
# Dimensional-modelling invariants
# --------------------------------------------------------------------------------------

def test_fact_dimension_keys_are_not_nullable(tables, foreign_keys):
    """The executable form of the unknown-member decision.

    A nullable dimension key on a fact means an unresolved lookup becomes NULL, and a NULL key
    drops the fact from every measure sliced by that dimension — silently, with no error and no
    row count to notice. The whole point of the -1 unknown member in `02_dimensions.sql` is that
    this cannot happen, and NOT NULL is what makes that decision impossible to forget in a later
    loader.
    """
    for fk in foreign_keys:
        if fk["table"] not in FACT_TABLES:
            continue
        for col in fk["cols"]:
            assert not tables[fk["table"]][col], (
                f"{fk['table']}.{col} is nullable; fact dimension keys must resolve to the "
                "unknown member (-1) instead of NULL"
            )


def test_every_fact_and_dimension_has_a_primary_key(tables, keys):
    """Scoped to `dbo`, and the scope is the interesting part.

    `stg.*` mirrors the silver Delta schemas and carries the same `dim_`/`fact_` names, so the
    obvious form of this test — every table whose name starts with `dim_` — demands primary keys on
    seven staging tables that `06_staging.sql` says in as many words should have none: "No
    constraints, no keys, no statistics". It is right about that. A Fabric `NOT ENFORCED` key is
    metadata for the optimiser and a statement of intent to a reader; a truncate-and-fill table that
    exists for the duration of one load has no readers to inform and no plan worth shaping, and
    declaring keys on it would assert a uniqueness the staging load does not guarantee — silver's
    dedupe does, one layer earlier.

    So the model is `dbo`, and staging is not part of it. Naming that here rather than widening the
    prefix list keeps the next `stg` table from silently acquiring an obligation.
    """
    pk_tables = {k["table"] for k in keys if k["kind"] == "PRIMARY KEY"}
    modelled = {
        t for t in tables
        if t.startswith("dbo.") and t.split(".")[1].startswith(("dim_", "fact_", "agg_"))
    }
    assert modelled, "no modelled tables found — the DDL fixture is not parsing"
    assert modelled - pk_tables == set(), f"no PRIMARY KEY declared on: {sorted(modelled - pk_tables)}"


def test_scd2_dimensions_are_keyed_on_business_key_plus_validity(tables, keys):
    """An SCD2 dimension holds several rows per business key, so the business key alone cannot be
    unique. Asserting the *composite* alternate key is what distinguishes "SCD2 was implemented"
    from "SCD2 was intended and the loader overwrites".
    """
    unique = {(k["table"], tuple(k["cols"])) for k in keys if k["kind"] == "UNIQUE"}
    for table, business_key in (
        ("dbo.dim_account", "account_id"),
        ("dbo.dim_customer", "customer_id"),
        ("dbo.dim_merchant", "merchant_id"),
    ):
        assert (table, (business_key, "valid_from")) in unique, (
            f"{table} needs a UNIQUE ({business_key}, valid_from) constraint"
        )
        assert (table, (business_key,)) not in unique, (
            f"{table} declares {business_key} unique on its own, which is false for SCD2"
        )


def test_security_predicate_columns_exist_on_every_filtered_table(tables, ddl):
    """Row-level security is the one control here with no local test and no fallback proof, so the
    little that *can* be checked statically is worth checking: that every table the policy filters
    actually carries the column the predicate is applied to.

    A typo here would deploy (the predicate is bound at policy creation, but the column reference
    is per-table) and leave a table either unfilterable or filtered on the wrong column — and
    unlike a wrong join, an over-permissive filter produces no visible symptom at all.
    """
    predicates = re.findall(
        r"ADD\s+FILTER\s+PREDICATE\s+[\w\.]+\s*\(\s*(?P<col>\w+)\s*\)\s*ON\s+(?P<table>[\w\[\]\.]+)",
        _strip_comments(ddl), re.IGNORECASE,
    )
    assert len(predicates) >= 3, predicates
    for col, table in predicates:
        table = _qualify(table)
        assert table in tables, f"filter predicate on unknown table {table}"
        assert col.lower() in tables[table], f"{table} has no column {col!r} to filter on"
        assert not tables[table][col.lower()], (
            f"{table}.{col} is nullable; a NULL never satisfies the predicate, so those rows "
            "become invisible to every user rather than to the right ones"
        )


# --------------------------------------------------------------------------------------
# The nullability parse itself
# --------------------------------------------------------------------------------------

def test_explicit_null_is_distinguished_from_not_null():
    """Pins the sqlglot quirk described in `_is_nullable`.

    This is a test about a library's AST rather than about the repo's SQL, which normally would not
    earn a place here. It earns one because getting it wrong is silent in exactly one direction: it
    makes every "must not be nullable" assertion in this file pass, so the suite goes green while
    checking nothing. A test that fails when sqlglot changes its representation is cheaper than
    discovering that again.
    """
    tree = sqlglot.parse_one(
        "CREATE TABLE t (a int NULL, b int NOT NULL, c int)", dialect="tsql"
    )
    parsed = {col.name: _is_nullable(col) for col in tree.this.expressions}
    assert parsed == {"a": True, "b": False, "c": True}


def test_silver_mirror_tables_declare_every_column_nullable(tables):
    """`06_staging.sql` says it in prose; this asserts it.

    A mirror mirrors silver, and silver legitimately holds NULLs — `active_to` on a current card
    product, `resolved_date` on an open dispute, `merchant_id` on an ATM withdrawal. A NOT NULL on a
    mirror column would reject those rows at the bridge, which on Fabric is a failed cross-database
    INSERT with no obvious connection to the contract that permits the NULL.

    `stg.load_log` is deliberately excluded, and the exclusion is the useful part: `stg` holds two
    kinds of table. Seven are mirrors of silver, owned by the load and shaped by the source contract.
    The eighth is the warehouse's own run log, written by the procs about themselves — a row with no
    `proc_name` or no `status` is not a permissive mirror of anything, it is a log entry that cannot
    be read. So the mirrors take their nullability from silver and `load_log` takes its from what a
    log needs to be useful.

    This is also the positive form of the check that was missing: every other nullability assertion
    in this file asserts NOT NULL, so none of them could detect a parser that reported NOT NULL for
    everything. This one fails in that case.
    """
    staging = {
        t: cols for t, cols in tables.items()
        if t.startswith("stg.") and t != "stg.load_log"
    }
    assert len(staging) == 7, sorted(staging)
    offenders = {
        f"{t}.{c}" for t, cols in staging.items() for c, nullable in cols.items() if not nullable
    }
    assert offenders == set(), (
        f"staging columns declared NOT NULL: {sorted(offenders)} — staging mirrors silver, which "
        "legitimately holds NULLs"
    )
