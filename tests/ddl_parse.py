"""Reading the warehouse DDL — shared by every test that needs to know the star's actual shape.

This was extracted from `tests/test_warehouse_ddl.py` when a second module needed the same parse.
`tests/test_semantic_model.py` checks the TMDL against the warehouse, which means it needs the
same column lists and the same foreign keys — and copying the parse would have created exactly the
divergence risk that this repo has already been bitten by five times in the form of stale
citations. One parser, two callers.

The parsing strategy and its two sqlglot quirks are explained at length in
`tests/test_warehouse_ddl.py`'s module docstring, which is the right place for it: that file is
where the reasoning is load-bearing. The short version, because a reader who lands here first
deserves it:

- `CREATE TABLE` is read with **sqlglot**, which models columns and their nullability properly.
- Constraints are read with **regular expressions**, because ``ALTER TABLE ... ADD CONSTRAINT ...
  NOT ENFORCED`` — the only form Fabric accepts, so the only form in this repo — falls back to an
  opaque `Command` node in sqlglot's TSQL dialect. An AST-only version would check nothing.
- An unparseable `CREATE TABLE` **raises**. A table that cannot be parsed is a table exempt from
  every assertion in every caller, which is worse than a red test.

These are plain functions, not fixtures, so any module can call them. Callers are expected to wrap
them in their own module-scoped fixtures — the parse is fast, but it is not free, and it has no
reason to run once per test.
"""
from __future__ import annotations

import re
from pathlib import Path

import sqlglot
from sqlglot import exp

DDL_DIR = Path(__file__).resolve().parents[1] / "src" / "warehouse" / "ddl"

# Facts and the aggregate. Kept as an explicit list rather than a `startswith("fact_")` test so
# that adding a fact table is a deliberate decision about which invariants apply to it.
FACT_TABLES = {"dbo.fact_transaction", "dbo.fact_dispute", "dbo.agg_merchant_daily"}


def strip_comments(sql: str) -> str:
    """Remove `--` comments. Needed before splitting on `;`, and before matching constraint
    syntax — this repo's DDL carries more prose than SQL, and several comments contain the word
    `CONSTRAINT` while describing one.
    """
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def statements(sql: str) -> list[str]:
    return [s.strip() for s in strip_comments(sql).split(";") if s.strip()]


def qualify(name: str) -> str:
    """`dbo.t` unchanged; bare `t` assumed to be in `dbo`, matching T-SQL's default schema."""
    name = name.replace("[", "").replace("]", "").lower()
    return name if "." in name else f"dbo.{name}"


def cols(raw: str) -> list[str]:
    return [c.strip().replace("[", "").replace("]", "").lower() for c in raw.split(",") if c.strip()]


def is_nullable(col: exp.ColumnDef) -> bool:
    """Whether a parsed column accepts NULL.

    The `allow_null` check is the whole point. sqlglot represents an **explicit** ``NULL`` as
    ``NotNullColumnConstraint(allow_null=True)`` — the same node as ``NOT NULL``, differing only in
    that argument. An earlier version of this code tested only the node type, and so read every
    column in the DDL as NOT NULL. Every nullability assertion in `tests/test_warehouse_ddl.py`
    asserts that something is *not* nullable, which meant all of them passed unconditionally and
    none of them could ever have caught the thing they exist to catch.

    It was found by `src/lib/gold.py` creating the staging tables from this same parse and failing on
    a NOT NULL violation for a column the DDL declares ``NULL``. Worth recording as the pattern it
    is: a static check that agreed with itself for months, corrected the moment something executed
    against it.
    """
    return not any(
        isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null")
        for c in col.constraints
    )


CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
MASK_CLAUSE = re.compile(
    r"\s+MASKED\s+WITH\s*\(\s*FUNCTION\s*=\s*'(?:[^']|'')*'\s*\)", re.IGNORECASE
)
# `ALTER TABLE <t> ADD CONSTRAINT <n> PRIMARY KEY|UNIQUE ... (<cols>) NOT ENFORCED`
KEY_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[\w\[\]\.]+)\s+ADD\s+CONSTRAINT\s+(?P<name>\w+)\s+"
    r"(?P<kind>PRIMARY\s+KEY|UNIQUE)\b[^(]*\((?P<cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
# `... FOREIGN KEY (<cols>) REFERENCES <t2> (<cols2>) NOT ENFORCED`
FK_RE = re.compile(
    r"ALTER\s+TABLE\s+(?P<table>[\w\[\]\.]+)\s+ADD\s+CONSTRAINT\s+(?P<name>\w+)\s+"
    r"FOREIGN\s+KEY\s*\((?P<cols>[^)]*)\)\s*REFERENCES\s+(?P<ref_table>[\w\[\]\.]+)\s*"
    r"\((?P<ref_cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
STATS_RE = re.compile(
    r"CREATE\s+STATISTICS\s+(?P<name>\w+)\s+ON\s+(?P<table>[\w\[\]\.]+)\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)


def read_ddl(ddl_dir: Path = DDL_DIR) -> str:
    """Every `.sql` file under the DDL directory, concatenated in filename order.

    Filename order is deployment order (`01_schemas`, `02_dimensions`, ...), which matters for a
    reader but not for any parse here — nothing in this module is order-sensitive.
    """
    files = sorted(ddl_dir.glob("*.sql"))
    assert files, f"no DDL found under {ddl_dir}"
    return "\n".join(f.read_text() for f in files)


def parse_tables(ddl: str) -> dict[str, dict[str, bool]]:
    """`{qualified table: {column: is_nullable}}`, from every CREATE TABLE in the DDL.

    Parsed one statement at a time and filtered to ``CREATE TABLE`` first — see
    `tests/test_warehouse_ddl.py`'s module docstring for why a whole-file ``sqlglot.parse`` does
    not survive this DDL.
    """
    out: dict[str, dict[str, bool]] = {}
    for stmt in statements(ddl):
        if not CREATE_TABLE_RE.match(stmt):
            continue
        try:
            tree = sqlglot.parse_one(MASK_CLAUSE.sub("", stmt), dialect="tsql")
        except Exception as exc:  # noqa: BLE001 — re-raised immediately with context
            raise AssertionError(
                f"a CREATE TABLE statement could not be parsed, so it would have been exempt "
                f"from every check that uses this parse ({type(exc).__name__}: {exc}):\n"
                f"{stmt[:400]}"
            ) from exc
        if not isinstance(tree, exp.Create) or (tree.kind or "").upper() != "TABLE":
            continue
        schema = tree.this
        name = qualify(schema.this.sql(dialect="tsql"))
        columns: dict[str, bool] = {}
        for col in schema.expressions:
            if not isinstance(col, exp.ColumnDef):
                continue
            columns[col.name.lower()] = is_nullable(col)
        out[name] = columns
    return out


def parse_keys(ddl: str) -> list[dict]:
    """Every PRIMARY KEY and UNIQUE constraint, as `{table, name, cols, kind}`."""
    return [
        {"table": qualify(m["table"]), "name": m["name"], "cols": cols(m["cols"]),
         "kind": " ".join(m["kind"].upper().split())}
        for stmt in statements(ddl)
        for m in [KEY_RE.search(stmt)] if m
    ]


def parse_foreign_keys(ddl: str) -> list[dict]:
    """Every FOREIGN KEY, as `{table, name, cols, ref_table, ref_cols}`."""
    return [
        {"table": qualify(m["table"]), "name": m["name"], "cols": cols(m["cols"]),
         "ref_table": qualify(m["ref_table"]), "ref_cols": cols(m["ref_cols"])}
        for stmt in statements(ddl)
        for m in [FK_RE.search(stmt)] if m
    ]
