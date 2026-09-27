"""Local execution harness for the gold layer — and the bridge that feeds it.

`docs/gold-execution.md` records the decision this module implements. The short version: the
SQL Server container never pulled, so there is no T-SQL engine on this machine. The ten procedures
in `src/warehouse/procs/` are still the deliverable — they are what would be deployed to a Fabric
Warehouse — and this module executes *them*, statement by statement, against the silver Delta
tables via Spark SQL.

**It is a harness, not a second implementation.** That distinction is the whole design:

- Nothing here restates the procs' logic. Every `INSERT`, `MERGE`, `UPDATE` and `DELETE` executed
  below is read out of the `.sql` file, transpiled by sqlglot from `tsql` to `spark`, and run. If
  a proc is edited, this module executes the edit; there is no second copy to keep in sync, and no
  way for the local numbers to be right while the shipped T-SQL is wrong.
- The DDL is translated the same way: `src/warehouse/ddl/*.sql`'s `CREATE TABLE` statements are
  parsed and re-emitted as Delta tables. The column names, order, types and nullability that gold
  runs against are the ones in the deliverable DDL, not a hand-written mirror of it.

**What that buys, stated precisely.** A green reconciliation test proves *this logic produces the
right numbers*. It does not prove *this T-SQL runs on Fabric Warehouse* — sqlglot happily transpiles
constructs Fabric would reject, and Spark happily runs them. That second claim belongs entirely to
`tools/fabric_tsql_lint.py`. The two claims are kept apart in `docs/gold-execution.md` and they are
kept apart here: this module never reports on portability, and the linter never reports on numbers.

**What the translation loses, and why each loss is acceptable.**

1. *Transactions.* `BEGIN TRAN` / `COMMIT TRAN` are skipped. Delta gives per-statement atomicity
   and no multi-statement transaction, so a proc that fails halfway leaves gold partially loaded
   where Fabric would roll back. Every proc is written to be re-runnable from the top for exactly
   this reason — delete-then-insert keyed on the business key, `MERGE` on the alternate key — so
   the repair for a half-load is to run it again, which is also the repair on Fabric.
2. *`TRY` / `CATCH` / `THROW`.* Skipped. A statement that fails raises a Python exception instead,
   which propagates out of `load()` and fails the run — the same outcome by a different route. The
   `FAILED` row the CATCH would have written to `stg.load_log` is written here by `run_proc`'s own
   `except` clause, so the log still records the failure.
3. *`IF` guards.* Skipped, because there is no procedural evaluator here. This is the one skip that
   could silently change behaviour, so it is checked rather than trusted: `_assert_no_conditional_dml`
   refuses to run a proc that has DML inside an `IF` block, and the one real guard in the repo —
   `sp_load_dim_date`'s span check — is re-implemented in `dim_date_range` so the invariant survives.
4. *`varchar` length and `NOT NULL`.* Both survive, because the Delta tables are created from the
   DDL's own column definitions. Spark enforces `VARCHAR(n)` on write, so a value silver produces
   that Fabric would reject fails here too. That is fidelity worth having and it is why the types
   are not flattened to `STRING`.

**Two rewrites are applied to the transpiled SQL**, both because Delta is narrower than Fabric
Warehouse rather than wider, and both recorded on the statement so `ProcResult` can report them:

- `TRUNCATE TABLE t` → `DELETE FROM t`. Delta rejects `TRUNCATE` on a table with an external
  location; an unpredicated `DELETE` is the same thing.
- `DELETE FROM t WHERE c IN (<subquery>)` → `MERGE INTO t USING (<subquery>) ON t.c = s.c WHEN
  MATCHED THEN DELETE`. Delta rejects subqueries in a `DELETE` predicate. This is the canonical
  workaround and it is semantically exact. The rewrite only accepts that one shape and raises on
  any other subquery-bearing `DELETE`, so a future proc cannot quietly have its delete dropped.

Anything else that fails, fails. Widening this list is a decision, not a fix.
"""
from __future__ import annotations

import importlib.util
import logging
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import sqlglot
from sqlglot import exp

from src.runtime.context import Layer, config, get_spark, read_table, table_exists

log = logging.getLogger("gold")

REPO_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DIR = REPO_ROOT / "src" / "warehouse"
DDL_DIR = WAREHOUSE_DIR / "ddl"
PROC_DIR = WAREHOUSE_DIR / "procs"


class GoldError(RuntimeError):
    """Raised when a proc cannot be translated. Deliberately not a subclass of anything the
    caller might already be catching: a translation failure is a defect in this module or in the
    proc, never a data condition, and it must not be mistaken for one."""


# --------------------------------------------------------------------------------------
# Statement splitting — borrowed from the linter, on purpose
# --------------------------------------------------------------------------------------
# `_sql_statements` is the linter's own string- and bracket-aware `;` scanner. Importing it rather
# than writing a second splitter is the point: the scanner that decides what the linter *checks*
# is the scanner that decides what this module *executes*. A bug in it can no longer be invisible —
# it used to be able to drop a statement from the AST pass with no symptom, and it did; now the same
# bug drops a statement from the load and the reconciliation test goes red.
#
# Loaded by path rather than as `tools.fabric_tsql_lint` because `tools/` is a scripts directory,
# not a package, and an implicit-namespace import of it would depend on the process's CWD.

def _load_linter():
    spec = importlib.util.spec_from_file_location(
        "_fabric_tsql_lint", REPO_ROOT / "tools" / "fabric_tsql_lint.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover — defensive
        raise GoldError("could not load tools/fabric_tsql_lint.py")
    mod = importlib.util.module_from_spec(spec)
    # Registered before execution because the linter defines `@dataclass`es, and `dataclasses`
    # resolves a field's type by looking the defining module up in `sys.modules` — which fails
    # with an opaque `NoneType has no attribute __dict__` if the module is not there yet.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_LINTER = None


def _sql_statements(sql: str) -> list[tuple[str, int]]:
    global _LINTER
    if _LINTER is None:
        _LINTER = _load_linter()
    return _LINTER._sql_statements(sql)


# --------------------------------------------------------------------------------------
# Where gold lives
# --------------------------------------------------------------------------------------
# Under `_onelake/` because a Fabric Warehouse's storage *is* OneLake — the tables are Delta either
# way, and the difference is the endpoint in front of them, not the substrate beneath.
#
# There is deliberately no `Layer.GOLD` in `src/runtime/context.py`. `Layer` resolves to a lakehouse
# item (`lh_silver.dim_account`) on Fabric, and gold is not a lakehouse: it is a Warehouse addressed
# over T-SQL, whose tables are never read by Spark there. Adding a GOLD member would make
# `table_ref` return `lh_gold.fact_transaction`, an item that does not exist in the deployment this
# repo describes. So gold owns its own path resolution, here, and the absence is the design.

SCHEMAS: tuple[str, ...] = ("dbo", "stg", "sec")


def gold_root() -> Path:
    return config().onelake_root / "gold"


def _location(schema: str, table: str) -> Path:
    return gold_root() / schema / table


# --------------------------------------------------------------------------------------
# DDL translation
# --------------------------------------------------------------------------------------

_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
# `MASKED WITH (FUNCTION = '...')` is a parse error for sqlglot and irrelevant here — masking is a
# presentation control on a serving endpoint, and there is no serving endpoint locally. Stripping it
# keeps `dim_customer`, the only table in the warehouse carrying PII, inside the translation instead
# of silently absent from it.
_MASK_CLAUSE = re.compile(
    r"\s+MASKED\s+WITH\s*\(\s*FUNCTION\s*=\s*'(?:[^']|'')*'\s*\)", re.IGNORECASE
)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    spark_type: str
    nullable: bool


@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    columns: tuple[ColumnSpec, ...]

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def create_sql(self) -> str:
        cols = ", ".join(
            f"{c.name} {c.spark_type}{'' if c.nullable else ' NOT NULL'}" for c in self.columns
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.qualified} ({cols}) "
            f"USING DELTA LOCATION '{_location(self.schema, self.name)}'"
        )


def _is_nullable(col: exp.ColumnDef) -> bool:
    """Whether a parsed column accepts NULL.

    sqlglot models an **explicit** `NULL` as `NotNullColumnConstraint(allow_null=True)` — the same
    node type as `NOT NULL`, distinguished only by that argument. An `isinstance` test alone
    therefore reads every column in `06_staging.sql` (where every column is explicitly `NULL`) as
    NOT NULL. That is not a cosmetic misread: it is what made the first run of `fill_staging` fail
    with a NOT NULL violation on `stg.dim_card_product.active_to`, a column the DDL and
    `docs/data-contracts.md` both declare nullable.

    The same mistake is in the history of `tests/test_warehouse_ddl.py`, and it mattered more there:
    its nullability assertions are all of the form "this column must not be nullable", so a parser
    that reports everything as NOT NULL makes every one of them pass unconditionally. Both are fixed,
    and `test_explicit_null_is_distinguished_from_not_null` pins the behaviour so neither can drift
    back.
    """
    return not any(
        isinstance(c.kind, exp.NotNullColumnConstraint) and not c.kind.args.get("allow_null")
        for c in col.constraints
    )


def warehouse_tables() -> dict[str, TableSpec]:
    """Every `CREATE TABLE` in `ddl/`, as a Spark-typed spec.

    The type mapping is sqlglot's: each column's parsed `DataType` is re-emitted in the `spark`
    dialect. `bit` becomes `BOOLEAN`, `datetime2(6)` becomes `TIMESTAMP`, `decimal(18,8)` survives
    intact, and `varchar(20)` stays `VARCHAR(20)` rather than being flattened to `STRING` — see the
    module docstring on why keeping the length is fidelity and not pedantry.

    An unparseable `CREATE TABLE` raises. Skipping one would exempt a table from the load while
    leaving every other table's numbers looking right, which is the failure mode that made
    `tests/test_warehouse_ddl.py` insist on the same thing.
    """
    out: dict[str, TableSpec] = {}
    files = sorted(DDL_DIR.glob("*.sql"))
    if not files:
        raise GoldError(f"no DDL found under {DDL_DIR}")
    for path in files:
        for text, line in _sql_statements(path.read_text()):
            if not _CREATE_TABLE_RE.match(text):
                continue
            try:
                tree = sqlglot.parse_one(_MASK_CLAUSE.sub("", text), dialect="tsql")
            except Exception as exc:  # noqa: BLE001 — re-raised with the location
                raise GoldError(
                    f"{path.name}:{line}: CREATE TABLE could not be parsed, so the table would "
                    f"have been missing from gold ({type(exc).__name__}: {exc})"
                ) from exc
            if not isinstance(tree, exp.Create) or (tree.kind or "").upper() != "TABLE":
                continue
            raw = tree.this.this.sql(dialect="tsql").replace("[", "").replace("]", "")
            schema, _, name = raw.rpartition(".")
            schema = (schema or "dbo").lower()
            name = name.lower()
            if schema not in SCHEMAS:
                raise GoldError(f"{path.name}:{line}: unknown schema {schema!r} for table {name!r}")
            cols = tuple(
                ColumnSpec(
                    name=col.name.lower(),
                    spark_type=col.args["kind"].sql(dialect="spark"),
                    nullable=_is_nullable(col),
                )
                for col in tree.this.expressions
                if isinstance(col, exp.ColumnDef)
            )
            if not cols:
                raise GoldError(f"{path.name}:{line}: {schema}.{name} parsed with no columns")
            out[f"{schema}.{name}"] = TableSpec(schema, name, cols)
    return out


def _registered_location(schema: str) -> Path | None:
    """Where the Spark catalog currently thinks `schema` lives, or None if it is not registered.

    The Spark catalog is per *session* while `gold_root()` is per `ONELAKE_ROOT`, so the two can
    disagree — and every function that writes or drops has to ask before acting. See the callers.
    """
    spark = get_spark()
    if not spark.catalog.databaseExists(schema):
        return None
    got = next(
        r["info_value"]
        for r in spark.sql(f"DESCRIBE DATABASE EXTENDED {schema}").collect()
        if r["info_name"] == "Location"
    )
    return Path(got.removeprefix("file:")).resolve()


def ensure_schema() -> list[str]:
    """Create the Spark databases and the Delta tables, idempotently.

    The databases are named `dbo`, `stg` and `sec` so that the transpiled T-SQL runs **verbatim**:
    a proc that says `FROM stg.fact_transaction` needs no rewriting, and nothing in this module
    touches a table name. Rewriting two-part names into flat ones would have been easier and would
    have put a name-mangling step between the deliverable SQL and what actually executed.
    """
    spark = get_spark()
    for schema in SCHEMAS:
        want = gold_root() / schema
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{want}'")
        # `IF NOT EXISTS` is silent about the LOCATION when the database already exists, so a test
        # that repoints the runtime at a temporary lake and calls this function gets a `dbo` still
        # pointing at the committed `_onelake/gold/` — and then writes the demo warehouse. Nothing
        # about that failure looks like a failure, so the location is read back and checked.
        got = _registered_location(schema)
        if got != want.resolve():
            raise GoldError(
                f"database {schema} is registered at {got}, but this lake's gold root is {want}. "
                "The Spark catalog outlives a change of ONELAKE_ROOT; call gold.drop_gold() before "
                "pointing the runtime at a different lake, or the load will write into the wrong "
                "warehouse."
            )
    created = []
    for spec in warehouse_tables().values():
        spark.sql(spec.create_sql())
        created.append(spec.qualified)
    log.info("gold schema ready: %s table(s) across %s", len(created), ", ".join(SCHEMAS))
    return created


def drop_gold() -> None:
    """Delete the gold tables and their data. Used by the tests and by `make run --rebuild`.

    Refuses a schema the catalog has registered somewhere other than this lake's gold root. That is
    the mirror of the guard in `ensure_schema`, and it is here for the same reason: the caller that
    most needs to drop gold is a test that has just repointed `ONELAKE_ROOT`, which is exactly the
    situation in which the registration still names the committed warehouse. Unregistering that one
    is recoverable — `ensure_schema` re-attaches it — but the `rmtree` below is not, and a function
    whose safety depends on which of its two halves runs first is not safe.
    """
    spark = get_spark()
    want = gold_root().resolve()
    for schema in SCHEMAS:
        got = _registered_location(schema)
        if got is not None and got != want / schema:
            raise GoldError(
                f"refusing to drop {schema}: it is registered at {got}, not under this lake's gold "
                f"root {want}. Drop it while ONELAKE_ROOT still points at that lake."
            )
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
    shutil.rmtree(want, ignore_errors=True)


# --------------------------------------------------------------------------------------
# The bridge — silver Delta into stg.*
# --------------------------------------------------------------------------------------
# `06_staging.sql` names this the single seam between the two substrates, and it is the only step
# in the whole pipeline that disappears on Fabric: there, `stg` is filled by cross-database query
# against the lakehouse SQL analytics endpoint, in the same transaction as the load.
#
# WHICH TABLES ARE MIRRORED IN FULL, AND WHY IT IS NOT ABOUT SIZE
#
# The obvious contract is "stage this batch" — mirror the rows silver wrote under this batch id and
# let the procs merge them. That is correct for a table silver only ever *appends* to or *restamps*
# on update, and wrong for one silver mutates in place without restamping.
#
# `src/lib/scd2.py` is the second kind. Its close-out branch is a `whenMatchedUpdate` that sets
# `valid_to`, `is_current` and `_updated_ts` — and deliberately not `_batch_id`, because `_batch_id`
# records which batch *created* the version. So a version created by batch 4 and closed by batch 9
# still carries `_batch_id = 4`, and a mirror filtered to batch 9 would not contain it. Gold would
# then keep `valid_to = 9999-12-31` and `is_current = 1` on a version silver had closed, forever,
# with two current rows for one account and no error anywhere. `03_sp_load_dim_account`'s MERGE
# exists precisely to re-sync that close-out; it cannot re-sync a row that was never staged.
#
# So the three SCD2 dimensions are mirrored in full. The rule generalises, and is the reason this
# list is a table with a stated mode rather than a loop over config:
#
#   append-only, or restamped on update  → mirror the batch
#   mutated in place without restamping  → mirror in full
#
# The `scd_type = 0` feeds are the first kind: `nb_02_silver_transform`'s MERGE is a
# `whenMatchedUpdateAll`, which rewrites `_silver_batch_id` along with everything else, so an
# updated row moves into the new batch and a batch filter finds it.
#
# `dim_fx_rate` is mirrored in full for a third, unrelated reason: `06_sp_load_dim_fx_rate` rebuilds
# the whole dimension because it derives each rate's validity interval by looking at the *next*
# rate, and `07_sp_load_fact_transaction` resolves rates by interval across the entire history. A
# batch of one day's rates cannot produce either.
#
# `dim_card_product` is SCD1 — silver overwrites the table, so every row carries the current batch
# and full and batch-filtered are the same set. It is listed as `full` because that is what the
# semantics are, not because the filter would have failed.


@dataclass(frozen=True)
class StagingSpec:
    silver: str
    staging: str
    mode: str  # "batch" | "full"
    batch_column: str
    why: str


STAGING: tuple[StagingSpec, ...] = (
    StagingSpec("dim_card_product", "stg.dim_card_product", "full", "_silver_batch_id",
                "SCD1 overwrite — the whole table is one batch"),
    StagingSpec("dim_account", "stg.dim_account", "full", "_batch_id",
                "SCD2 close-out does not restamp _batch_id"),
    StagingSpec("dim_customer", "stg.dim_customer", "full", "_batch_id",
                "SCD2 close-out does not restamp _batch_id"),
    StagingSpec("dim_merchant", "stg.dim_merchant", "full", "_batch_id",
                "SCD2 close-out does not restamp _batch_id"),
    StagingSpec("dim_fx_rate", "stg.dim_fx_rate", "full", "_silver_batch_id",
                "validity intervals are derived across the whole rate history"),
    StagingSpec("fact_transaction", "stg.fact_transaction", "batch", "_silver_batch_id",
                "MERGE restamps _silver_batch_id on update"),
    StagingSpec("fact_dispute", "stg.fact_dispute", "batch", "_silver_batch_id",
                "MERGE restamps _silver_batch_id on update"),
)


def fill_staging(batch_ids: dict[str, str] | None = None) -> dict[str, int]:
    """Truncate and refill every `stg.*` mirror from silver. Returns rows staged per table.

    `batch_ids` maps a silver table name to the batch id to stage; tables in `full` mode ignore it,
    and a `batch` table with no entry is staged in full with a warning. That default is deliberate:
    a missing batch id should over-stage, which the procs tolerate because every one of them is
    idempotent, rather than under-stage, which would silently drop rows from gold.

    The column list comes from the DDL, and the write is a positional `insertInto`. Both matter:
    if `06_staging.sql`'s mirror of a silver schema is wrong — a missing column, a renamed one, a
    narrower type — this fails here rather than producing a gold table full of plausible nulls. It
    is the only check in the repo that the staging DDL and the silver schemas actually agree.
    """
    spark = get_spark()
    specs = warehouse_tables()
    batch_ids = batch_ids or {}
    staged: dict[str, int] = {}

    for s in STAGING:
        spec = specs.get(s.staging)
        if spec is None:
            raise GoldError(f"{s.staging} is not declared in the warehouse DDL")
        if not table_exists(Layer.SILVER, s.silver):
            raise GoldError(
                f"silver table {s.silver!r} does not exist — run nb_02_silver_transform first"
            )

        df = read_table(Layer.SILVER, s.silver)
        if s.mode == "batch":
            batch = batch_ids.get(s.silver)
            if batch is None:
                log.warning(
                    "%s: no batch id supplied, staging the whole table (see fill_staging docstring)",
                    s.silver,
                )
            else:
                df = df.filter(df[s.batch_column] == batch)

        missing = [c for c in spec.column_names if c not in df.columns]
        if missing:
            raise GoldError(
                f"{s.staging}: silver table {s.silver!r} has no column(s) {missing} — the staging "
                f"DDL and the silver schema disagree"
            )

        spark.sql(f"DELETE FROM {s.staging}")
        df.select(*spec.column_names).write.insertInto(s.staging, overwrite=False)
        staged[s.staging] = spark.table(s.staging).count()
        log.info("staged %-24s %8d rows  (%s: %s)", s.staging, staged[s.staging], s.mode, s.why)
    return staged


# --------------------------------------------------------------------------------------
# The proc interpreter
# --------------------------------------------------------------------------------------

_SCAFFOLDING = re.compile(
    r"^\s*(CREATE\s+PROCEDURE|DECLARE|SET\s+@|BEGIN\s+TRY|END\s+TRY|BEGIN\s+CATCH|END\s+CATCH"
    r"|BEGIN\s+TRAN|COMMIT\s+TRAN|ROLLBACK\s+TRAN|THROW|IF\b|BEGIN\b|END\b)",
    re.IGNORECASE,
)
_DML = re.compile(r"^\s*(WITH|INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b", re.IGNORECASE)
_SELECT_INTO_VAR = re.compile(r"^\s*SELECT\s+@(?P<var>\w+)\s*=\s*(?P<expr>.*)$", re.IGNORECASE | re.DOTALL)
_DECLARE = re.compile(
    r"DECLARE\s+@(?P<var>\w+)\s+(?P<type>[A-Za-z0-9_]+(?:\s*\(\s*[\w,\s]+\s*\))?)"
    r"(?:\s*=\s*(?P<init>.+?))?(?=;|$)",
    re.IGNORECASE | re.DOTALL,
)
_VAR = re.compile(r"@(\w+)")
# T-SQL escapes a quote by doubling it, so a literal is `'` then any run of non-quotes and `''`
# pairs, then `'`. Captured rather than discarded so `re.split` hands back the literals too.
_STRING_LITERAL = re.compile(r"('(?:[^']|'')*')")
# Fragments that open a nesting level. `IF ... BEGIN` is caught by the trailing-BEGIN test rather
# than a prefix, because the condition sits between the two words.
_OPENS = re.compile(r"^\s*(BEGIN\s+TRY|BEGIN\s+CATCH|BEGIN)\s*$", re.IGNORECASE)
_CLOSES = re.compile(r"^\s*(END\s+TRY|END\s+CATCH|END)\b", re.IGNORECASE)
# Searched anywhere in a fragment rather than anchored: the statement splitter fuses
# `END TRY BEGIN CATCH IF XACT_STATE() <> 0 ROLLBACK TRAN` into one fragment, and `END CATCH END`
# into another, because neither contains a `;`.
_BEGIN_CATCH = re.compile(r"\bBEGIN\s+CATCH\b", re.IGNORECASE)
_END_CATCH = re.compile(r"\bEND\s+CATCH\b", re.IGNORECASE)


def _literal(value) -> str:
    """Render a Python value as a **T-SQL** literal, for substitution before transpiling.

    T-SQL, not Spark SQL, and the temporal cases are why. Substitution happens before the statement
    is transpiled — it has to, because a `@var` is not parseable — so every literal produced here is
    read by sqlglot in the `tsql` dialect. `TIMESTAMP '...'` is the obvious spelling and it is wrong:
    in T-SQL, `TIMESTAMP` is the deprecated synonym for `ROWVERSION`, a binary type, so that literal
    transpiles to `CAST('...' AS BINARY)` and Spark rejects the insert with a type mismatch on a
    column the proc declared `datetime2(6)`.

    `CAST('...' AS datetime2(6))` is the spelling the procs themselves use and it round-trips to
    `CAST('...' AS TIMESTAMP)`. Worth recording because the failure is loud here and would be silent
    in a system that coerced instead of rejecting.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, datetime):
        return f"CAST('{value.isoformat(sep=' ', timespec='microseconds')}' AS datetime2(6))"
    if isinstance(value, date):
        return f"CAST('{value.isoformat()}' AS date)"
    return "'" + str(value).replace("'", "''") + "'"


def _substitute(sql: str, env: dict[str, object]) -> str:
    """Replace every `@var` with its literal value, outside string literals.

    Skipping literals is not hypothetical tidiness: `00_sp_seed_reference_data` seeds the unknown
    member's email as `'unknown@example.invalid'`, and a naive scan reads `@example` as an
    undeclared variable and refuses to run the proc. This is the same shape of bug as the one that
    made the linter's statement splitter drop coverage after a semicolon inside a string — a scanner
    that does not know what a literal is will eventually look inside one.

    An unknown variable outside a literal is an error rather than a pass-through. Leaving
    `@rows_read` in the SQL would produce a Spark parse error some statements later, at a line with
    nothing to do with the cause; failing here names the variable and the proc.
    """
    def repl(m: re.Match) -> str:
        name = m.group(1)
        key = name.lower()
        if key not in env:
            raise GoldError(f"undeclared variable @{name}")
        return _literal(env[key])

    # `re.split` with a capturing group alternates non-literal, literal, non-literal, ...
    parts = _STRING_LITERAL.split(sql)
    return "".join(
        part if i % 2 else _VAR.sub(repl, part) for i, part in enumerate(parts)
    )


# sqlglot parses T-SQL `DATEPART(part, x)` / `DATENAME(part, x)` into a `TimeToStr` node carrying a
# strftime-style format, and the Spark generator then renders that format as a Spark datetime
# pattern. For several parts the result is not a Spark pattern at all. Verified by probe against
# Spark 3.5, keyed on the format literal sqlglot puts on the T-SQL-side tree:
#
#   part        literal      Spark rendering        verdict
#   dayofyear   '%j'         DATE_FORMAT(x,'DD')    correct — 'DD' is day-of-year in Spark
#   year        '%Y'         DATE_FORMAT(x,'yyyy')  correct (string; every call site CASTs it)
#   month       '%m'         DATE_FORMAT(x,'MM')    correct
#   day         '%d'         DATE_FORMAT(x,'dd')    correct
#   month name  '%B'         DATE_FORMAT(x,'MMMM')  correct
#   day name    '%A'         DATE_FORMAT(x,'EEEE')  correct
#   iso_week    'i%-So_%W'   DATE_FORMAT(x,'iso_%W')  raises: week-based pattern `W`
#   quarter     'quarter'    DATE_FORMAT(x,'quarter') raises: week-based pattern `u`
#   week        '%W'         DATE_FORMAT(x,'%W')      raises: week-based pattern `W`
#   weekday     '%w'         DATE_FORMAT(x,'%w')      raises: week-based pattern `w`
#
# The repairs below replace the four broken renderings with Spark functions that genuinely mean the
# same thing. `WEEKOFYEAR` is ISO-8601 week-of-year, which is what `DATEPART(iso_week, ...)` means;
# `QUARTER` returns 1-4. `week` and `weekday` are DATEFIRST-dependent in T-SQL and are deliberately
# not used anywhere in `src/warehouse/` (01_sp_load_dim_date.sql:36 and :167 say why) — they are
# mapped anyway so that the repair table documents the whole family rather than only the parts this
# repo happens to reach.
#
# Keys are the T-SQL-side literal, not the generated Spark pattern, because generating Spark SQL and
# re-parsing it corrupts the literal further: 'iso_%W' comes back as 'i%-So_%W' mangled again to
# 'i%-So_%W' -> 'iso_%W' -> 'i%-So_%W'. `_to_spark` therefore repairs the T-SQL tree and generates
# exactly once.
_DATEPART_REPAIRS: dict[str, str] = {
    "i%-So_%W": "WEEKOFYEAR",
    "quarter": "QUARTER",
    "%W": "WEEKOFYEAR",
    "%w": "DAYOFWEEK",
}

# Literals left alone because a probe confirmed Spark reads them as T-SQL means them. A literal in
# neither table is refused rather than executed — see `_repair_datepart`.
_DATEPART_VERIFIED: frozenset[str] = frozenset({"%j", "%Y", "%m", "%d", "%B", "%A"})


def _repair_datepart(tree: exp.Expression) -> exp.Expression:
    """Repair sqlglot's `DATEPART` renderings, and refuse any rendering not verified by probe.

    The refusal is the point. Both defects this repo actually hits happen to raise inside Spark,
    which made them loud — but that is a property of those two patterns, not of the translation. A
    date part nobody anticipated could just as easily render as a pattern Spark accepts and reads
    differently, and then the reconciliation would go green against wrong numbers. Whitelisting the
    verified literals converts that silent class into this loud one.

    Mutates and returns `tree`, which is a throwaway parse owned by `_to_spark`.
    """
    for node in list(tree.find_all(exp.TimeToStr)):
        fmt = node.args.get("format")
        literal = fmt.name if isinstance(fmt, exp.Literal) else None
        if literal in _DATEPART_VERIFIED:
            continue
        if literal in _DATEPART_REPAIRS:
            node.replace(exp.func(_DATEPART_REPAIRS[literal], node.this.copy()))
            continue
        raise GoldError(
            f"sqlglot rendered a date part with format {literal!r}, which no probe has verified "
            "against Spark. Check what Spark makes of it and add it to _DATEPART_VERIFIED or "
            f"_DATEPART_REPAIRS in src/lib/gold.py; do not assume it is right:\n{node.sql(dialect='tsql')}"
        )
    return tree


def _to_spark(sql: str) -> str:
    """Transpile one T-SQL statement to Spark SQL, repairing known sqlglot defects.

    Transpilation is not trusted blindly: `_repair_datepart` runs over the T-SQL tree and raises on
    any date rendering this repo has not verified by probe.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="tsql")
        return _repair_datepart(tree).sql(dialect="spark")
    except GoldError:
        raise
    except Exception as exc:  # noqa: BLE001 — re-raised with the statement
        raise GoldError(f"could not transpile to Spark SQL ({type(exc).__name__}: {exc}):\n{sql[:400]}") from exc


def _select_branches(node: exp.Expression) -> list[exp.Select]:
    """The `SELECT`s whose projections define a query's output columns.

    For a plain `SELECT` that is the node itself; for a `UNION` / `INTERSECT` / `EXCEPT` it is every
    branch, found recursively. Nested selects inside a branch's own `FROM` or `WHERE` are *not*
    returned, which is the difference between this and `find_all(exp.Select)` — those inner selects
    do not contribute output columns and aliasing their projections would be wrong.
    """
    if isinstance(node, exp.Subquery):
        return _select_branches(node.this)
    if isinstance(node, exp.SetOperation):
        return _select_branches(node.this) + _select_branches(node.expression)
    if isinstance(node, exp.Select):
        return [node]
    raise GoldError(f"cannot determine the output columns of a {type(node).__name__}")


def _rewrite_for_delta(sql: str) -> tuple[str, str | None]:
    """Apply the two Delta-narrowness rewrites. Returns `(sql, rewrite_name | None)`."""
    tree = sqlglot.parse_one(sql, dialect="spark")

    if isinstance(tree, exp.TruncateTable):
        tables = tree.expressions
        if len(tables) != 1:
            raise GoldError(f"TRUNCATE of {len(tables)} tables is not translated:\n{sql}")
        return f"DELETE FROM {tables[0].sql(dialect='spark')}", "truncate->delete"

    if isinstance(tree, exp.Delete):
        where = tree.args.get("where")
        subqueries = list(where.find_all(exp.Select)) if where else []
        if not subqueries:
            return sql, None
        cond = where.this
        if not (
            isinstance(cond, exp.In)
            and isinstance(cond.this, exp.Column)
            and cond.args.get("query") is not None
        ):
            raise GoldError(
                "Delta rejects subqueries in DELETE, and this one is not the `col IN (SELECT ...)` "
                f"shape the MERGE rewrite handles. Rewrite the proc or extend _rewrite_for_delta:\n{sql}"
            )
        target = tree.this.sql(dialect="spark")
        column = cond.this.name
        source = cond.args["query"].this.copy()

        # The MERGE references `__src.<column>`, so the subquery has to *project* that name. In a
        # `WHERE col IN (SELECT ...)` the projection needs no alias and in this repo does not have
        # one: proc 09 selects a computed date key, and Spark then names the column
        # `(((year(...) * 10000) + ...))`, which `ON __tgt.date_sk = __src.date_sk` cannot resolve.
        # So each branch's single projection is aliased to the target column's name. Every branch,
        # not just the first: a UNION takes its output names from the first branch, so aliasing only
        # that one would work, but it would leave the rewrite's correctness resting on which branch
        # sqlglot happens to put first.
        for branch in _select_branches(source):
            projections = branch.expressions
            if len(projections) != 1:
                raise GoldError(
                    f"the subquery in `{column} IN (...)` projects {len(projections)} columns; the "
                    f"MERGE rewrite needs exactly one to alias as {column}:\n{sql}"
                )
            if not isinstance(projections[0], exp.Alias):
                branch.set("expressions", [projections[0].as_(column)])

        return (
            f"MERGE INTO {target} AS __tgt USING ({source.sql(dialect='spark')}) AS __src "
            f"ON __tgt.{column} = __src.{column} WHEN MATCHED THEN DELETE"
        ), "delete-subquery->merge"

    return sql, None


def _assert_no_conditional_dml(fragments: list[tuple[str, int]], proc: str) -> None:
    """Refuse to run a proc whose DML sits inside an `IF` block.

    There is no procedural evaluator here, so an `IF` is skipped and its body is not. For the
    repo's actual guard clauses — which contain only `SET` and `THROW` — that is harmless. For a
    hypothetical `IF <condition> BEGIN INSERT ... END` it would execute the insert unconditionally,
    which is a wrong answer rather than an error. So the simplification is checked instead of
    assumed, and the check is here rather than in a test because a test can be skipped.
    """
    depth_at_if: list[int] = []
    depth = 0
    for text, line in fragments:
        stripped = text.strip()
        opening = bool(_OPENS.match(stripped)) or bool(
            re.search(r"\bBEGIN\s*$", stripped, re.IGNORECASE)
        )
        if depth_at_if and _DML.match(stripped):
            raise GoldError(
                f"{proc}:{line}: DML inside an IF block. The local harness has no procedural "
                "evaluator and would run this unconditionally. Restructure the proc, or teach "
                "_assert_no_conditional_dml and the executor about the condition."
            )
        if re.match(r"^\s*IF\b", stripped, re.IGNORECASE):
            depth_at_if.append(depth + (1 if opening else 0))
        if opening:
            depth += 1
        elif _CLOSES.match(stripped):
            depth -= 1
            while depth_at_if and depth < depth_at_if[-1]:
                depth_at_if.pop()
        # A single-statement `IF <cond> <stmt>` with no BEGIN closes as soon as that statement is
        # consumed. `IF XACT_STATE() <> 0 ROLLBACK TRAN;` is one fragment, so nothing is pending.
        if depth_at_if and not opening and not re.match(r"^\s*IF\b", stripped, re.IGNORECASE):
            if depth_at_if[-1] == depth:
                depth_at_if.pop()


@dataclass
class ProcResult:
    proc: str
    executed: int = 0
    skipped: int = 0
    skipped_in_catch: int = 0
    rewrites: dict[str, int] = field(default_factory=dict)
    variables: dict[str, object] = field(default_factory=dict)
    seconds: float = 0.0
    error: str | None = None

    def __str__(self) -> str:
        head = (
            f"{self.proc}: {self.executed} executed, {self.skipped} skipped "
            f"({self.skipped_in_catch} in CATCH), {self.seconds:.1f}s"
        )
        counts = {k: v for k, v in self.variables.items() if k.startswith("rows_")}
        return head + (f"  {counts}" if counts else "") + (f"  ERROR {self.error}" if self.error else "")


def run_proc(path: Path, params: dict[str, object]) -> ProcResult:
    """Execute one proc file. `params` seeds the procedure's parameters, by lowercased name."""
    spark = get_spark()
    fragments = [(t, ln) for t, ln in _sql_statements(path.read_text()) if t.strip()]
    proc = path.stem
    _assert_no_conditional_dml(fragments, proc)

    env: dict[str, object] = {k.lower(): v for k, v in params.items()}
    result = ProcResult(proc=proc)
    started = time.perf_counter()
    in_catch = False

    for text, line in fragments:
        stripped = text.strip()
        where = f"{path.name}:{line}"

        # A CATCH body is conditional in exactly the way an IF body is, and its statements are real
        # DML: every proc's handler rolls the transaction back and UPDATEs `stg.load_log` to FAILED.
        # Executing that unconditionally would mark a successful load as failed — and it did, on the
        # first real run, which is how this branch came to exist. So the whole block is skipped, and
        # `_log_failure` writes that FAILED row from Python when a statement actually raises.
        #
        # The flag is set *after* the fragment is otherwise handled, because `BEGIN CATCH` arrives
        # fused to the `END TRY` that precedes it and to the `IF XACT_STATE() <> 0 ROLLBACK TRAN`
        # that follows it, all in one fragment — which is scaffolding either way.
        if in_catch:
            if _END_CATCH.search(stripped):
                in_catch = False
            result.skipped += 1
            result.skipped_in_catch += 1
            continue
        if _BEGIN_CATCH.search(stripped):
            in_catch = True
            result.skipped += 1
            result.skipped_in_catch += 1
            continue

        # DML is tested first. Declarations are harvested from anywhere in a fragment rather than
        # by prefix (see below), so a DML statement mentioning `DECLARE @` in a string would
        # otherwise be classified as a declaration and skipped — a silently missing INSERT.
        if _DML.match(stripped):
            sql = _to_spark(_substitute(stripped, env))
            sql, rewrite = _rewrite_for_delta(sql)
            if rewrite:
                result.rewrites[rewrite] = result.rewrites.get(rewrite, 0) + 1
            try:
                spark.sql(sql)
            except Exception as exc:  # noqa: BLE001 — re-raised with the location
                raise GoldError(f"{where}: {type(exc).__name__}: {str(exc).splitlines()[0]}\n{sql[:600]}") from exc
            result.executed += 1
            continue

        # DECLARE can arrive bundled with the CREATE PROCEDURE header, and several can share a
        # fragment, so declarations are harvested wherever they appear rather than by prefix.
        declared = list(_DECLARE.finditer(stripped))
        if declared:
            for m in declared:
                name = m.group("var").lower()
                init = (m.group("init") or "").strip()
                env[name] = _evaluate(spark, init, env, where) if init else None
            result.skipped += 1
            continue

        m = _SELECT_INTO_VAR.match(stripped)
        if m:
            env[m.group("var").lower()] = _evaluate(spark, m.group("expr"), env, where)
            result.skipped += 1
            continue

        if _SCAFFOLDING.match(stripped):
            result.skipped += 1
            continue

        raise GoldError(f"{where}: unrecognised statement, neither DML nor scaffolding:\n{stripped[:300]}")

    result.seconds = time.perf_counter() - started
    result.variables = {k: v for k, v in env.items() if k.startswith("rows_")}
    log.info("%s", result)
    return result


def _evaluate(spark, expr: str, env: dict[str, object], where: str):
    """Evaluate a scalar T-SQL expression. Literals are read directly; anything else is a query.

    Short-circuiting literals is not just a speed concern, though it does save ~40 Spark jobs per
    load: `DECLARE @rows_read bigint = 0` evaluated as a query would make the harness depend on a
    live session to read a zero, and `ensure_schema` would then have to run before the first
    DECLARE of the first proc for reasons nobody could guess from the code.
    """
    expr = expr.strip().rstrip(";").strip()
    if re.fullmatch(r"-?\d+", expr):
        return int(expr)
    if re.fullmatch(r"'(?:[^']|'')*'", expr):
        return expr[1:-1].replace("''", "'")
    # `SELECT` is prepended *before* transpiling, not after. Half of these expressions carry their
    # own `FROM` — `SELECT @rows_read = COUNT_BIG(*) FROM stg.dim_account` reaches here as
    # `COUNT_BIG(*) FROM stg.dim_account`, which is not a parseable expression on its own.
    sql = _to_spark(f"SELECT {_substitute(expr, env)}")
    try:
        return spark.sql(sql).collect()[0][0]
    except Exception as exc:  # noqa: BLE001 — re-raised with the location
        raise GoldError(f"{where}: evaluating `{expr[:200]}` failed ({type(exc).__name__}: {exc})") from exc


# --------------------------------------------------------------------------------------
# Ordering, retry and the entry point
# --------------------------------------------------------------------------------------

def proc_files() -> list[Path]:
    """The procs in execution order, which is filename order, which is a hard dependency.

    `07_sp_load_fact_transaction` must run before `08_sp_load_fact_dispute` because the dispute
    fact borrows its dimensional identity from the transaction fact rather than resolving it
    against the dimensions — see that proc's header for why that is correctness and not a
    shortcut — and `09_sp_load_agg_merchant_daily` rebuilds whole days from both facts, so it
    must run last.

    The numeric prefixes are asserted contiguous from 00. A proc added without a number, or with a
    duplicate one, would otherwise sort into a plausible-looking position and be wrong only
    sometimes.
    """
    files = sorted(PROC_DIR.glob("*.sql"))
    if not files:
        raise GoldError(f"no procs found under {PROC_DIR}")
    prefixes = []
    for f in files:
        m = re.match(r"^(\d\d)_", f.name)
        if not m:
            raise GoldError(f"{f.name}: procs must be prefixed NN_ to fix their execution order")
        prefixes.append(int(m.group(1)))
    if prefixes != list(range(len(files))):
        raise GoldError(f"proc prefixes are not contiguous from 00: {prefixes}")
    return files


def dim_date_range() -> tuple[date, date]:
    """Calendar bounds wide enough to cover every `date_sk` the facts will compute.

    Derived from the data rather than configured, and padded to whole years, because a fact whose
    date falls outside `dim_date` lands on the unknown member (-1) — visibly, by design, but as a
    data-quality symptom rather than the configuration error it actually is.

    Also re-implements `01_sp_load_dim_date`'s own span guard. That guard is an `IF ... THROW` the
    harness skips, so the invariant is enforced here instead of quietly lost.
    """
    from pyspark.sql import functions as F

    tx = read_table(Layer.SILVER, "fact_transaction").select(
        F.min(F.to_date("auth_ts")).alias("lo"), F.max(F.to_date("auth_ts")).alias("hi")
    ).first()
    dp = read_table(Layer.SILVER, "fact_dispute").select(
        F.min("raised_date").alias("lo"),
        F.max(F.greatest(F.col("raised_date"), F.coalesce("resolved_date", "raised_date"))).alias("hi"),
    ).first()
    lo = min(d for d in (tx["lo"], dp["lo"]) if d is not None)
    hi = max(d for d in (tx["hi"], dp["hi"]) if d is not None)
    lo, hi = date(lo.year, 1, 1), date(hi.year, 12, 31)
    if (hi - lo).days > 9999:
        raise GoldError(
            f"dim_date would span {(hi - lo).days} days; sp_load_dim_date's tally supports 0..9999"
        )
    return lo, hi


_CONFLICT = re.compile(r"Concurrent(Append|Delete|Transaction|WriteException)|DELTA_CONCURRENT", re.I)


def _with_retry(fn, *, attempts: int = 3, base_delay: float = 0.5):
    """Retry a proc on a concurrent-write conflict.

    Retry lives in the caller and not in a proc because a procedure cannot retry its own
    transaction: by the time it can observe the conflict its transaction is already doomed, and on
    Fabric Warehouse `XACT_STATE()` returns -1 and the only legal next move is `ROLLBACK`. The
    caller is the only place that can start a new one.

    On Fabric the conflicts to catch are errors 24556 and 24706 — write-write on a table-level lock,
    which `MERGE` can hit even when every writer is appending. Locally the equivalent is Delta's
    concurrent-modification family, which can only fire if two loads run against the same
    `_onelake/` at once. So this is thin on a laptop and load-bearing on a capacity; it is here,
    with both error families named, because the seam is the same one either way.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — re-raised unless it is a conflict
            if attempt == attempts or not _CONFLICT.search(f"{type(exc).__name__}: {exc}"):
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log.warning("write conflict on attempt %s/%s; retrying in %.1fs", attempt, attempts, delay)
            time.sleep(delay)


def load(
    *,
    load_batch_id: str | None = None,
    loaded_ts: datetime | None = None,
    batch_ids: dict[str, str] | None = None,
    stage: bool = True,
) -> list[ProcResult]:
    """Run the whole gold load: schema, bridge, then the ten procs in order.

    `load_batch_id` defaults to a timestamp-derived id. Unlike bronze's batch id this one is *not*
    required to be deterministic, and the difference is worth stating: bronze's id decides what a
    retry replaces, so it must be a function of the window. Gold's id only labels a load, because
    what a gold retry replaces is decided by the business key in each proc's own `DELETE` or
    `MERGE`. Passing the same id twice is therefore safe and is what the idempotency test does.
    """
    loaded_ts = loaded_ts or datetime.utcnow().replace(microsecond=0)
    load_batch_id = load_batch_id or f"gold|{loaded_ts.isoformat(timespec='seconds')}"

    ensure_schema()
    if stage:
        fill_staging(batch_ids)

    date_from, date_to = dim_date_range()
    log.info("gold load %s  dim_date %s..%s", load_batch_id, date_from, date_to)

    results: list[ProcResult] = []
    for path in proc_files():
        params: dict[str, object] = {"load_batch_id": load_batch_id, "loaded_ts": loaded_ts}
        if "dim_date" in path.name:
            params |= {"from_date": date_from, "to_date": date_to}
        try:
            results.append(_with_retry(lambda p=path, q=params: run_proc(p, q)))
        except Exception as exc:  # noqa: BLE001 — logged to stg.load_log, then re-raised
            _log_failure(path.stem, load_batch_id, exc)
            raise
    return results


def _log_failure(proc: str, load_batch_id: str, exc: BaseException) -> None:
    """Write the `FAILED` row the skipped CATCH block would have written.

    Without this the log would say `RUNNING` forever for the proc that died, which is the one state
    a run log must never be left in: it is indistinguishable from a load still in flight.
    """
    message = f"{type(exc).__name__}: {exc}".replace("'", "''")[:4000]
    try:
        get_spark().sql(
            "UPDATE stg.load_log SET finished_ts = current_timestamp(), status = 'FAILED', "
            f"message = '{message}' "
            f"WHERE load_batch_id = '{load_batch_id}' AND proc_name = '{proc}'"
        )
    except Exception:  # noqa: BLE001 — telemetry must not replace the real error
        log.exception("could not record the failure of %s in stg.load_log", proc)


def load_log(load_batch_id: str | None = None):
    """`stg.load_log` as a DataFrame, newest first. The procs' own account of what they did."""
    df = get_spark().table("stg.load_log")
    if load_batch_id:
        df = df.filter(df.load_batch_id == load_batch_id)
    return df.orderBy("started_ts", "proc_name")
