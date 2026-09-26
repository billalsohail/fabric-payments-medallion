"""Config-driven data-quality gate.

Rules live in ``meta_dq_rules``, never in code. Adding a rule is a row; changing a threshold is an
``UPDATE``. That is the same argument as metadata-driven ingestion, applied to correctness instead of
movement, and it is what makes the gate reviewable by someone who does not read Python.

Two rule categories, and the distinction is not cosmetic
-------------------------------------------------------
**Row rules** — ``not_null``, ``unique``, ``range``, ``enum_domain``, ``expression``,
``referential`` — give a verdict per row, so a failing row can be quarantined.

**Batch rules** — ``freshness``, ``continuity`` — give one verdict per load. They quarantine
nothing, because what they detect is rows that are *absent*: a feed that stopped producing, or a
missing rate date. You cannot quarantine a row that does not exist. Conflating the two categories is
how a "completeness check" ends up quarantining a perfectly good batch, so they are separated here
and recorded differently in ``meta_dq_results``.

One pass, not one pass per rule
-------------------------------
Every row rule is compiled to a boolean column and all of them are evaluated in a single scan. The
alternative — filter once per rule — costs N scans and, worse, makes a row that breaks three rules
arrive in quarantine three times. Here a quarantined row carries ``_dq_rule_ids``, the *complete*
list of what it violated, so someone fixing it upstream fixes everything at once instead of the
same row returning tomorrow for the next reason.

Severity, threshold, and what actually fails a run
-------------------------------------------------
Failing rows are quarantined regardless of severity; severity only decides whether the *run* fails.

- ``error`` — fails the run. Reserved for a broken feed rather than a bad row: a missing business
  key, an unknown enum value (a new status code is a schema change, not a data error).
- ``warn`` — quarantine and continue. One negative amount must not halt a payments feed.
- ``fail_threshold_pct`` — escalates a ``warn`` to a failure by *rate*. Two hundred bad rows is a
  data problem; twenty percent bad rows is a broken upstream system, and treating those identically
  is how a pipeline ships a quietly empty day.

Results and quarantine rows are written **before** the failure is raised. A run that fails without
explaining itself in ``meta_dq_results`` has made the outage harder to diagnose, not easier.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.runtime.context import (
    Layer,
    get_spark,
    read_table,
    table_exists,
    write_table,
)

log = logging.getLogger(__name__)

RULES_TABLE = "meta_dq_rules"
RESULTS_TABLE = "meta_dq_results"
QUARANTINE_PREFIX = "q_"

# Columns added to a quarantined row. Prefixed like bronze's audit columns so the same convention
# ("underscore means platform, not business") holds across every layer.
RULE_IDS_COL = "_dq_rule_ids"
SEVERITY_COL = "_dq_severity"

ROW_RULE_TYPES = frozenset(
    {"not_null", "unique", "range", "enum_domain", "expression", "referential"}
)
BATCH_RULE_TYPES = frozenset({"freshness", "continuity"})

# Rule types that are *derived from code* rather than read from `meta_dq_rules`. Currently one:
# `castable`, generated from the silver type contract in `src/lib/contracts.py`. They are row rules
# in every respect that matters here — a row either survives its contracted type or it does not, so
# it can be quarantined and the `warn`-plus-threshold escalation applies — but they are not
# configurable, and `load_rules` refuses them so the two mechanisms cannot quietly overlap.
DERIVED_RULE_TYPES = frozenset({"castable"})


class DQFailure(RuntimeError):
    """Raised when a rule of severity ``error`` fires, or a ``warn`` rule breaches its threshold."""


@dataclass(frozen=True)
class Rule:
    rule_id: str
    rule_set: str
    column_name: str | None
    rule_type: str
    params: dict
    condition: str | None
    severity: str
    fail_threshold_pct: float | None
    description: str

    @property
    def is_row_rule(self) -> bool:
        return self.rule_type in ROW_RULE_TYPES or self.rule_type in DERIVED_RULE_TYPES


@dataclass
class RuleResult:
    rule: Rule
    rows_evaluated: int
    rows_failed: int
    outcome: str  # passed | quarantined | failed_run | batch_warn | not_evaluated
    breached: bool = False
    detail: str = ""

    """``breached`` is the verdict; ``rows_failed`` is only ever a count of rows.

    They are separate fields because for a batch rule they are not the same thing. A `continuity`
    rule that finds five missing days has breached, but *no row* failed — the defect is rows that do
    not exist. Deriving the verdict from ``rows_failed`` forced the earlier version of this class to
    report "920 of 920 rows failed (100%)" for exactly that case, which is a lie of the specific kind
    the row/batch split exists to prevent: it invites someone to go looking for 920 bad rows. A batch
    rule now reports ``rows_failed = 0`` and puts its measure in ``detail``.
    """

    @property
    def failed_pct(self) -> float:
        """Failure *rate*. Meaningful for row rules only; a batch rule has no denominator."""
        if not self.rule.is_row_rule or not self.rows_evaluated:
            return 0.0
        return 100.0 * self.rows_failed / self.rows_evaluated

    @property
    def fails_run(self) -> bool:
        if not self.breached:
            return False
        if self.rule.severity == "error":
            return True
        # `warn` escalates by *rate*, which a batch rule does not have — one verdict per load cannot
        # be a percentage of anything. So a batch rule at `warn` severity records and never fails the
        # run, and the only way to make one fatal is to set its severity to `error`. Stated here
        # rather than left implicit because "warn + threshold" reads like it should apply to every
        # rule, and silently ignoring a configured threshold would be worse than refusing it.
        if not self.rule.is_row_rule:
            return False
        threshold = self.rule.fail_threshold_pct
        return threshold is not None and self.failed_pct > threshold


@dataclass
class DQOutcome:
    """What the gate decided, and the rows that survived it."""

    entity: str
    clean: DataFrame
    rows_in: int
    rows_quarantined: int
    results: list[RuleResult] = field(default_factory=list)

    @property
    def breaches(self) -> list[RuleResult]:
        """Rules that fail the run."""
        return [r for r in self.results if r.fails_run]

    @property
    def warnings(self) -> list[RuleResult]:
        """Rules that found a defect but do not fail the run. Reported separately so a run summary
        cannot say "0 breaches" about a load that quarantined 605 rows."""
        return [r for r in self.results if r.breached and not r.fails_run]

    @property
    def failed(self) -> bool:
        return bool(self.breaches)

    def raise_if_failed(self) -> None:
        if not self.failed:
            return
        detail = "; ".join(
            f"{r.rule.rule_id} [{r.rule.severity}] {r.detail}"
            + (f" > {r.rule.fail_threshold_pct}% threshold" if r.rule.severity == "warn" else "")
            for r in self.breaches
        )
        raise DQFailure(f"{self.entity}: data quality gate failed — {detail}")


# --------------------------------------------------------------------------------------
# Rule loading
# --------------------------------------------------------------------------------------

def load_rules(rule_set: str) -> list[Rule]:
    """Read the enabled rules for a rule set.

    An empty rule set is a hard error rather than a quiet pass. A feed whose rules were deleted
    would otherwise sail through the gate looking pristine, which is the single most dangerous
    behaviour a DQ framework can have.
    """
    rows = (
        read_table(Layer.META, RULES_TABLE)
        .filter((F.col("rule_set") == rule_set) & F.col("enabled"))
        .orderBy("rule_id")
        .collect()
    )
    if not rows:
        known = sorted(
            r["rule_set"]
            for r in read_table(Layer.META, RULES_TABLE).select("rule_set").distinct().collect()
        )
        raise ValueError(
            f"no enabled DQ rules for rule_set {rule_set!r}. A feed with no rules passes the gate "
            f"vacuously, so this is treated as a configuration error. Known rule sets: {known}"
        )
    out = []
    for r in rows:
        if r["rule_type"] in DERIVED_RULE_TYPES:
            raise ValueError(
                f"rule {r['rule_id']} has rule_type {r['rule_type']!r}, which is derived from code "
                "rather than configured. Type contracts live in SILVER_TYPES in src/lib/contracts.py "
                "because changing a column's type is a breaking migration of a table other layers "
                "read, not a config flip. Delete the row."
            )
        threshold = r["fail_threshold_pct"]
        out.append(
            Rule(
                rule_id=r["rule_id"],
                rule_set=r["rule_set"],
                column_name=r["column_name"],
                rule_type=r["rule_type"],
                # Stored as a JSON string for the same reason watermarks are stored as strings:
                # the control plane has to be readable from a Data Factory pipeline expression,
                # where every value is a string.
                params=json.loads(r["rule_params"]) if r["rule_params"] else {},
                condition=r["condition"],
                severity=r["severity"],
                fail_threshold_pct=float(threshold) if threshold is not None else None,
                description=r["description"] or "",
            )
        )
    return out


# --------------------------------------------------------------------------------------
# Row rules: compiled to boolean columns
# --------------------------------------------------------------------------------------

def _require_column(rule: Rule, df: DataFrame) -> str:
    """Resolve a rule's column, failing loudly when it is absent.

    This is deliberately fatal. If a source rename made ``merchant_id`` into ``merchantId``, a
    tolerant gate would skip the rule and report a clean run — the rule would be *silently off*
    precisely when the schema moved under it. A pipeline that cannot evaluate its own rules must
    stop, not shrug.
    """
    col = rule.column_name
    if col and col in df.columns:
        return col
    raise ValueError(
        f"rule {rule.rule_id} targets column {col!r}, which is not in the DataFrame. "
        f"Available: {sorted(df.columns)}"
    )


def _violation(rule: Rule, df: DataFrame) -> Column:
    """Boolean column: true where this row *violates* the rule.

    Null handling follows one consistent principle: a null is ``not_null``'s business and nobody
    else's. A ``range`` or ``enum_domain`` rule passes a null row rather than double-reporting what
    another rule already owns, which keeps ``_dq_rule_ids`` a list of distinct problems instead of
    the same problem restated.
    """
    kind = rule.rule_type
    if kind == "expression":
        predicate = rule.params["predicate"]
        # An invariant that evaluates to NULL is treated as violated. The opposite choice exempts
        # exactly the rows with missing data — the rows most likely to be wrong.
        return ~F.coalesce(F.expr(predicate), F.lit(False))

    col_name = _require_column(rule, df)
    col = F.col(col_name)

    if kind == "not_null":
        return col.isNull()
    if kind == "range":
        bounds = []
        if "min" in rule.params:
            bounds.append(col < F.lit(rule.params["min"]))
        if "max" in rule.params:
            bounds.append(col > F.lit(rule.params["max"]))
        breach = bounds[0]
        for b in bounds[1:]:
            breach = breach | b
        return col.isNotNull() & breach
    if kind == "enum_domain":
        return col.isNotNull() & ~col.isin(*rule.params["values"])
    if kind == "unique":
        # Duplicate detection as a window rather than a self-join: one shuffle, and every copy of a
        # duplicated key is flagged. SQL uniqueness semantics ignore nulls, so nulls are excluded —
        # a column that should not be null has a not_null rule saying so.
        dupes = F.count(F.lit(1)).over(Window.partitionBy(col_name))
        return col.isNotNull() & (dupes > 1)
    raise ValueError(f"{rule.rule_id}: {kind!r} is not a row rule")


def _referential_flag(rule: Rule, df: DataFrame) -> tuple[DataFrame, Column] | None:
    """Attach a "key exists in the reference table" flag, or ``None`` if it cannot be checked.

    Returns ``None`` when the reference table does not exist yet, which is a real state on a partial
    run: ``--entities disputes`` against an empty lake has no ``fact_transaction`` to check against.
    An ``error``-severity rule in that state is fatal, because the run must not report that an
    integrity constraint held when it was never evaluated. A ``warn`` rule is skipped and recorded
    as ``not_evaluated`` — visible in ``meta_dq_results``, not hidden behind a green run.

    Why any of this is necessary: on Fabric, Warehouse ``FOREIGN KEY`` constraints are metadata-only
    (``NOT ENFORCED``). This rule *is* the referential guarantee — there is no engine behind it.
    """
    col_name = _require_column(rule, df)
    layer = Layer(rule.params["ref_layer"])
    ref_table, ref_col = rule.params["ref_table"], rule.params["ref_column"]
    if not table_exists(layer, ref_table):
        if rule.severity == "error":
            raise DQFailure(
                f"{rule.rule_id}: reference table {layer.value}.{ref_table} does not exist, so an "
                "error-severity referential rule cannot be evaluated. Load the referenced entity "
                "first (see priority in meta_source_config), or run the full pipeline."
            )
        log.warning("%s: reference %s.%s missing — rule not evaluated",
                    rule.rule_id, layer.value, ref_table)
        return None

    flag = f"__dq_ref_{rule.rule_id.replace('.', '_')}"
    ref = (
        read_table(layer, ref_table)
        .select(F.col(ref_col).alias(f"{flag}_key"))
        .distinct()          # without this a duplicated reference key would multiply the left rows
        .withColumn(flag, F.lit(True))
    )
    joined = df.join(
        F.broadcast(ref), df[col_name] == F.col(f"{flag}_key"), how="left"
    ).drop(f"{flag}_key")
    return joined, F.col(flag).isNull()


# --------------------------------------------------------------------------------------
# Batch rules
# --------------------------------------------------------------------------------------

def _evaluate_batch_rule(rule: Rule, df: DataFrame, rows_in: int, as_of: date | None) -> RuleResult:
    """Evaluate a rule whose subject is the load, not the row.

    ``rows_failed`` is always 0 and the verdict lives in ``breached``: the batch either satisfies the
    assertion or does not, and either way no individual row is at fault. Nothing is quarantined —
    see the module docstring.
    """
    col_name = _require_column(rule, df)
    agg = df.select(
        F.min(F.col(col_name).cast("date")).alias("lo"),
        F.max(F.col(col_name).cast("date")).alias("hi"),
        F.countDistinct(F.col(col_name).cast("date")).alias("distinct_days"),
    ).first()
    lo, hi, distinct_days = agg["lo"], agg["hi"], agg["distinct_days"]

    if rule.rule_type == "freshness":
        # Measured against the batch's own arrival date, never wall-clock. A rule that compares to
        # `current_date` makes its own verdict depend on when the test happens to run, which is how
        # a suite starts failing on a Monday for reasons nobody can reproduce. It still catches the
        # thing that matters — a feed that has stopped producing — because `as_of` moves with the
        # data and the newest value in it does not.
        reference = as_of or hi
        if hi is None:
            detail, failed = "no dated rows", True
        else:
            age = (reference - hi).days
            failed = age > int(rule.params["max_age_days"])
            detail = f"newest {col_name}={hi} is {age}d behind as_of={reference}"
    elif rule.rule_type == "continuity":
        # The gap rule. `distinct_days` against the span tells us whether any day in [lo, hi] is
        # missing entirely — the injected FX rate gaps. Deliberately *not* a row rule: the defect is
        # an absent row, and there is nothing to quarantine.
        span = 0 if lo is None else (hi - lo).days + 1
        missing = max(0, span - distinct_days)
        failed = missing > int(rule.params.get("max_missing_days", 0))
        detail = f"{missing} missing day(s) across {span}d span [{lo}..{hi}]"
    else:
        raise ValueError(f"{rule.rule_id}: {rule.rule_type!r} is not a batch rule")

    result = RuleResult(rule, rows_in, 0, "passed", breached=failed, detail=detail)
    result.outcome = "failed_run" if result.fails_run else ("batch_warn" if failed else "passed")
    log.log(logging.WARNING if failed else logging.DEBUG,
            "%s %s: %s", rule.rule_id, "FAILED" if failed else "ok", detail)
    return result


# --------------------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------------------

def _as_of_from_batch_id(batch_id: str) -> date | None:
    """Recover the high end of the load window from ``entity|lo|hi``."""
    parts = batch_id.split("|")
    try:
        return date.fromisoformat(parts[-1])
    except (ValueError, IndexError):
        return None


def apply(
    df: DataFrame,
    entity: str,
    rule_set: str,
    run_id: str,
    batch_id: str,
    as_of: date | None = None,
    write_results: bool = True,
) -> DQOutcome:
    """Evaluate every rule in ``rule_set`` against ``df``.

    Returns the rows that violated nothing. Quarantined rows and rule outcomes are persisted before
    control returns, so a caller that goes on to raise still leaves a diagnosable trail.
    """
    rules = load_rules(rule_set)
    as_of = as_of or _as_of_from_batch_id(batch_id)

    # Cached because every count below scans it, and for a bronze-sized batch recomputing the
    # upstream plan per aggregation is the difference between seconds and minutes.
    df = df.cache()
    rows_in = df.count()

    row_rules: list[tuple[Rule, str]] = []
    results: list[RuleResult] = []
    flagged = df

    for rule in rules:
        if not rule.is_row_rule:
            continue
        if rule.rule_type == "referential":
            attached = _referential_flag(rule, flagged)
            if attached is None:
                ref = f"{rule.params['ref_layer']}.{rule.params['ref_table']}"
                results.append(RuleResult(
                    rule, 0, 0, "not_evaluated",
                    detail=f"reference table {ref} absent — rule not evaluated",
                ))
                continue
            flagged, violation = attached
        else:
            violation = _violation(rule, flagged)
        if rule.condition:
            # `condition` narrows the rule's scope, so a row outside it is not evaluated at all —
            # which is why rows_evaluated is per-rule. An ATM withdrawal is not a merchant_id
            # failure; it is a row the merchant rule has no opinion about.
            in_scope = F.coalesce(F.expr(rule.condition), F.lit(False))
            violation = in_scope & violation
        else:
            in_scope = F.lit(True)

        flag = f"__dq_{len(row_rules)}"
        scope = f"__scope_{len(row_rules)}"
        flagged = flagged.withColumn(flag, violation).withColumn(scope, in_scope)
        row_rules.append((rule, flag))

    if row_rules:
        flagged = flagged.cache()
        # One aggregation for every rule at once: 2N counts in a single pass rather than N scans.
        agg = flagged.agg(*[
            F.sum(F.col(f"__dq_{i}").cast("long")).alias(f"failed_{i}")
            for i in range(len(row_rules))
        ], *[
            F.sum(F.col(f"__scope_{i}").cast("long")).alias(f"scope_{i}")
            for i in range(len(row_rules))
        ]).first()
        for i, (rule, _) in enumerate(row_rules):
            failed = int(agg[f"failed_{i}"] or 0)
            evaluated = int(agg[f"scope_{i}"] or 0)
            result = RuleResult(
        rule, evaluated, failed, "passed", breached=failed > 0,
        detail=f"{failed}/{evaluated} row(s) ({100.0 * failed / evaluated if evaluated else 0.0:.3f}%)",
    )
            result.outcome = (
                "failed_run" if result.fails_run else ("quarantined" if failed else "passed")
            )
            results.append(result)

    for rule in rules:
        if rule.rule_type in BATCH_RULE_TYPES:
            results.append(_evaluate_batch_rule(rule, df, rows_in, as_of))

    # ---- split ---------------------------------------------------------------------------
    business_cols = list(df.columns)
    if row_rules:
        any_violation = F.lit(False)
        for _, flag in row_rules:
            any_violation = any_violation | F.col(flag)
        rule_ids = F.array_compact(F.array(*[
            F.when(F.col(flag), F.lit(rule.rule_id)) for rule, flag in row_rules
        ]))
        # The row's severity is the worst severity among the rules it actually broke, so triage can
        # sort quarantine by "feed is broken" ahead of "one bad amount" without re-deriving it.
        hit_error = F.lit(False)
        for rule, flag in row_rules:
            if rule.severity == "error":
                hit_error = hit_error | F.col(flag)
        worst = F.when(hit_error, F.lit("error")).otherwise(F.lit("warn"))

        quarantined = (
            flagged.filter(any_violation)
            .withColumn(RULE_IDS_COL, rule_ids)
            .withColumn(SEVERITY_COL, worst)
            .select(*business_cols, RULE_IDS_COL, SEVERITY_COL)
        )
        clean = flagged.filter(~any_violation).select(*business_cols)
        rows_quarantined = quarantined.count()
    else:
        clean, quarantined, rows_quarantined = df, None, 0

    if write_results:
        if rows_quarantined:
            _write_quarantine(quarantined, entity, run_id, batch_id)
        _write_results(results, entity, run_id, batch_id)

    outcome = DQOutcome(entity, clean, rows_in, rows_quarantined, results)
    log.info(
        "dq %s: %d row(s) in, %d quarantined, %d rule(s) evaluated, "
        "%d fatal, %d warned, %d not evaluated",
        entity, rows_in, rows_quarantined, len(results), len(outcome.breaches),
        len(outcome.warnings), sum(1 for r in results if r.outcome == "not_evaluated"),
    )
    for r in outcome.breaches + outcome.warnings:
        log.log(logging.ERROR if r.fails_run else logging.WARNING,
                "  %s [%s] %s: %s", r.rule.rule_id, r.rule.severity, r.outcome, r.detail)
    return outcome


def quarantine(df: DataFrame, entity: str, run_id: str, batch_id: str) -> None:
    """Send rows to the entity's quarantine table.

    Public because the type-contract gate in `src/lib/contracts.py` has to quarantine rows before
    `apply` ever runs — a `range` rule on a string that will not cast to a number is a lexical
    comparison wearing a numeric disguise, so castability is checked first and separately. Rather
    than let that gate write to the quarantine table itself, it comes through here: *where rejects
    go, and what shape they take* stays owned by this module, so there is one quarantine convention
    rather than two that drift.

    ``df`` must already carry ``RULE_IDS_COL`` and ``SEVERITY_COL``.
    """
    missing = {RULE_IDS_COL, SEVERITY_COL} - set(df.columns)
    if missing:
        raise ValueError(
            f"quarantined rows must carry {sorted(missing)} — a rejected row without the rule that "
            "rejected it is unactionable."
        )
    _write_quarantine(df, entity, run_id, batch_id)


def record(results: list[RuleResult], entity: str, run_id: str, batch_id: str) -> None:
    """Append rule outcomes to ``meta_dq_results``.

    Public for the same reason as `quarantine`: the type gate's verdicts belong in the same table as
    every other rule's, so that "show me every rule that fired on this run" is one query and not a
    union over wherever each gate decided to keep its own log.
    """
    _write_results(results, entity, run_id, batch_id)


def _write_quarantine(df: DataFrame, entity: str, run_id: str, batch_id: str) -> None:
    """Append rejected rows, keeping every reason they were rejected.

    ``mergeSchema`` is on for the same reason bronze needs it: the quarantine table inherits the
    source schema, so a feed that gains a column gains it here too.
    """
    out = (
        df.withColumn("_dq_run_id", F.lit(run_id))
        .withColumn("_dq_batch_id", F.lit(batch_id))
        .withColumn("_quarantined_ts", F.current_timestamp())
    )
    write_table(out, Layer.QUARANTINE, f"{QUARANTINE_PREFIX}{entity}",
                mode="append", merge_schema=True)


def _write_results(results: list[RuleResult], entity: str, run_id: str, batch_id: str) -> None:
    if not results:
        return
    now = datetime.now(tz=timezone.utc)
    rows = [
        (
            run_id, batch_id, entity, r.rule.rule_id, r.rule.rule_type, r.rule.column_name,
            r.rule.severity, int(r.rows_evaluated), int(r.rows_failed),
            f"{r.failed_pct:.6f}", r.outcome, r.detail, now,
        )
        for r in results
    ]
    schema = (
        "run_id string, batch_id string, entity string, rule_id string, rule_type string, "
        "column_name string, severity string, rows_evaluated long, rows_failed long, "
        "failed_pct string, outcome string, detail string, evaluated_ts timestamp"
    )
    write_table(get_spark().createDataFrame(rows, schema), Layer.META, RESULTS_TABLE, mode="append")
