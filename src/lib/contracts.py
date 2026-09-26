"""The silver type contract, and the gate that enforces it.

Why this is code and not config
-------------------------------
Everything else about a feed lives in `meta_source_config`, so a type map in a Python module looks
like an inconsistency. It is a deliberate one. Changing `load_type` from `full_snapshot` to
`incremental_watermark` is a configuration change: the next run behaves differently and nothing that
already exists is invalidated. Changing `amount_minor` from `long` to `decimal(18,2)` is a **breaking
migration** of a table other layers already read — it needs a code review, a rewrite of the existing
Delta table, and a matching change in the warehouse DDL. Putting it one UPDATE statement away from a
production table would misrepresent what it costs. `docs/data-contracts.md` is the prose; this is the
machine-readable form of the same document, and `tests/` holds them to each other.

Why casting needs a gate at all
-------------------------------
`cast` in Spark is silent and lossy: `cast('2026-13-45' as date)` is `NULL`, not an error. So the
naive silver notebook — read bronze, cast to contract types, write — converts every malformed source
value into a well-typed null and reports a clean run. That is precisely the failure bronze refuses to
commit (it keeps CSV as strings for this reason, see nb_01's header), and silver would then commit it
one layer later.

So the cast is preceded by a gate that asks, per column, "is there a non-null value here that will
not survive its contracted type?" — and rows where the answer is yes are quarantined with the column
that failed, before the cast happens. After the gate, the cast is lossless by construction, which is
what lets every downstream rule (`range`, `enum_domain`) mean what it says: those rules are evaluated
on typed values, and a `range` check against a string is a lexical comparison wearing a numeric
disguise.

Type violations are quarantined at `warn` with a rate threshold, not failed outright, for the same
reason a negative amount is: one unparseable timestamp is a bad row, while a fifth of a column
failing to cast means the source changed its date format overnight — a feed incident. Severity by
*rate* is what distinguishes those two, and it is the same escalation the configured rules use.
"""
from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.lib import dq

log = logging.getLogger(__name__)

# Rate above which "some rows will not cast" becomes "this column's type contract is wrong".
CAST_FAIL_THRESHOLD_PCT = 1.0

# Spark accepts several spellings of the same type in a cast but reports exactly one of them back in
# `simpleString()`: `long` casts fine and reads back as `bigint`. The gate below decides which columns
# to check by comparing the contracted type against the frame's current one, so without this map a
# column that is *already* `bigint` compares unequal to its own contract, gets a `castable` rule it
# cannot fail, and leaves a permanently-passing row in `meta_dq_results` for every integer column in
# the lake. A table is used rather than Spark's own parser because `_parse_datatype_string` is private
# and needs a live session, and a type contract should be readable without one.
_CANONICAL = {"long": "bigint", "int": "integer", "short": "smallint", "byte": "tinyint"}


def canonical(spark_type: str) -> str:
    """The spelling `simpleString()` would use for ``spark_type``."""
    compact = spark_type.replace(" ", "")
    return _CANONICAL.get(compact, compact)

# entity -> column -> Spark type. Columns absent from a feed's map are carried through untouched:
# bronze audit columns keep the types bronze gave them, and a column that appears mid-life (the
# month-10 `wallet_type`) is typed here so it is contracted from the day it shows up rather than
# inheriting whatever the first file inferred.
SILVER_TYPES: dict[str, dict[str, str]] = {
    "transactions": {
        "transaction_id": "string",
        "account_id": "string",
        "card_id": "string",
        "merchant_id": "string",
        "card_product_code": "string",
        # Minor units, integer. A float here would make totals depend on summation order, which is
        # how two reports of the same day disagree by a penny and nobody can say which is right.
        "amount_minor": "long",
        "currency_code": "string",
        "auth_ts": "timestamp",
        "capture_ts": "timestamp",
        "settlement_date": "date",
        "status": "string",
        "decline_reason_code": "string",
        "mcc": "string",
        "channel": "string",
        "country_code": "string",
        "is_3ds": "boolean",
        "device_id": "string",
        "wallet_type": "string",
    },
    "accounts": {
        "account_id": "string",
        "customer_id": "string",
        # Leading zeros are significant in a sort code, so it stays a string forever. Typing it as
        # an integer would turn 012345 into 12345 and the join to the payment scheme would stop
        # matching — the canonical example of a number that is not a quantity.
        "sort_code": "string",
        "account_number_masked": "string",
        "account_type": "string",
        "status": "string",
        "risk_band": "string",
        "credit_limit_minor": "long",
        "opened_date": "date",
        "closed_date": "date",
        "region": "string",
        "_op": "string",
        "_change_ts": "timestamp",
    },
    "customers": {
        "customer_id": "string",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "dob": "date",
        "kyc_status": "string",
        "country_code": "string",
        "segment": "string",
        "marketing_opt_in": "boolean",
        "_snapshot_date": "date",
    },
    "merchants": {
        "merchant_id": "string",
        "merchant_name": "string",
        "mcc": "string",
        "category": "string",
        "country_code": "string",
        "acquirer_id": "string",
        "risk_score": "int",
        "status": "string",
        "onboarded_date": "date",
        "_snapshot_date": "date",
    },
    "disputes": {
        "dispute_id": "string",
        "transaction_id": "string",
        "raised_date": "date",
        "reason_code": "string",
        "disputed_amount_minor": "long",
        "currency_code": "string",
        "status": "string",
        "resolved_date": "date",
        "_event_ts": "timestamp",
    },
    "fx_rates": {
        "rate_date": "date",
        "from_currency": "string",
        "to_currency": "string",
        # Decimal, not double: an FX rate multiplies every monetary figure in the report, so a
        # binary-floating-point representation error is not confined to the rate column.
        "rate": "decimal(18,8)",
        "source": "string",
    },
    "card_products": {
        "card_product_code": "string",
        "product_name": "string",
        "network": "string",
        "tier": "string",
        "annual_fee_minor": "long",
        "active_from": "date",
        "active_to": "date",
    },
}


def types_for(entity: str) -> dict[str, str]:
    if entity not in SILVER_TYPES:
        raise ValueError(
            f"no silver type contract for entity {entity!r}. Contracted: "
            f"{sorted(SILVER_TYPES)}. A feed without a type contract would be written to silver "
            "with whatever types the source files happened to infer."
        )
    return SILVER_TYPES[entity]


def _uncastable(column: str, to_type: str) -> Column:
    """True where a populated value will not survive the cast.

    Null in, null out is not a violation — that is `not_null`'s subject, and reporting it here too
    would make one bad row show up as two different defects in `meta_dq_results`.
    """
    return F.col(column).isNotNull() & F.col(column).cast(to_type).isNull()


def _cast_rule(entity: str, column: str, to_type: str) -> dq.Rule:
    """A synthetic DQ rule for one column's type contract.

    Synthetic, not seeded: these rules are *derived* from `SILVER_TYPES`, so seeding them would mean
    maintaining fifty rows that must agree with a dict in this file, and the day they disagree the
    table wins and the code is silently ignored. The rule ids follow the same
    `<rule_set>.<column>.<rule_type>` shape as the configured ones so `meta_dq_results` reads
    uniformly and a query does not need to know which rules came from where.
    """
    return dq.Rule(
        rule_id=f"{entity}.{column}.castable",
        rule_set=entity,
        column_name=column,
        rule_type="castable",
        params={"to": to_type},
        condition=None,
        severity="warn",
        fail_threshold_pct=CAST_FAIL_THRESHOLD_PCT,
        description=f"Value must be representable as {to_type}; see src/lib/contracts.py.",
    )


def enforce(df: DataFrame, entity: str, run_id: str, batch_id: str,
            write_results: bool = True) -> tuple[DataFrame, dq.DQOutcome]:
    """Quarantine rows that violate the type contract, then cast the survivors.

    Returns the cast frame and the gate's outcome. The caller decides what a breach means, exactly as
    with `dq.apply` — the gate's job is to record and separate, never to choose whether the run dies.
    """
    contract = types_for(entity)
    # Only columns that are actually present, and only those whose type would really change. A cast
    # to the type a column already has cannot fail, so evaluating it would add a rule that always
    # passes — noise in `meta_dq_results`, and a slower gate.
    present = {c: t for c, t in contract.items() if c in df.columns}
    checkable = {
        c: t for c, t in present.items()
        if df.schema[c].dataType.simpleString() != canonical(t)
    }

    rules = [_cast_rule(entity, c, t) for c, t in sorted(checkable.items())]
    if not rules:
        return _cast(df, present), dq.DQOutcome(entity, df, df.count(), 0, [])

    flags = {c: f"__cast_{i}" for i, c in enumerate(sorted(checkable))}
    flagged = df
    for c, t in sorted(checkable.items()):
        flagged = flagged.withColumn(flags[c], _uncastable(c, t))
    flagged = flagged.cache()

    rows_in = flagged.count()
    agg = flagged.agg(*[
        F.sum(F.col(flag).cast("long")).alias(flag) for flag in flags.values()
    ]).first()

    results = []
    for rule in rules:
        failed = int(agg[flags[rule.column_name]] or 0)
        pct = 100.0 * failed / rows_in if rows_in else 0.0
        result = dq.RuleResult(
            rule, rows_in, failed, outcome="passed", breached=failed > 0,
            detail=f"{failed}/{rows_in} row(s) ({pct:.3f}%) not representable as "
                   f"{rule.params['to']}",
        )
        # Computed after construction because `fails_run` reads `breached` and the severity rules
        # that go with it, and restating that logic here is how the gate's verdict and the rule
        # engine's verdict end up disagreeing about the same row.
        result.outcome = "failed_run" if result.fails_run else ("quarantined" if failed else "passed")
        results.append(result)

    any_bad = F.lit(False)
    for flag in flags.values():
        any_bad = any_bad | F.col(flag)
    rule_ids = F.array_compact(F.array(*[
        F.when(F.col(flags[r.column_name]), F.lit(r.rule_id)) for r in rules
    ]))

    business = list(df.columns)
    bad = (
        flagged.filter(any_bad)
        .withColumn(dq.RULE_IDS_COL, rule_ids)
        .withColumn(dq.SEVERITY_COL, F.lit("warn"))
        .select(*business, dq.RULE_IDS_COL, dq.SEVERITY_COL)
    )
    rows_quarantined = bad.count()

    if write_results:
        if rows_quarantined:
            # Quarantined *uncast*, which is the whole point: the value that broke the contract has
            # to be readable in the quarantine table. Writing the cast — and therefore null — value
            # would file the row with the evidence removed.
            dq.quarantine(bad, entity, run_id, batch_id)
        dq.record(results, entity, run_id, batch_id)

    clean = _cast(flagged.filter(~any_bad).select(*business), present)
    outcome = dq.DQOutcome(entity, clean, rows_in, rows_quarantined, results)
    log.info("cast contract %s: %d row(s) in, %d quarantined, %d column(s) checked",
             entity, rows_in, rows_quarantined, len(rules))
    flagged.unpersist()
    return clean, outcome


def _cast(df: DataFrame, contract: dict[str, str]) -> DataFrame:
    return df.select(*[
        F.col(c).cast(contract[c]).alias(c) if c in contract else F.col(c)
        for c in df.columns
    ])
