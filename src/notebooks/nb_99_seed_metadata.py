# %% [markdown]
# # nb_99 — Seed the metadata control plane
#
# Five Delta tables in `lh_meta` that together make the pipeline **configuration-driven** rather
# than code-driven. The test of that claim is concrete: adding an eighth source feed must be one row
# in `meta_source_config` plus its DQ rules — no new notebook, no new pipeline, no code change.
#
# The tables split into two kinds, and the split matters:
#
# | Table | Kind | Reseed behaviour |
# |---|---|---|
# | `meta_source_config` | **declarative** — this file is source of truth | overwritten every run |
# | `meta_dq_rules` | **declarative** | overwritten every run |
# | `meta_watermark` | **operational state** | created if absent, otherwise preserved |
# | `meta_dq_results` | operational history | created if absent, otherwise preserved |
# | `meta_run_log` | operational history | created if absent, otherwise preserved |
#
# Reseeding must never silently reset a watermark — that would turn a config change into a full
# reload of every feed. `--reset true` does it explicitly, and says so in the log.
#
# On Fabric this is a notebook writing to the `lh_meta` lakehouse; the config rows are the input to
# the `Lookup` activity that drives the `ForEach` in `pl_master`.

# %%
from __future__ import annotations

import json
import logging

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.runtime import params
from src.runtime.context import Layer, get_spark, read_table, table_exists, write_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nb_99")

# %% tags=["parameters"]
PARAMS = params.resolve({"reset": False})

# %% [markdown]
# ## `meta_source_config` — one row per source feed
#
# Column choices worth defending:
#
# - **`load_type`** drives the bronze read: `incremental_watermark` reads only partitions newer than
#   the stored watermark; `full_snapshot` reads the latest `ingest_date` only; `static` reads all.
# - **`reprocess_window_days`** is what makes late-arriving disputes correct. Non-null means silver
#   rebuilds a trailing window instead of only touching new rows — the single most commonly missed
#   requirement in this kind of pipeline.
# - **`merge_keys`** is a comma-separated list rather than an array because Data Factory's `Lookup`
#   activity hands pipeline expressions strings, and a config schema that only works from Spark
#   isn't config. Parsed at the point of use.
# - **`scd_type`** is `0` (no history), `1` (overwrite) or `2` (full history). Three feeds are `2`.
# - **`priority`** is the execution wave, and it encodes a real dependency rather than a preference:
#   dimensions load before facts because `transactions` carries a `referential` rule against
#   `dim_merchant`. Run the fact first and that rule checks against a stale dimension, quarantining
#   valid rows — a failure that looks like bad source data. Reference data (5) → dimensions (10) →
#   facts (20) → late-arriving facts (30).

# %%
SOURCE_CONFIG = [
    dict(
        entity="transactions",
        source_format="json",
        read_options={"multiLine": "false"},
        target_table="fact_transaction",
        load_type="incremental_watermark",
        watermark_column="ingest_date",
        merge_keys="transaction_id",
        scd_type=0,
        dq_rule_set="transactions",
        partition_column="ingest_date",
        reprocess_window_days=None,
        # Bronze must not fail when wallet_type appears at month 10.
        allow_schema_evolution=True,
        priority=20,
        enabled=True,
    ),
    dict(
        entity="accounts",
        source_format="csv",
        read_options={"header": "true"},
        target_table="dim_account",
        load_type="incremental_watermark",
        watermark_column="ingest_date",
        merge_keys="account_id",
        scd_type=2,
        dq_rule_set="accounts",
        partition_column="ingest_date",
        reprocess_window_days=None,
        allow_schema_evolution=False,
        priority=10,
        enabled=True,
    ),
    dict(
        entity="customers",
        source_format="csv",
        read_options={"header": "true"},
        target_table="dim_customer",
        load_type="full_snapshot",
        watermark_column="ingest_date",
        merge_keys="customer_id",
        scd_type=2,
        dq_rule_set="customers",
        partition_column="ingest_date",
        reprocess_window_days=None,
        allow_schema_evolution=False,
        priority=10,
        enabled=True,
    ),
    dict(
        entity="merchants",
        source_format="parquet",
        read_options={},
        target_table="dim_merchant",
        load_type="full_snapshot",
        watermark_column="ingest_date",
        merge_keys="merchant_id",
        scd_type=2,
        dq_rule_set="merchants",
        partition_column="ingest_date",
        reprocess_window_days=None,
        allow_schema_evolution=False,
        priority=10,
        enabled=True,
    ),
    dict(
        entity="disputes",
        source_format="json",
        read_options={"multiLine": "false"},
        target_table="fact_dispute",
        load_type="incremental_watermark",
        watermark_column="ingest_date",
        merge_keys="dispute_id",
        scd_type=0,
        dq_rule_set="disputes",
        partition_column="ingest_date",
        # Disputes are raised 0-90 days after the transaction they dispute. Processing only new
        # partitions would leave chargeback rates quietly wrong for up to three months.
        reprocess_window_days=90,
        allow_schema_evolution=False,
        priority=30,
        enabled=True,
    ),
    dict(
        entity="fx_rates",
        source_format="csv",
        read_options={"header": "true"},
        target_table="dim_fx_rate",
        load_type="incremental_watermark",
        watermark_column="ingest_date",
        merge_keys="rate_date,from_currency",
        scd_type=0,
        dq_rule_set="fx_rates",
        partition_column="ingest_date",
        reprocess_window_days=None,
        allow_schema_evolution=False,
        # FX must be in place before anything computes GBP-normalised volume.
        priority=5,
        enabled=True,
    ),
    dict(
        entity="card_products",
        source_format="csv",
        read_options={"header": "true"},
        target_table="dim_card_product",
        load_type="static",
        watermark_column=None,
        merge_keys="card_product_code",
        scd_type=1,
        dq_rule_set="card_products",
        partition_column="ingest_date",
        reprocess_window_days=None,
        allow_schema_evolution=False,
        priority=5,
        enabled=True,
    ),
]

SOURCE_CONFIG_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("source_format", StringType(), False),
    StructField("read_options", StringType(), False),      # JSON object
    StructField("target_table", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("watermark_column", StringType(), True),
    StructField("merge_keys", StringType(), False),        # comma-separated
    StructField("scd_type", IntegerType(), False),
    StructField("dq_rule_set", StringType(), False),
    StructField("partition_column", StringType(), True),
    StructField("reprocess_window_days", IntegerType(), True),
    StructField("allow_schema_evolution", BooleanType(), False),
    StructField("priority", IntegerType(), False),
    StructField("enabled", BooleanType(), False),
])

# %% [markdown]
# ## `meta_dq_rules` — the data-quality contract, as data
#
# Eight rule types, in two categories that behave differently on purpose.
#
# **Row rules** give a verdict per row, so a failing row can be quarantined:
#
# | Type | `rule_params` | Meaning |
# |---|---|---|
# | `not_null` | — | column is populated |
# | `unique` | — | no duplicate values (post-dedupe) |
# | `range` | `min`, `max` | numeric bounds, either side optional |
# | `enum_domain` | `values` | value is in the allowed set |
# | `referential` | `ref_layer`, `ref_table`, `ref_column` | value exists in a parent table |
# | `expression` | `predicate` | arbitrary SQL that must be **true** for every row |
#
# **Batch rules** give one verdict per load and quarantine nothing:
#
# | Type | `rule_params` | Meaning |
# |---|---|---|
# | `freshness` | `max_age_days` | the newest value has not fallen behind the batch's arrival date |
# | `continuity` | `max_missing_days` | no day is absent from the span the batch covers |
#
# The split is not cosmetic. What a batch rule detects is rows that are **absent** — a feed that
# stopped producing, a missing rate date — and a row that does not exist cannot be quarantined.
# Collapsing the two categories is how a completeness check ends up rejecting a perfectly good
# batch. `expression` is the deliberate escape hatch for cross-field invariants and is used twice,
# so it stays an exception rather than a habit.
#
# **`condition`** restricts a rule to a subset of rows. This is not a convenience: `merchant_id` is
# legitimately null for ATM withdrawals, so a blanket `not_null` would be wrong and a rule engine
# without row-level conditions would force that logic into code, which is the thing being avoided.
#
# ### Severity, and why a threshold exists
#
# - **`error`** — the run fails. Reserved for failures that mean the *feed* is broken rather than a
#   row: a missing business key, an unknown enum value (a new status code is a schema change and
#   must not be silently absorbed), a violated uniqueness constraint.
# - **`warn`** — the row is quarantined, the run continues. A single negative amount must not halt a
#   payments feed.
# - **`fail_threshold_pct`** — a `warn` rule escalates to a failure when the failure *rate* exceeds
#   it. Two hundred bad rows is a data problem; twenty percent bad rows is a broken upstream system,
#   and treating those identically is how a pipeline ships a quietly empty day.
#
# Failing rows are quarantined in **both** cases. Severity decides whether the run fails, never
# whether the row is captured.

# %%
def rule(rule_set, column, rule_type, severity="warn", condition=None,
         fail_threshold_pct=None, description="", **rule_params):
    return dict(
        rule_set=rule_set, column_name=column, rule_type=rule_type, severity=severity,
        condition=condition, fail_threshold_pct=fail_threshold_pct,
        rule_params=rule_params, description=description, enabled=True,
    )


ISO_CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD", "SEK", "NOK"]

DQ_RULES = [
    # ---- transactions ----------------------------------------------------------------
    rule("transactions", "transaction_id", "not_null", "error",
         description="No business key means no idempotency and no dedupe."),
    rule("transactions", "transaction_id", "unique", "error",
         description="Asserted after dedupe: a residual duplicate would double-count revenue."),
    rule("transactions", "account_id", "not_null", "error",
         description="An unattributable transaction cannot be reported on."),
    rule("transactions", "amount_minor", "range", "warn", min=1, fail_threshold_pct=1.0,
         description="Negative or zero authorisation amounts are quarantined, not corrected."),
    rule("transactions", "currency_code", "enum_domain", "warn", values=ISO_CURRENCIES,
         fail_threshold_pct=1.0, description="ISO 4217 subset this platform settles in."),
    rule("transactions", "status", "enum_domain", "error",
         values=["AUTHORISED", "DECLINED", "REVERSED", "CAPTURED", "SETTLED"],
         description="An unknown status is a schema change upstream, not a bad row."),
    rule("transactions", "channel", "enum_domain", "error",
         values=["POS", "ECOM", "ATM", "MOTO"],
         description="Channel drives the conditional merchant rule; an unknown one invalidates it."),
    rule("transactions", "merchant_id", "not_null", "warn", condition="channel <> 'ATM'",
         fail_threshold_pct=2.0,
         description="ATM withdrawals legitimately have no merchant — hence the condition."),
    rule("transactions", "merchant_id", "referential", "warn", condition="merchant_id is not null",
         ref_layer="silver", ref_table="dim_merchant", ref_column="merchant_id",
         fail_threshold_pct=1.0,
         description="Warehouse FKs are NOT ENFORCED on Fabric, so this rule is the guarantee."),
    rule("transactions", "decline_reason_code", "expression", "warn",
         predicate="(status = 'DECLINED') = (decline_reason_code is not null)",
         fail_threshold_pct=1.0,
         description="Cross-field invariant: a reason code iff declined. Decline analysis "
                     "silently breaks in both directions if this drifts."),

    # ---- accounts --------------------------------------------------------------------
    rule("accounts", "account_id", "not_null", "error",
         description="SCD2 merge key."),
    rule("accounts", "_op", "enum_domain", "error", values=["I", "U", "D"],
         description="An unrecognised CDC operation would be silently skipped by the merge."),
    rule("accounts", "_change_ts", "not_null", "error",
         description="Dedupe ordering and watermark column: without it, CDC ordering is undefined."),
    rule("accounts", "risk_band", "enum_domain", "warn", values=["A", "B", "C", "D", "E"],
         fail_threshold_pct=1.0, description="Risk band drives fraud-rate segmentation."),
    rule("accounts", "status", "enum_domain", "warn",
         values=["ACTIVE", "DORMANT", "FROZEN", "CLOSED"], fail_threshold_pct=1.0),
    rule("accounts", "customer_id", "not_null", "warn", fail_threshold_pct=1.0),

    # ---- customers -------------------------------------------------------------------
    rule("customers", "customer_id", "not_null", "error"),
    rule("customers", "customer_id", "unique", "error",
         description="A full snapshot with a duplicated key means a broken extract."),
    rule("customers", "kyc_status", "enum_domain", "warn",
         values=["VERIFIED", "PENDING", "FAILED"], fail_threshold_pct=1.0),
    rule("customers", "segment", "enum_domain", "warn",
         values=["RETAIL", "PREMIER", "BUSINESS"], fail_threshold_pct=1.0),
    rule("customers", "email", "not_null", "warn", fail_threshold_pct=5.0,
         description="Masked in gold, not silver, so reprocessing stays possible."),

    # ---- merchants -------------------------------------------------------------------
    rule("merchants", "merchant_id", "not_null", "error"),
    rule("merchants", "merchant_id", "unique", "error"),
    rule("merchants", "risk_score", "range", "warn", min=0, max=100, fail_threshold_pct=1.0),
    rule("merchants", "status", "enum_domain", "warn",
         values=["ACTIVE", "SUSPENDED", "TERMINATED"], fail_threshold_pct=1.0),
    rule("merchants", "mcc", "not_null", "warn", fail_threshold_pct=1.0),

    # ---- disputes --------------------------------------------------------------------
    rule("disputes", "dispute_id", "not_null", "error"),
    rule("disputes", "dispute_id", "unique", "error",
         description="The rolling 90-day reprocess re-reads rows; a duplicate here would mean the "
                     "window is double-counting rather than replacing."),
    rule("disputes", "transaction_id", "referential", "error",
         ref_layer="silver", ref_table="fact_transaction", ref_column="transaction_id",
         description="An orphan dispute inflates chargeback rate against no revenue."),
    rule("disputes", "disputed_amount_minor", "range", "warn", min=1, fail_threshold_pct=1.0),
    rule("disputes", "status", "enum_domain", "warn",
         values=["OPEN", "WON", "LOST", "WITHDRAWN"], fail_threshold_pct=1.0),
    rule("disputes", "resolved_date", "expression", "warn",
         predicate="(status <> 'OPEN') = (resolved_date is not null)", fail_threshold_pct=1.0,
         description="Dispute win rate is computed over resolved disputes only."),

    # ---- fx_rates --------------------------------------------------------------------
    rule("fx_rates", "rate_date", "not_null", "error"),
    rule("fx_rates", "rate", "range", "error", min=0.000001,
         description="A zero or negative rate would silently zero out GBP-normalised volume — the "
                     "worst class of defect, because the report still renders."),
    rule("fx_rates", "from_currency", "enum_domain", "warn",
         values=[c for c in ISO_CURRENCIES if c != "GBP"], fail_threshold_pct=1.0),
    rule("fx_rates", "rate_date", "freshness", "warn", max_age_days=3, fail_threshold_pct=None,
         description="Detects a feed that has stopped producing: the newest rate_date falling "
                     "behind the batch's arrival date. Catches a trailing gap, not an interior "
                     "one — that is what the continuity rule below is for."),
    rule("fx_rates", "rate_date", "continuity", "warn", max_missing_days=0,
         fail_threshold_pct=None,
         description="Catches the injected interior rate gaps: days absent from [min, max]. A "
                     "batch-level rule by necessity — the defect is a row that does not exist, and "
                     "a missing row cannot be quarantined. Recorded, not fatal: gold forward-fills "
                     "from the last known rate and flags the fill rather than dropping the "
                     "transaction, because a dropped transaction is a worse answer than a stale "
                     "rate."),

    # ---- card_products ---------------------------------------------------------------
    rule("card_products", "card_product_code", "not_null", "error"),
    rule("card_products", "card_product_code", "unique", "error"),
    rule("card_products", "annual_fee_minor", "range", "warn", min=0, fail_threshold_pct=1.0),
]

DQ_RULES_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_set", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("rule_type", StringType(), False),
    StructField("rule_params", StringType(), False),   # JSON object
    StructField("condition", StringType(), True),       # SQL predicate narrowing the row scope
    StructField("severity", StringType(), False),       # error | warn
    StructField("fail_threshold_pct", StringType(), True),
    StructField("description", StringType(), True),
    StructField("enabled", BooleanType(), False),
])

# %% [markdown]
# ## Operational tables
#
# Created empty with an explicit schema rather than inferred on first write: an inferred schema from
# an empty or partial first batch is how a `long` column silently becomes a `string`.

# %%
WATERMARK_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("watermark_value", StringType(), True),
    StructField("last_batch_id", StringType(), True),
    StructField("updated_ts", TimestampType(), True),
])

DQ_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("severity", StringType(), False),
    StructField("rows_evaluated", LongType(), False),
    StructField("rows_failed", LongType(), False),
    StructField("failed_pct", StringType(), True),
    # passed | quarantined | failed_run | batch_warn | not_evaluated. `failed_run` is what makes a
    # DQ gate visible after the fact; `not_evaluated` is what stops a rule that never ran from
    # looking like a rule that passed.
    StructField("outcome", StringType(), False),
    # The measure behind the outcome, in words: "7/50158 row(s) (0.014%)", or for a batch rule
    # "5 missing day(s) across 120d span [2026-05-24..2026-09-20]". A batch rule's defect is not a
    # row count, so without this column its finding would be recorded as zeroes and the table would
    # explain nothing at exactly the moment someone needed it to.
    StructField("detail", StringType(), True),
    StructField("evaluated_ts", TimestampType(), False),
])

RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("step", StringType(), False),
    StructField("status", StringType(), False),      # running | succeeded | failed | skipped
    StructField("started_ts", TimestampType(), False),
    StructField("ended_ts", TimestampType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("error_message", StringType(), True),
])

# name -> (schema, partition columns)
#
# `meta_watermark` is partitioned by `entity` even though it holds one row per feed — seven tiny
# files, which looks absurd until you run the pipeline in parallel. Every entity MERGEs into this
# one table concurrently, and Delta's optimistic concurrency raises ConcurrentAppendException when
# two transactions commit against files the other read. Partitioning on the column the MERGE
# predicate filters is the documented remedy: concurrent writers then touch disjoint files. The
# other half of the fix lives in `src/lib/watermark.py`, which must supply a *literal* partition
# predicate so Delta can prune statically — `t.entity = s.entity` alone cannot be resolved at plan
# time, because the value is on the source side.
#
# The two history tables need none of this: they are blind appends, and a blind append never
# conflicts. That is a second, unplanned dividend from choosing an append-only run log.
OPERATIONAL_TABLES = {
    "meta_watermark": (WATERMARK_SCHEMA, ["entity"]),
    "meta_dq_results": (DQ_RESULTS_SCHEMA, None),
    "meta_run_log": (RUN_LOG_SCHEMA, None),
}


# %%
def seed_source_config() -> int:
    spark = get_spark()
    rows = [
        (
            c["entity"], c["source_format"], json.dumps(c["read_options"], sort_keys=True),
            c["target_table"], c["load_type"], c["watermark_column"], c["merge_keys"],
            c["scd_type"], c["dq_rule_set"], c["partition_column"], c["reprocess_window_days"],
            c["allow_schema_evolution"], c["priority"], c["enabled"],
        )
        for c in SOURCE_CONFIG
    ]
    df = spark.createDataFrame(rows, SOURCE_CONFIG_SCHEMA)
    write_table(df, Layer.META, "meta_source_config", mode="overwrite")
    return len(rows)


def rule_id(r: dict) -> str:
    """Deterministic, readable rule id.

    Stable across reseeds so `meta_dq_results` history keeps pointing at the same rule — an
    ordinal-based id would renumber every rule after an insertion, silently re-attributing history.

    The shape implies a constraint: at most one rule of a given type per column per rule set.
    That is a deliberate limit, not an oversight — two `range` rules on one column is always
    clearer expressed as one rule with both bounds. `validate()` enforces it.
    """
    return f"{r['rule_set']}.{r['column_name']}.{r['rule_type']}"


def seed_dq_rules() -> int:
    spark = get_spark()
    rows = []
    for r in DQ_RULES:
        rows.append((
            rule_id(r), r["rule_set"], r["column_name"], r["rule_type"],
            json.dumps(r["rule_params"], sort_keys=True), r["condition"], r["severity"],
            None if r["fail_threshold_pct"] is None else str(r["fail_threshold_pct"]),
            r["description"] or None, r["enabled"],
        ))
    df = spark.createDataFrame(rows, DQ_RULES_SCHEMA)
    write_table(df, Layer.META, "meta_dq_rules", mode="overwrite")
    return len(rows)


def ensure_operational_tables(reset: bool) -> None:
    spark = get_spark()
    for name, (schema, partition_by) in OPERATIONAL_TABLES.items():
        exists = table_exists(Layer.META, name)
        if exists and not reset:
            log.info("preserved %-16s (operational state)", name)
            continue
        if exists and reset:
            log.warning("RESET %-16s — operational history discarded", name)
        write_table(spark.createDataFrame([], schema), Layer.META, name, mode="overwrite",
                    partition_by=partition_by)
        log.info("created  %-16s%s", name,
                 f" partitioned by {partition_by}" if partition_by else "")


def validate() -> None:
    """Cross-check the two declarative tables against each other.

    Config naming a rule set that does not exist is the failure mode this catches: the pipeline
    would run, quarantine nothing, and look healthy.
    """
    cfg = {c["entity"]: c["dq_rule_set"] for c in SOURCE_CONFIG}
    rule_sets = {r["rule_set"] for r in DQ_RULES}
    missing = {e: rs for e, rs in cfg.items() if rs not in rule_sets}
    if missing:
        raise ValueError(f"entities reference an undefined dq_rule_set: {missing}")
    orphan = rule_sets - set(cfg.values())
    if orphan:
        raise ValueError(f"rule sets defined but referenced by no entity: {sorted(orphan)}")

    ids = [rule_id(r) for r in DQ_RULES]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ValueError(
            f"duplicate rule_id(s) {dupes} — at most one rule of a given type per column per rule "
            "set; combine them (e.g. one range rule with both min and max)"
        )

    for r in DQ_RULES:
        if r["severity"] not in ("error", "warn"):
            raise ValueError(f"{r['rule_set']}.{r['column_name']}: bad severity {r['severity']!r}")
        if r["severity"] == "error" and r["fail_threshold_pct"] is not None:
            raise ValueError(
                f"{r['rule_set']}.{r['column_name']}: an error rule already fails the run, so a "
                "threshold on it is contradictory"
            )
        required = {
            "range": ({"min", "max"}, False),
            "enum_domain": ({"values"}, True),
            "referential": ({"ref_layer", "ref_table", "ref_column"}, True),
            "freshness": ({"max_age_days"}, True),
            "expression": ({"predicate"}, True),
        }.get(r["rule_type"])
        if required:
            keys, all_required = required
            present = keys & set(r["rule_params"])
            if (all_required and present != keys) or (not all_required and not present):
                raise ValueError(
                    f"{r['rule_set']}.{r['column_name']} ({r['rule_type']}): "
                    f"expected params {sorted(keys)}, got {sorted(r['rule_params'])}"
                )


# %%
def main() -> None:
    reset = bool(PARAMS["reset"])
    validate()
    n_cfg = seed_source_config()
    n_rules = seed_dq_rules()
    ensure_operational_tables(reset)
    log.info("control plane seeded: %d feeds, %d dq rules%s",
             n_cfg, n_rules, " (operational history reset)" if reset else "")

    log.info("enabled feeds by execution priority:")
    for r in (read_table(Layer.META, "meta_source_config")
              .filter("enabled").orderBy("priority", "entity").collect()):
        log.info("  %2d  %-14s %-22s scd%-2s %s", r["priority"], r["entity"],
                 r["load_type"], r["scd_type"],
                 f"reprocess {r['reprocess_window_days']}d"
                 if r["reprocess_window_days"] else "")


if __name__ == "__main__":
    main()
