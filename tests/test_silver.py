"""Silver layer — the reconciliation, the type gate, and the two idempotency mechanisms.

The claim this file exists to defend is the one on the README's reconciliation line:

    bronze rows = silver rows + quarantined + deduped

That equation is only worth printing if it can fail. It is checked here against the *recorded*
counts in `meta_run_log` rather than against numbers this test computes for itself — because a test
that recomputes both sides proves the arithmetic and nothing about the pipeline. If `nb_02` ever
writes a row it does not account for, the run log is where the discrepancy has to show up, since the
run log is what a person reads at 3am.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.lib import contracts, dq
from src.runtime.context import Layer, read_table

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def loaded(isolated_lake):
    """Bronze and silver loaded once, for every entity, in the module's private lake."""
    from src.notebooks import nb_01_bronze_ingest as nb01
    from src.notebooks import nb_02_silver_transform as nb02

    entities = [
        "card_products", "fx_rates", "accounts", "customers", "merchants",
        "transactions", "disputes",
    ]
    bronze = {e: nb01.ingest(entity=e, run_id="test-silver") for e in entities}
    silver = {e: nb02.transform(entity=e, run_id="test-silver") for e in entities}
    return bronze, silver


# --------------------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------------------

def test_every_bronze_row_is_accounted_for(loaded):
    """Written, quarantined or deduplicated — a row may not simply disappear.

    `rows_deduped` is recorded rather than derived as `read - written - quarantined` precisely so
    this can be an assertion. Derive one term from the other two and the equation holds by algebra
    no matter what the pipeline did.
    """
    log = read_table(Layer.META, "meta_run_log").filter(
        "layer = 'silver' AND status = 'succeeded'"
    )
    rows = log.groupBy("entity").agg(
        F.sum("rows_read").alias("read"),
        F.sum("rows_written").alias("written"),
        F.sum("rows_quarantined").alias("quarantined"),
        F.sum("rows_deduped").alias("deduped"),
    ).collect()
    assert rows, "no successful silver steps were logged at all"

    unexplained = []
    for r in rows:
        # SCD2 writes *versions*, not rows: one CDC row can close a predecessor and insert a
        # successor, and a no-op change writes nothing. So the equation is asserted on the feeds
        # whose silver grain is still a row, and the SCD2 feeds get their own invariants in
        # tests/test_scd2.py. Stating that boundary is the honest version; quietly applying the
        # equation to a dimension and tuning it until it passed would be the other one.
        if r["entity"] in {"accounts", "customers", "merchants"}:
            continue
        accounted = r["written"] + r["quarantined"] + r["deduped"]
        if accounted != r["read"]:
            unexplained.append((r["entity"], r["read"], accounted))
    assert not unexplained, f"rows unaccounted for: {unexplained}"


def test_silver_row_counts_match_what_the_run_log_claims(loaded):
    """The run log is not allowed to be optimistic about tables anyone can count."""
    _, silver = loaded
    for entity in ("transactions", "disputes", "fx_rates", "card_products"):
        from src.lib import config

        table = config.load_source_config(entity)["target_table"]
        actual = read_table(Layer.SILVER, table).count()
        assert actual == silver[entity]["rows"], f"{entity}: {table} has {actual} rows"


def test_defects_were_actually_quarantined_not_merely_counted(loaded):
    """A quarantine count with no quarantine table behind it is a number, not a control."""
    quarantined = read_table(Layer.QUARANTINE, f"{dq.QUARANTINE_PREFIX}transactions")
    assert quarantined.count() > 0, "the generator injects defects; none reached quarantine"
    # Every quarantined row must name the rule that rejected it — an unactionable reject is the
    # failure mode quarantining exists to avoid.
    assert quarantined.filter(F.size(dq.RULE_IDS_COL) == 0).count() == 0
    results = read_table(Layer.META, "meta_dq_results").filter("entity = 'transactions'")
    assert results.filter("rows_failed > 0").count() > 0


# --------------------------------------------------------------------------------------
# The type gate
# --------------------------------------------------------------------------------------

def test_silver_types_match_the_contract(loaded):
    """Silver is typed, and typed to `SILVER_TYPES` — not to whatever the source files inferred."""
    from src.lib import config

    for entity in ("transactions", "disputes", "fx_rates"):
        table = config.load_source_config(entity)["target_table"]
        schema = {f.name: f.dataType.simpleString() for f in read_table(Layer.SILVER, table).schema}
        for column, expected in contracts.types_for(entity).items():
            if column in schema:
                assert schema[column] == contracts.canonical(expected), (
                    f"{entity}.{column} is {schema[column]}, contract says {expected}"
                )


def test_an_uncastable_value_is_quarantined_rather_than_cast_to_null(isolated_lake):
    """The failure the gate exists for, stated as a test.

    Without the gate this row survives: `cast('not-a-date' as date)` is `NULL`, the write succeeds,
    and the run reports clean. The defect then lives in silver as a well-typed null that no rule can
    distinguish from a legitimately absent value.
    """
    from src.runtime.context import get_spark

    spark = get_spark()
    df = spark.createDataFrame(
        [("d1", "t1", "2026-01-01", "FRAUD", "100", "GBP", "OPEN", None, "2026-01-01T00:00:00Z"),
         ("d2", "t2", "not-a-date", "FRAUD", "100", "GBP", "OPEN", None, "2026-01-01T00:00:00Z")],
        "dispute_id string, transaction_id string, raised_date string, reason_code string, "
        "disputed_amount_minor string, currency_code string, status string, resolved_date string, "
        "_event_ts string",
    )
    clean, outcome = contracts.enforce(df, "disputes", "test-cast", "b1", write_results=False)
    assert outcome.rows_quarantined == 1
    assert clean.count() == 1
    assert clean.first()["dispute_id"] == "d1"
    assert dict(clean.dtypes)["raised_date"] == "date"
    # And the rule that caught it must be recorded as breached, not silently passed.
    breached = [r for r in outcome.results if r.breached]
    assert [r.rule.rule_id for r in breached] == ["disputes.raised_date.castable"]


def test_a_type_contract_cannot_be_smuggled_in_as_a_config_rule(isolated_lake):
    """`castable` is code, not configuration — changing a column's type is a migration.

    Seeding it as a rule row would put a breaking change to a table three layers read one UPDATE
    statement away from production, so `load_rules` refuses the rule type outright.
    """
    from src.runtime.context import get_spark, write_table

    # Built from the seeded schema rather than a hand-written one, so this test cannot drift into
    # asserting something about a table shape the control plane no longer has.
    schema = read_table(Layer.META, "meta_dq_rules").schema
    values = {
        "rule_id": "smuggled.x.castable", "rule_set": "smuggled", "column_name": "x",
        "rule_type": "castable", "rule_params": '{"to": "int"}', "severity": "error",
        "enabled": True, "description": "a type change dressed up as a config change",
    }
    rule = get_spark().createDataFrame(
        [tuple(values.get(f.name) for f in schema)], schema
    )
    write_table(rule, Layer.META, "meta_dq_rules", mode="append", merge_schema=True)
    with pytest.raises(ValueError, match="derived from code"):
        dq.load_rules("smuggled")


# --------------------------------------------------------------------------------------
# Idempotency — two mechanisms, so two tests
# --------------------------------------------------------------------------------------

def test_a_second_silver_run_is_skipped_by_the_watermark(loaded):
    """The cheap mechanism: nothing is re-read, because there is nothing new to read."""
    from src.notebooks import nb_02_silver_transform as nb02

    again = nb02.transform(entity="transactions", run_id="test-silver-2")
    assert again["status"] == "skipped"
    assert "watermark" in again["reason"]


def test_a_forced_silver_reload_rewrites_nothing(loaded):
    """The mechanism that has to work when the watermark is bypassed.

    `--force-reload` re-reads every partition and presents every row to the MERGE. The `_row_hash`
    guard is what makes that free: unchanged rows match, compare equal, and are not rewritten. A
    reload that reported its input size as rows written would hide a MERGE that had started
    rewriting the table on every run — which is why `_rows_affected` reads the Delta commit instead.
    """
    from src.lib import config
    from src.notebooks import nb_02_silver_transform as nb02

    table = config.load_source_config("transactions")["target_table"]
    before = read_table(Layer.SILVER, table).count()
    forced = nb02.transform(entity="transactions", run_id="test-silver-3", force_reload=True)
    assert forced["status"] == "succeeded"
    assert forced["rows"] == 0, "a forced reload of unchanged data rewrote rows"
    assert read_table(Layer.SILVER, table).count() == before


def test_the_dispute_window_reprocesses_without_duplicating(loaded):
    """Late arrivals are absorbed, not duplicated — the reason the window is rolling.

    The 90-day window deliberately re-reads partitions silver has already processed, so the same
    `dispute_id` is presented again. Uniqueness in silver must survive that.
    """
    from src.lib import config
    from src.notebooks import nb_02_silver_transform as nb02

    table = config.load_source_config("disputes")["target_table"]
    again = nb02.transform(entity="disputes", run_id="test-silver-4")
    # Not skipped: the window means disputes always has work in scope, which is the whole point.
    assert again["status"] == "succeeded"
    assert again["rows"] == 0, "reprocessing the window rewrote unchanged disputes"
    dupes = read_table(Layer.SILVER, table).groupBy("dispute_id").count().filter("count > 1")
    assert dupes.count() == 0, dupes.take(5)


# --------------------------------------------------------------------------------------
# The dedupe grain
# --------------------------------------------------------------------------------------

def test_cdc_dimensions_dedupe_on_the_change_instant_not_the_key(loaded):
    """The mistake this guards is the one that looks correct.

    Deduplicating `accounts` on `account_id` alone would leave one row per account, pass every
    obvious check, and destroy the multi-version history SCD2 exists to record. So the grain for a
    CDC feed feeding an SCD2 dimension includes the effective-from column — asserted here against
    the config rather than against a hard-coded list, so a config change cannot quietly drop it.
    """
    from src.lib import config
    from src.notebooks import nb_02_silver_transform as nb02

    accounts = config.load_source_config("accounts")
    assert nb02.dedupe_grain(accounts) == ["account_id", "_change_ts"]

    # And the snapshot feeds must *not* include it: one snapshot per batch means the merge key is
    # already the grain, and adding `_snapshot_date` would make the check vacuous.
    customers = config.load_source_config("customers")
    assert nb02.dedupe_grain(customers) == ["customer_id"]


def test_duplicate_transactions_are_removed_and_counted(loaded):
    """The 0.3% injected duplicates, both halves: gone from silver, and recorded as gone."""
    _, silver = loaded
    assert silver["transactions"]["rows_deduped"] > 0, "no duplicates were removed"
    table = read_table(Layer.SILVER, "fact_transaction")
    dupes = table.groupBy("transaction_id").count().filter("count > 1")
    assert dupes.count() == 0, dupes.take(5)


# --------------------------------------------------------------------------------------
# The negative test: a gate that cannot be shown to fail is not a gate
# --------------------------------------------------------------------------------------

@pytest.mark.uses_isolated_lake
def test_an_error_severity_rule_fails_the_run_and_records_why(tmp_path_factory):
    """The test the whole DQ layer answers to, and the one that is easy to leave unwritten.

    A framework that evaluates every rule, quarantines every bad row, writes a tidy
    `meta_dq_results` — and then returns success anyway — is indistinguishable from a working one on
    green data. Every other DQ test in this suite would still pass. So this one takes a rule that is
    `warn` in the shipped control plane, flips it to `error`, and asserts four separate things about
    the failure, because "it raised" is the least interesting of them:

    * the exception is a `DQFailure` naming the rule (not a `Py4JJavaError` from a later write),
    * the silver watermark did **not** advance, so a retry re-reads the same window,
    * `meta_run_log` holds a `failed` row for the step, so the 3am reader sees it,
    * the offending rows are in `q_transactions`, tagged with the rule that caught them.

    The watermark assertion is the one that would catch the worst version of this bug. A gate that
    raises *after* the watermark has moved has not protected anything: the run is red, the data is
    incomplete, and the next run skips the window that was never processed. `nb_02` advances the
    watermark once, after the batch loop, precisely so that cannot happen.

    Own lake root, not the module fixture: this mutates `meta_dq_rules`, and a mutated control plane
    leaking into the other tests would make their results meaningless in a way that is very hard to
    see from a failure message.
    """
    from src.lib import watermark
    from src.notebooks import nb_01_bronze_ingest as nb01
    from src.notebooks import nb_02_silver_transform as nb02
    from src.notebooks import nb_99_seed_metadata as nb99
    from src.runtime.context import get_spark, write_table
    from tests.conftest import point_at

    rule_id = "transactions.amount_minor.range"
    point_at(tmp_path_factory.mktemp("lake_dqfail"))
    nb99.main()

    # Escalate one rule, in the control plane, exactly as an operator would — not by monkeypatching
    # `dq`. Patching the engine would test the patch; this tests the path a real change takes.
    rules = read_table(Layer.META, dq.RULES_TABLE)
    escalated = rules.withColumn(
        "severity",
        F.when(F.col("rule_id") == rule_id, F.lit("error")).otherwise(F.col("severity")),
    )
    # Collected first: overwriting a Delta table from a plan that reads it is undefined behaviour.
    write_table(
        get_spark().createDataFrame(escalated.collect(), rules.schema),
        Layer.META, dq.RULES_TABLE, mode="overwrite",
    )
    assert [r.severity for r in dq.load_rules("transactions") if r.rule_id == rule_id] == ["error"], (
        "the control-plane edit did not take, so the rest of this test proves nothing"
    )

    # `merchants` first because `transactions` carries a `referential` rule against `dim_merchant`.
    # Without it that rule is unevaluable, and a test whose subject is "which rule fired" must not
    # run against a half-built silver.
    for entity in ("merchants", "transactions"):
        assert nb01.ingest(entity=entity, run_id="dq-fail")["status"] == "succeeded"
    assert nb02.transform(entity="merchants", run_id="dq-fail")["status"] == "succeeded"

    with pytest.raises(dq.DQFailure, match=rule_id):
        nb02.transform(entity="transactions", run_id="dq-fail")

    # 1. The watermark did not move, so the window is still owed.
    assert watermark.get("transactions", layer=watermark.SILVER) is None, (
        "silver watermark advanced despite a failed gate — the next run would skip this window"
    )

    # 2. The run log explains it. Asserted as a non-empty set of failed steps rather than a count:
    #    `transactions` loads in several batches, so an earlier batch may legitimately have logged
    #    `succeeded` before a later one breached.
    log = read_table(Layer.META, "meta_run_log").filter(
        (F.col("run_id") == "dq-fail") & (F.col("entity") == "transactions")
        & (F.col("layer") == "silver") & (F.col("status") == "failed")
    )
    assert log.count() > 0, "the failed step left no row in meta_run_log"
    assert all("DQFailure" in r["error_message"] for r in log.select("error_message").collect())

    # 3. `meta_dq_results` records the verdict, at the severity that produced it.
    results = read_table(Layer.META, dq.RESULTS_TABLE).filter(
        (F.col("rule_id") == rule_id) & (F.col("severity") == "error")
    )
    assert results.count() > 0, "the breaching rule wrote no result row"
    assert {r["outcome"] for r in results.select("outcome").collect()} == {"failed_run"}
    assert results.filter(F.col("rows_failed") <= 0).count() == 0

    # 4. The rows themselves are recoverable, tagged with what caught them. Severity decides whether
    #    the run fails; it never decides whether the row is captured.
    quarantined = read_table(Layer.QUARANTINE, f"{dq.QUARANTINE_PREFIX}transactions").filter(
        F.array_contains(F.col(dq.RULE_IDS_COL), rule_id)
    )
    assert quarantined.count() > 0, (
        "no row is in quarantine for the rule that failed the run — the gate rejected a batch "
        "without keeping the evidence"
    )
    assert quarantined.filter(F.col("amount_minor").cast("long") >= 1).count() == 0, (
        "a row that satisfies the rule was quarantined by it"
    )
