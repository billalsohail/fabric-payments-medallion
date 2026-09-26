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
