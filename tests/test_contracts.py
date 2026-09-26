"""The generator must not drift from `docs/data-contracts.md`, and must be deterministic.

These are contract tests, not unit tests: they read the landed dataset and assert the shapes and
injected defects that every downstream test depends on. If this file fails, the failure is either a
real regression in the generator or an intentional change to the contract — in which case the doc
changes in the same commit.

Defect volumes are asserted as *rates with tolerance*, never as magic numbers, because the same
contract has to hold at `--scale tiny` and `--scale demo`.
"""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.runtime.context import get_spark, landing_path

ISO_CURRENCIES = {"GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD", "SEK", "NOK"}


@pytest.fixture(scope="module")
def spark():
    return get_spark()


@pytest.fixture(scope="module")
def tx(spark) -> DataFrame:
    return spark.read.json(landing_path("transactions")).cache()


@pytest.fixture(scope="module")
def accounts(spark) -> DataFrame:
    return spark.read.option("header", "true").csv(landing_path("accounts")).cache()


@pytest.fixture(scope="module")
def customers(spark) -> DataFrame:
    return spark.read.option("header", "true").csv(landing_path("customers")).cache()


@pytest.fixture(scope="module")
def merchants(spark) -> DataFrame:
    return spark.read.parquet(landing_path("merchants")).cache()


@pytest.fixture(scope="module")
def disputes(spark) -> DataFrame:
    return spark.read.json(landing_path("disputes")).cache()


@pytest.fixture(scope="module")
def fx(spark) -> DataFrame:
    return spark.read.option("header", "true").csv(landing_path("fx_rates")).cache()


def rate(part: int, whole: int) -> float:
    return part / whole if whole else 0.0


# --------------------------------------------------------------------------- transactions
def test_duplicate_transactions_injected_at_contract_rate(tx):
    """0.3% exact duplicates. Proves dedupe has something to remove."""
    total = tx.count()
    dupes = total - tx.select("transaction_id").distinct().count()
    assert dupes > 0
    assert rate(dupes, total) == pytest.approx(0.003, abs=0.0015)


def test_dq_defects_present(tx):
    """Every DQ rule in the contract must have rows that trip it."""
    assert tx.filter("amount_minor < 0").count() > 0, "no negative amounts to quarantine"
    assert tx.filter(~F.col("currency_code").isin(*ISO_CURRENCIES)).count() > 0
    assert tx.filter("channel <> 'ATM' and merchant_id is null").count() > 0
    assert tx.filter("(status = 'DECLINED') <> (decline_reason_code is not null)").count() > 0


def test_atm_transactions_never_carry_a_merchant(tx):
    """The not_null rule on merchant_id is conditional for a reason, not as an escape hatch."""
    assert tx.filter("channel = 'ATM' and merchant_id is not null").count() == 0


def test_status_lifecycle_columns_are_consistent(tx):
    """capture_ts and settlement_date are functions of status; gold's settlement lag depends on it."""
    assert tx.filter("(status = 'SETTLED') <> (settlement_date is not null)").count() == 0
    assert tx.filter("(status in ('CAPTURED','SETTLED')) <> (capture_ts is not null)").count() == 0


def test_wallet_type_drift_is_real_not_simulated(tx):
    """The column must be absent from early files, not present-and-null.

    Written in two passes split at the month-10 boundary. If this ever became a null-filled column
    across the whole feed, bronze's schema-evolution path would stop being exercised.
    """
    assert "wallet_type" in tx.columns
    blocks = {
        r["missing"]: (r["lo"], r["hi"])
        for r in tx.groupBy(F.col("wallet_type").isNull().alias("missing"))
        .agg(F.min("ingest_date").alias("lo"), F.max("ingest_date").alias("hi"))
        .collect()
    }
    assert set(blocks) == {True, False}, "expected both a pre-drift and post-drift block"
    assert blocks[True][1] < blocks[False][0], "drift must be a clean cutover, not interleaved"


def test_money_is_integer_minor_units(tx):
    assert dict(tx.dtypes)["amount_minor"] == "bigint"


# --------------------------------------------------------------------------- accounts CDC
def test_cdc_feed_has_seed_updates_and_logical_deletes(accounts):
    ops = {r["_op"]: r["c"] for r in accounts.groupBy("_op").agg(F.count("*").alias("c")).collect()}
    assert set(ops) == {"I", "U", "D"}
    assert ops["I"] == accounts.select("account_id").distinct().count(), "seed must cover every account"
    assert ops["U"] > 0 and ops["D"] > 0


def test_logical_deletes_never_hard_delete(accounts):
    """`_op = D` closes the SCD2 row. It must present as a CLOSED state, not a vanished key."""
    assert accounts.filter("_op = 'D' and status <> 'CLOSED'").count() == 0


def test_accounts_have_attribute_history_for_scd2(accounts):
    changed = (
        accounts.groupBy("account_id")
        .agg(F.countDistinct("risk_band").alias("bands"))
        .filter("bands > 1")
        .count()
    )
    assert changed > 0, "no risk_band movement — SCD2 would have nothing to close out"


# --------------------------------------------------------------------------- master data
def test_master_data_lands_as_repeated_full_snapshots(customers, merchants):
    """Monthly snapshots, uniform width — the contract's stated cadence."""
    for df, key in ((customers, "customer_id"), (merchants, "merchant_id")):
        snaps = df.select("ingest_date").distinct().count()
        assert snaps >= 2, "a single snapshot gives SCD2 no history to close out"
        widths = df.groupBy("ingest_date").count().select("count").distinct().collect()
        assert len(widths) == 1, "full snapshots must have identical width"
        assert df.count() == snaps * df.select(key).distinct().count()


def test_master_data_has_attribute_history(customers, merchants):
    assert (
        customers.groupBy("customer_id").agg(F.countDistinct("segment").alias("d"))
        .filter("d > 1").count() > 0
    )
    assert (
        merchants.groupBy("merchant_id").agg(F.countDistinct("risk_score").alias("d"))
        .filter("d > 1").count() > 0
    )


# --------------------------------------------------------------------------- fx + disputes
def test_fx_has_exactly_the_contracted_gaps(fx, spark):
    """Five missing dates. Gold must forward-fill and flag, not drop the transaction."""
    dates = fx.select("rate_date").distinct()
    lo, hi = dates.select(F.min("rate_date"), F.max("rate_date")).first()
    span = spark.sql(
        f"select explode(sequence(to_date('{lo}'), to_date('{hi}'), interval 1 day)) as d"
    ).select(F.col("d").cast("string").alias("rate_date"))
    assert span.join(dates, "rate_date", "left_anti").count() == 5

    # A gap is a whole missing day, not a partially missing one.
    widths = fx.groupBy("rate_date").count().select("count").distinct().collect()
    assert len(widths) == 1


def test_disputes_arrive_late_and_within_the_window(disputes, tx):
    """0-90 days after the transaction, landed on the day raised.

    This is the feed that makes a naive "process today's partition" design quietly wrong.
    """
    assert disputes.filter("raised_date <> ingest_date").count() == 0

    auth = tx.select("transaction_id", F.to_date("auth_ts").alias("auth_date")).distinct()
    joined = disputes.join(auth, "transaction_id")
    assert joined.count() == disputes.count(), "orphan dispute — no matching transaction"

    lags = joined.select(F.datediff("raised_date", "auth_date").alias("lag"))
    lo, hi = lags.select(F.min("lag"), F.max("lag")).first()
    assert lo >= 0, "a dispute cannot precede its transaction"
    assert hi <= 90, "outside the 90-day reprocessing window the pipeline claims to cover"
    assert lags.filter("lag > 0").count() > 0, "no dispute is actually late"
    assert hi > 30, "lags too compressed to exercise the rolling window"


def test_disputes_resolution_is_consistent(disputes):
    assert disputes.filter("(status <> 'OPEN') <> (resolved_date is not null)").count() == 0


# --------------------------------------------------------------------------- referential
def test_transaction_foreign_keys_resolve(tx, spark):
    """Constraints are NOT ENFORCED on Fabric Warehouse, so referential integrity is generated in
    and tested here rather than assumed from the database."""
    products = spark.read.option("header", "true").csv(landing_path("card_products"))
    assert products.count() == 12
    assert tx.join(products, "card_product_code", "left_anti").count() == 0
