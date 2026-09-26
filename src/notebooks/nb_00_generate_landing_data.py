# %% [markdown]
# # nb_00 — Generate landing data
#
# Stands in for seven upstream source systems by writing files into the landing zone.
#
# This is **not** fixture setup. Every defect it injects exists so that a specific mechanism further
# down the stack can be *demonstrated* rather than asserted: duplicates prove dedupe, late-arriving
# disputes prove rolling-window reprocessing, the month-10 column proves schema evolution. See
# `docs/data-contracts.md` for the full injected-defect contract.
#
# Generation is **deterministic**: every derived value is a pure function of (seed, salt, row id)
# via xxhash64, never `rand()`. That is what allows the idempotency test to rerun the whole pipeline
# and assert byte-identical results regardless of partitioning or task retries.
#
# On Fabric this runs as a notebook against the lakehouse `Files/` area; locally it writes under
# `_onelake/files/landing/`. The code is identical — see `src/runtime/context.py`.

# %%
from __future__ import annotations

import logging
import shutil
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.lib.determinism import bucket, chance, pick, uniform, weighted_pick
from src.runtime import params
from src.runtime.context import config, get_spark, landing_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nb_00")

# %% tags=["parameters"]
# Fabric injects pipeline parameters by overwriting this cell. Locally they come from CLI/env.
PARAMS = params.resolve(
    {
        "scale": "tiny",          # tiny | demo
        "seed": 20260929,         # any change reshapes the entire dataset
        "as_of_date": "2026-09-20",  # fixed so the dataset is reproducible, not clock-dependent
        "overwrite": True,
    }
)

# %%
SCALES = {
    # days: length of history. Snapshot feeds land monthly; event feeds land daily.
    #
    # `tiny` spans 120 days rather than 30 even though it is the CI profile. Row count, not span,
    # is what costs CI time — and a 30-day span produces only one monthly master-data snapshot
    # (so SCD2 has no history to close out) and truncates the 90-day dispute lag to ~26 days
    # (so late arrivals are never really late). CI has to exercise both.
    "tiny": dict(days=120, transactions=50_000, customers=2_000, accounts=3_000, merchants=500),
    "demo": dict(days=548, transactions=2_000_000, customers=50_000, accounts=80_000, merchants=8_000),
}

# Injected defect rates. Expressed as rates, not absolute counts, so they hold at any scale.
DEFECTS = dict(
    duplicate_txn=0.003,        # exact duplicate rows, transaction_id included
    null_merchant=0.005,        # null merchant_id on a non-ATM transaction
    negative_amount=0.0002,     # amount_minor < 0
    invalid_currency=0.00015,   # currency_code outside ISO 4217
    decline_mismatch=0.0005,    # decline_reason_code inconsistent with status
)

CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD", "SEK", "NOK"]
FX_CURRENCIES = [c for c in CURRENCIES if c != "GBP"]
BAD_CURRENCIES = ["XYZ", "ZZ1", "QQQ"]
REGIONS = ["LONDON", "SOUTH", "MIDLANDS", "NORTH", "SCOTLAND", "WALES", "NI"]
RISK_BANDS = ["A", "B", "C", "D", "E"]
SEGMENTS = ["RETAIL", "PREMIER", "BUSINESS"]
MCCS = ["5411", "5812", "5541", "5999", "4111", "7011", "5311", "5732", "6011", "5912"]
MCC_CATEGORY = {
    "5411": "Grocery", "5812": "Restaurants", "5541": "Fuel", "5999": "Retail",
    "4111": "Transport", "7011": "Hotels", "5311": "Department stores",
    "5732": "Electronics", "6011": "ATM", "5912": "Pharmacy",
}
DECLINE_REASONS = ["INSUFFICIENT_FUNDS", "DO_NOT_HONOUR", "EXPIRED_CARD",
                   "SUSPECTED_FRAUD", "INVALID_CVV", "LIMIT_EXCEEDED"]
DISPUTE_REASONS = ["FRAUD", "GOODS_NOT_RECEIVED", "DUPLICATE", "UNRECOGNISED", "QUALITY"]

SEED: int = int(PARAMS["seed"])
AS_OF = date.fromisoformat(str(PARAMS["as_of_date"]))


def profile() -> dict:
    scale = str(PARAMS["scale"])
    if scale not in SCALES:
        raise ValueError(f"unknown scale {scale!r}; expected one of {sorted(SCALES)}")
    return SCALES[scale]


def start_date() -> date:
    return AS_OF - timedelta(days=profile()["days"] - 1)


def wallet_boundary() -> date:
    """Date the `wallet_type` column starts appearing in the transactions feed (~month 10)."""
    days = profile()["days"]
    return start_date() + timedelta(days=int(days * 10 / 18))


# %%
def _name_pools(n: int = 300) -> tuple[list[str], list[str]]:
    """Small deterministic name pools. Faker is used once for the pools, never per row."""
    from faker import Faker

    fake = Faker("en_GB")
    Faker.seed(SEED)
    first = sorted({fake.first_name() for _ in range(n * 3)})[:n]
    last = sorted({fake.last_name() for _ in range(n * 3)})[:n]
    return first, last


def _write(df: DataFrame, entity: str, fmt: str, partition_col: str = "ingest_date",
           mode: str = "append") -> None:
    """Write a landing feed, partitioned by ingest_date exactly as the contract specifies."""
    root = landing_path(entity)
    if mode == "overwrite" and not config().is_fabric:
        shutil.rmtree(root, ignore_errors=True)
    writer = (
        df.repartition(partition_col)
        .write.mode("append")
        .partitionBy(partition_col)
    )
    if fmt == "json":
        writer.json(root)
    elif fmt == "csv":
        writer.option("header", "true").csv(root)
    elif fmt == "parquet":
        writer.parquet(root)
    else:
        raise ValueError(f"unsupported landing format {fmt!r}")
    log.info("landed %-14s %-7s -> %s", entity, fmt, root)


def _date_seq(col_alias: str, start: date, end: date, step_months: int | None = None) -> DataFrame:
    """A one-column DataFrame of dates, daily or monthly."""
    spark = get_spark()
    if step_months:
        expr = (f"sequence(to_date('{start}'), to_date('{end}'), "
                f"interval {step_months} month) as d")
    else:
        expr = f"sequence(to_date('{start}'), to_date('{end}'), interval 1 day) as d"
    return spark.sql(f"select explode({expr.replace(' as d', '')}) as {col_alias}")


# %% [markdown]
# ## card_products — static CSV
# Twelve rows, one snapshot. Small enough to state literally, which is itself the honest
# representation of static reference data.

# %%
def gen_card_products() -> None:
    spark = get_spark()
    rows = []
    for network in ("VISA", "MASTERCARD", "AMEX"):
        for tier, fee in (("CLASSIC", 0), ("GOLD", 2400), ("PLATINUM", 9900), ("BLACK", 39900)):
            code = f"{network[:2]}{tier[:2]}"
            rows.append((code, f"{network.title()} {tier.title()}", network, tier, fee,
                         str(start_date()), None, str(start_date())))
    df = spark.createDataFrame(
        rows,
        "card_product_code string, product_name string, network string, tier string, "
        "annual_fee_minor long, active_from string, active_to string, ingest_date string",
    )
    _write(df, "card_products", "csv")


# %% [markdown]
# ## customers — monthly full snapshots, CSV
# Monthly rather than daily: a daily full snapshot of every customer for 18 months would be ~137M
# rows of almost entirely unchanged data, which is neither realistic for master data nor
# laptop-sized. Monthly snapshots still give SCD2 real history to close out.

# %%
def gen_customers() -> None:
    spark = get_spark()
    n = profile()["customers"]
    first_names, last_names = _name_pools()

    base = spark.range(0, n).withColumn(
        "customer_id", F.concat(F.lit("CUST"), F.lpad(F.col("id").cast("string"), 8, "0"))
    )
    snaps = _date_seq("snapshot_date", start_date(), AS_OF, step_months=1)
    df = base.crossJoin(snaps)

    # A customer "changes" in ~2% of snapshots; version counts changes so far, which drives
    # the mutable attributes. This is what gives silver SCD2 something to close out.
    changed = chance(SEED, "cust_chg", F.concat_ws("|", F.col("customer_id"),
                                                   F.col("snapshot_date").cast("string")), 0.02)
    w = Window.partitionBy("customer_id").orderBy("snapshot_date")
    df = df.withColumn("_changed", changed.cast("int")).withColumn(
        "_version", F.sum("_changed").over(w)
    )

    seg_idx = bucket(SEED, "seg", F.col("id"), len(SEGMENTS))
    kyc_base = weighted_pick(SEED, "kyc", F.col("id"),
                             [("VERIFIED", 0.92), ("PENDING", 0.06), ("FAILED", 0.02)])

    df = (
        df.withColumn("first_name", F.element_at(
            F.array(*[F.lit(v) for v in first_names]),
            bucket(SEED, "fn", F.col("id"), len(first_names)) + F.lit(1)))
        .withColumn("last_name", F.element_at(
            F.array(*[F.lit(v) for v in last_names]),
            bucket(SEED, "ln", F.col("id"), len(last_names)) + F.lit(1)))
        .withColumn("email", F.lower(F.concat_ws(
            "", F.col("first_name"), F.lit("."), F.col("last_name"),
            F.col("id").cast("string"), F.lit("@example.com"))))
        .withColumn("dob", F.date_add(F.lit("1955-01-01").cast("date"),
                                      bucket(SEED, "dob", F.col("id"), 16_000)))
        # PENDING resolves to VERIFIED once the customer has any change event.
        .withColumn("kyc_status", F.when((kyc_base == "PENDING") & (F.col("_version") > 0),
                                         F.lit("VERIFIED")).otherwise(kyc_base))
        .withColumn("country_code", weighted_pick(
            SEED, "ctry", F.col("id"),
            [("GB", 0.85), ("IE", 0.05), ("FR", 0.03), ("DE", 0.03), ("ES", 0.02), ("NL", 0.02)]))
        # Segment can only ratchet upward, capped — mirrors how tiering actually behaves.
        .withColumn("segment", F.element_at(
            F.array(*[F.lit(s) for s in SEGMENTS]),
            (F.least(seg_idx + F.col("_version"), F.lit(len(SEGMENTS) - 1)) + F.lit(1))
            .cast("int")))
        .withColumn("marketing_opt_in", chance(SEED, "optin", F.col("id"), 0.35))
        .withColumn("_snapshot_date", F.col("snapshot_date").cast("string"))
        .withColumn("ingest_date", F.col("snapshot_date").cast("string"))
        .drop("id", "snapshot_date", "_changed", "_version")
    )
    _write(df, "customers", "csv")


# %% [markdown]
# ## merchants — monthly full snapshots, Parquet
# Parquet deliberately: proves the bronze notebook is format-agnostic through configuration rather
# than a code branch per source.

# %%
def gen_merchants() -> None:
    spark = get_spark()
    n = profile()["merchants"]
    base = spark.range(0, n).withColumn(
        "merchant_id", F.concat(F.lit("MERCH"), F.lpad(F.col("id").cast("string"), 7, "0"))
    )
    snaps = _date_seq("snapshot_date", start_date(), AS_OF, step_months=1)
    df = base.crossJoin(snaps)

    changed = chance(SEED, "merch_chg", F.concat_ws("|", F.col("merchant_id"),
                                                    F.col("snapshot_date").cast("string")), 0.04)
    w = Window.partitionBy("merchant_id").orderBy("snapshot_date")
    df = df.withColumn("_changed", changed.cast("int")).withColumn(
        "_version", F.sum("_changed").over(w))

    mcc = pick(SEED, "mcc", F.col("id"), MCCS)
    category = F.create_map(*[x for k, v in MCC_CATEGORY.items()
                              for x in (F.lit(k), F.lit(v))])[mcc]
    df = (
        df.withColumn("mcc", mcc)
        .withColumn("merchant_name", F.concat_ws(" ", F.lit("Merchant"),
                                                 F.col("id").cast("string")))
        .withColumn("category", category)
        .withColumn("country_code", weighted_pick(
            SEED, "mctry", F.col("id"), [("GB", 0.8), ("IE", 0.06), ("FR", 0.05),
                                         ("DE", 0.05), ("US", 0.04)]))
        .withColumn("acquirer_id", pick(SEED, "acq", F.col("id"),
                                        ["ACQ01", "ACQ02", "ACQ03", "ACQ04"]))
        # Risk score drifts upward with each change event, capped at 100.
        .withColumn("risk_score", F.least(
            bucket(SEED, "risk", F.col("id"), 55) + (F.col("_version") * F.lit(7)),
            F.lit(100)).cast("int"))
        .withColumn("status", F.when(F.col("risk_score") >= 90, F.lit("TERMINATED"))
                    .when(F.col("risk_score") >= 78, F.lit("SUSPENDED"))
                    .otherwise(F.lit("ACTIVE")))
        .withColumn("onboarded_date", F.date_sub(F.lit(start_date()).cast("date"),
                                                 bucket(SEED, "onb", F.col("id"), 1200)))
        .withColumn("_snapshot_date", F.col("snapshot_date").cast("string"))
        .withColumn("ingest_date", F.col("snapshot_date").cast("string"))
        .drop("id", "snapshot_date", "_changed", "_version")
    )
    _write(df, "merchants", "parquet")


# %% [markdown]
# ## accounts — daily CDC extract, CSV
# The primary SCD2 driver. Day one is a full seed (`_op = I`); after that only changed rows appear,
# with `U` for updates and `D` for logical deletes that must close the SCD2 row rather than
# hard-delete it.
#
# Change days are generated by exploding a per-account list of offsets rather than cross-joining
# accounts against every day — the cross join would generate ~44M rows to discard ~99% of them.

# %%
def gen_accounts_cdc() -> None:
    spark = get_spark()
    n = profile()["accounts"]
    days = profile()["days"]
    n_cust = profile()["customers"]

    base = (
        spark.range(0, n)
        .withColumn("account_id", F.concat(F.lit("ACCT"), F.lpad(F.col("id").cast("string"), 8, "0")))
        # Deterministic reference to an existing customer without a join.
        .withColumn("customer_id", F.concat(F.lit("CUST"), F.lpad(
            bucket(SEED, "acct_cust", F.col("id"), n_cust).cast("string"), 8, "0")))
        .withColumn("sort_code", F.lpad(
            bucket(SEED, "sort", F.col("id"), 999_999).cast("string"), 6, "0"))
        .withColumn("account_number_masked", F.concat(F.lit("****"), F.lpad(
            bucket(SEED, "acctno", F.col("id"), 9999).cast("string"), 4, "0")))
        .withColumn("account_type", weighted_pick(
            SEED, "atype", F.col("id"),
            [("CURRENT", 0.55), ("SAVINGS", 0.2), ("CREDIT", 0.25)]))
        .withColumn("region", pick(SEED, "region", F.col("id"), REGIONS))
        .withColumn("opened_date", F.date_sub(F.lit(start_date()).cast("date"),
                                              bucket(SEED, "opened", F.col("id"), 2500)))
        .withColumn("_risk_idx", bucket(SEED, "risk0", F.col("id"), len(RISK_BANDS)))
    )

    # Seed extract: every account, on day one.
    seed_rows = (
        base.withColumn("_version", F.lit(0))
        .withColumn("_change_date", F.lit(start_date()).cast("date"))
        .withColumn("_op", F.lit("I"))
    )

    # Change extracts: 0-5 changes per account at deterministic day offsets.
    n_changes = bucket(SEED, "nchg", F.col("id"), 6)
    # Note: Spark's sequence(1, 0) returns [1, 0] — it infers a descending step rather than an
    # empty array — so the zero-change case has to be guarded rather than relied upon.
    offsets = F.when(
        n_changes > 0,
        F.transform(
            F.sequence(F.lit(1), n_changes),
            lambda i: F.pmod(F.xxhash64(F.lit(SEED), F.lit("chgday"), F.col("id"), i),
                             F.lit(days - 1)).cast("int") + F.lit(1),
        ),
    ).otherwise(F.lit(None).cast("array<int>"))
    change_rows = (
        base.withColumn("_offset", F.explode_outer(offsets))
        .filter(F.col("_offset").isNotNull())
        .withColumn("_change_date", F.date_add(F.lit(start_date()).cast("date"), F.col("_offset")))
        # Version = ordinal of this change for the account, so attributes evolve monotonically.
        .withColumn("_version", F.row_number().over(
            Window.partitionBy("account_id").orderBy("_change_date")))
        .withColumn("_op", F.when(
            chance(SEED, "acct_del", F.concat_ws("|", F.col("account_id"),
                                                 F.col("_change_date").cast("string")), 0.02),
            F.lit("D")).otherwise(F.lit("U")))
        .drop("_offset")
    )

    df = seed_rows.unionByName(change_rows)

    # Risk band walks with each change; status follows _op and risk.
    risk_idx = F.least(F.col("_risk_idx") + F.col("_version"), F.lit(len(RISK_BANDS) - 1))
    df = (
        df.withColumn("risk_band", F.element_at(
            F.array(*[F.lit(b) for b in RISK_BANDS]), risk_idx + F.lit(1)))
        .withColumn("status", F.when(F.col("_op") == "D", F.lit("CLOSED"))
                    .when(F.col("risk_band") == "E", F.lit("FROZEN"))
                    .when(chance(SEED, "dorm", F.col("account_id"), 0.05), F.lit("DORMANT"))
                    .otherwise(F.lit("ACTIVE")))
        .withColumn("credit_limit_minor", F.when(
            F.col("account_type") == "CREDIT",
            (bucket(SEED, "climit", F.col("id"), 40) + F.lit(1)) * F.lit(50_000)).cast("long"))
        .withColumn("closed_date", F.when(F.col("status") == "CLOSED",
                                          F.col("_change_date").cast("string")))
        # Change timestamp: the CDC watermark and dedupe ordering column.
        .withColumn("_change_ts", F.concat(
            F.col("_change_date").cast("string"), F.lit("T"),
            F.lpad(bucket(SEED, "chgh", F.concat_ws("|", F.col("account_id"),
                                                    F.col("_change_date").cast("string")), 24)
                   .cast("string"), 2, "0"),
            F.lit(":"),
            F.lpad(bucket(SEED, "chgm", F.concat_ws("|", F.col("account_id"),
                                                    F.col("_change_date").cast("string")), 60)
                   .cast("string"), 2, "0"),
            F.lit(":00Z")))
        .withColumn("opened_date", F.col("opened_date").cast("string"))
        .withColumn("ingest_date", F.col("_change_date").cast("string"))
        .drop("id", "_risk_idx", "_version", "_change_date")
    )
    _write(df, "accounts", "csv")


# %% [markdown]
# ## fx_rates — daily reference, CSV
# Five dates are deliberately missing. GBP-normalised volume must therefore forward-fill from the
# last known rate and flag it, not silently drop the transaction. The `freshness` DQ rule catches
# the gap; the gold load applies the fill.

# %%
def gen_fx_rates() -> list[str]:
    days = _date_seq("rate_date", start_date(), AS_OF)
    ccy = get_spark().createDataFrame([(c,) for c in FX_CURRENCIES], "from_currency string")
    df = days.crossJoin(ccy)

    # Deterministic gaps: five evenly spread dates with no rates at all.
    span = profile()["days"]
    gap_offsets = sorted({int(span * f) for f in (0.17, 0.33, 0.51, 0.68, 0.86)})
    gap_dates = [str(start_date() + timedelta(days=o)) for o in gap_offsets]
    df = df.filter(~F.col("rate_date").cast("string").isin(gap_dates))

    base_rates = {"USD": 0.79, "EUR": 0.86, "JPY": 0.0053, "CHF": 0.90,
                  "AUD": 0.52, "CAD": 0.58, "SEK": 0.074, "NOK": 0.072}
    base_map = F.create_map(*[x for k, v in base_rates.items()
                              for x in (F.lit(k), F.lit(v))])
    wobble = (uniform(SEED, "fx", F.concat_ws("|", F.col("rate_date").cast("string"),
                                              F.col("from_currency"))) - F.lit(0.5)) * F.lit(0.04)
    df = (
        df.withColumn("to_currency", F.lit("GBP"))
        .withColumn("rate", F.round(base_map[F.col("from_currency")] * (F.lit(1.0) + wobble), 8))
        .withColumn("source", F.lit("ECB_DAILY"))
        .withColumn("ingest_date", F.col("rate_date").cast("string"))
        .withColumn("rate_date", F.col("rate_date").cast("string"))
    )
    _write(df, "fx_rates", "csv")
    log.info("fx gaps injected on: %s", gap_dates)
    return gap_dates


# %% [markdown]
# ## transactions — daily events, JSONL
# Grain is one authorisation attempt, not one payment: a decline followed by a successful retry is
# two rows, which is precisely what makes authorisation rate measurable.
#
# Written in two passes, split at the month-10 boundary, so files before that date genuinely lack
# the `wallet_type` column. JSON carries schema per file, so this produces real schema drift for
# bronze to absorb rather than a simulated flag.

# %%
def gen_transactions() -> DataFrame:
    spark = get_spark()
    p = profile()
    n, days = p["transactions"], p["days"]
    tid = F.col("id")

    channel = weighted_pick(SEED, "chan", tid,
                            [("POS", 0.55), ("ECOM", 0.35), ("ATM", 0.06), ("MOTO", 0.04)])
    status = weighted_pick(SEED, "status", tid,
                           [("SETTLED", 0.72), ("CAPTURED", 0.10), ("DECLINED", 0.12),
                            ("AUTHORISED", 0.04), ("REVERSED", 0.02)])
    auth_date = F.date_add(F.lit(start_date()).cast("date"), bucket(SEED, "day", tid, days))
    # Hours skewed toward waking hours rather than uniform across midnight.
    hour = F.when(chance(SEED, "night", tid, 0.12), bucket(SEED, "h_n", tid, 7)) \
        .otherwise(bucket(SEED, "h_d", tid, 16) + F.lit(7))

    df = (
        spark.range(0, n)
        .withColumn("transaction_id", F.concat(F.lit("TXN"), F.lpad(tid.cast("string"), 12, "0")))
        .withColumn("account_id", F.concat(F.lit("ACCT"), F.lpad(
            bucket(SEED, "txn_acct", tid, p["accounts"]).cast("string"), 8, "0")))
        .withColumn("card_id", F.concat(F.lit("CARD"), F.lpad(
            bucket(SEED, "card", tid, p["accounts"] * 2).cast("string"), 10, "0")))
        .withColumn("channel", channel)
        .withColumn("status", status)
        .withColumn("_auth_date", auth_date)
        .withColumn("auth_ts", F.concat(
            auth_date.cast("string"), F.lit("T"),
            F.lpad(hour.cast("string"), 2, "0"), F.lit(":"),
            F.lpad(bucket(SEED, "min", tid, 60).cast("string"), 2, "0"), F.lit(":"),
            F.lpad(bucket(SEED, "sec", tid, 60).cast("string"), 2, "0"), F.lit("Z")))
        .withColumn("mcc", F.when(F.col("channel") == "ATM", F.lit("6011"))
                    .otherwise(pick(SEED, "txn_mcc", tid, MCCS)))
        .withColumn("card_product_code", pick(SEED, "prod", tid,
                    [f"{n_[:2]}{t[:2]}" for n_ in ("VISA", "MASTERCARD", "AMEX")
                     for t in ("CLASSIC", "GOLD", "PLATINUM", "BLACK")]))
        .withColumn("country_code", weighted_pick(
            SEED, "txn_ctry", tid, [("GB", 0.88), ("IE", 0.04), ("FR", 0.03),
                                    ("DE", 0.03), ("US", 0.02)]))
        # ATM transactions legitimately have no merchant; the injected nulls are the non-ATM ones,
        # which is why the DQ rule has to be conditional rather than a blanket not-null.
        .withColumn("merchant_id", F.when(F.col("channel") == "ATM", F.lit(None))
                    .when(chance(SEED, "nullmerch", tid, DEFECTS["null_merchant"]), F.lit(None))
                    .otherwise(F.concat(F.lit("MERCH"), F.lpad(
                        bucket(SEED, "txn_merch", tid, p["merchants"]).cast("string"), 7, "0"))))
        .withColumn("_amount_base", (
            F.pow(F.lit(10.0), uniform(SEED, "amt_mag", tid) * F.lit(2.6)) * F.lit(100)
        ).cast("long") + F.lit(150))
        .withColumn("amount_minor", F.when(
            chance(SEED, "neg", tid, DEFECTS["negative_amount"]),
            -F.col("_amount_base")).otherwise(F.col("_amount_base")))
        .withColumn("currency_code", F.when(
            chance(SEED, "badccy", tid, DEFECTS["invalid_currency"]),
            pick(SEED, "whichbad", tid, BAD_CURRENCIES)
        ).otherwise(F.when(F.col("country_code") == "GB", F.lit("GBP"))
                    .otherwise(pick(SEED, "ccy", tid, CURRENCIES))))
        .withColumn("is_3ds", F.when(F.col("channel") == "ECOM",
                                     chance(SEED, "3ds", tid, 0.7)).otherwise(F.lit(False)))
        .withColumn("device_id", F.when(F.col("channel") == "ECOM", F.concat(
            F.lit("DEV"), F.lpad(bucket(SEED, "dev", tid, 500_000).cast("string"), 8, "0"))))
        # decline_reason_code must be non-null iff status = DECLINED. The mismatch defect breaks
        # that invariant in both directions, which is what the cross-field DQ rule catches.
        .withColumn("_mismatch", chance(SEED, "dmm", tid, DEFECTS["decline_mismatch"]))
        .withColumn("decline_reason_code", F.when(
            (F.col("status") == "DECLINED") & ~F.col("_mismatch"),
            pick(SEED, "dreason", tid, DECLINE_REASONS)
        ).when((F.col("status") != "DECLINED") & F.col("_mismatch"),
               pick(SEED, "dreason2", tid, DECLINE_REASONS)))
        .withColumn("capture_ts", F.when(
            F.col("status").isin("CAPTURED", "SETTLED"),
            F.concat(F.date_add(F.col("_auth_date"),
                                bucket(SEED, "capd", tid, 2)).cast("string"),
                     F.lit("T12:00:00Z"))))
        .withColumn("settlement_date", F.when(
            F.col("status") == "SETTLED",
            F.date_add(F.col("_auth_date"), bucket(SEED, "setd", tid, 3) + F.lit(1)).cast("string")))
        .withColumn("ingest_date", F.col("_auth_date").cast("string"))
    )

    # 0.3% exact duplicates — same transaction_id, same everything. Tests dedupe, not upsert.
    dupes = df.filter(chance(SEED, "dupe", tid, DEFECTS["duplicate_txn"]))
    df = df.unionByName(dupes)

    keep = ["transaction_id", "account_id", "card_id", "merchant_id", "card_product_code",
            "amount_minor", "currency_code", "auth_ts", "capture_ts", "settlement_date",
            "status", "decline_reason_code", "mcc", "channel", "country_code", "is_3ds",
            "device_id", "ingest_date"]

    boundary = wallet_boundary()
    log.info("wallet_type column appears from %s", boundary)
    pre = df.filter(F.col("_auth_date") < F.lit(boundary)).select(*keep)
    post = (
        df.filter(F.col("_auth_date") >= F.lit(boundary))
        .withColumn("wallet_type", F.when(F.col("channel") == "ECOM", weighted_pick(
            SEED, "wallet", tid, [("NONE", 0.55), ("APPLE_PAY", 0.28), ("GOOGLE_PAY", 0.17)]))
            .otherwise(F.lit("NONE")))
        .select(*keep, "wallet_type")
    )
    _write(pre, "transactions", "json")
    _write(post, "transactions", "json")

    return df.select("transaction_id", "_auth_date", "amount_minor", "currency_code", "status")


# %% [markdown]
# ## disputes — late-arriving events, JSONL
# Raised 0-90 days *after* the transaction, and landed on the day they were raised. This is the feed
# that makes a naive "process today's partition" design produce quietly wrong chargeback numbers.

# %%
def gen_disputes(txns: DataFrame) -> None:
    key = F.col("transaction_id")
    df = (
        txns.dropDuplicates(["transaction_id"])
        .filter(F.col("status") == "SETTLED")
        .filter(chance(SEED, "disp", key, 0.012))
        .withColumn("dispute_id", F.concat(F.lit("DSP"), F.substring(F.md5(key), 1, 16)))
        .withColumn("_lag_days", bucket(SEED, "displag", key, 91))
        .withColumn("raised_date", F.date_add(F.col("_auth_date"), F.col("_lag_days")))
        # Disputes raised beyond the dataset horizon simply have not arrived yet.
        .filter(F.col("raised_date") <= F.lit(AS_OF))
        .withColumn("reason_code", weighted_pick(
            SEED, "dreason", key, [("FRAUD", 0.38), ("GOODS_NOT_RECEIVED", 0.24),
                                   ("UNRECOGNISED", 0.18), ("DUPLICATE", 0.11),
                                   ("QUALITY", 0.09)]))
        .withColumn("disputed_amount_minor", F.greatest(
            (F.col("amount_minor") * (F.lit(0.5) + uniform(SEED, "dpct", key) * F.lit(0.5)))
            .cast("long"), F.lit(100)))
        .withColumn("status", weighted_pick(
            SEED, "dstat", key, [("WON", 0.34), ("LOST", 0.41), ("OPEN", 0.17),
                                 ("WITHDRAWN", 0.08)]))
        .withColumn("_resolve_days", bucket(SEED, "dres", key, 45) + F.lit(5))
        .withColumn("resolved_date", F.when(
            F.col("status") != "OPEN",
            F.date_add(F.col("raised_date"), F.col("_resolve_days")).cast("string")))
        .withColumn("_event_ts", F.concat(F.col("raised_date").cast("string"),
                                          F.lit("T09:30:00Z")))
        .withColumn("ingest_date", F.col("raised_date").cast("string"))
        .withColumn("raised_date", F.col("raised_date").cast("string"))
        .select("dispute_id", "transaction_id", "raised_date", "reason_code",
                "disputed_amount_minor", "currency_code", "status", "resolved_date",
                "_event_ts", "ingest_date")
    )
    _write(df, "disputes", "json")


# %%
def main() -> None:
    cfg = config()
    log.info("generating scale=%s seed=%s span=%s..%s env=%s",
             PARAMS["scale"], SEED, start_date(), AS_OF, cfg.env.value)

    if PARAMS["overwrite"] and not cfg.is_fabric:
        root = Path(landing_path("_")).parent
        shutil.rmtree(root, ignore_errors=True)
        log.info("cleared landing root %s", root)

    gen_card_products()
    gen_customers()
    gen_merchants()
    gen_accounts_cdc()
    gen_fx_rates()
    txns = gen_transactions()
    gen_disputes(txns)
    log.info("landing generation complete")


if __name__ == "__main__":
    main()
