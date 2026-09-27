"""Gold layer — the silver → gold half of the reconciliation, and the unknown-member accounting.

`tests/test_silver.py` owns `bronze = silver + quarantined + deduped`. This file continues the chain
into the warehouse, where the arithmetic changes shape: gold does not drop rows, so the differences
it is allowed to introduce are enumerable — exactly one unknown member per dimension that needs one,
and nothing else. Anything a proc invents or loses shows up as a term that does not balance.

The centrepiece is not a count, though. It is `test_every_unknown_member_has_a_named_cause`:
every `-1` surrogate key in the fact tables must be attributable to a cause this repo has decided is
correct, with zero left over. That test is here because its absence cost real hours. The dimensions
looked healthy by every obvious measure — one current row per key, no overlaps, no gaps, counts
tying to silver — while 78% of `fact_transaction` pointed at the unknown merchant, because bronze
was discarding all but the newest monthly snapshot and the dimensions therefore had no history for
the point-in-time join to find. Every count in this file tied *while that was true*. A count test
cannot see it; only asking "and why is this key unresolved?" can.

`make run` (bronze + silver) and `gold.load()` are run once for the module in a private lake, so the
whole file reads one warehouse.
"""
from __future__ import annotations

import pytest
from pyspark.sql import Row

from src.lib import gold
from src.runtime.context import get_spark

pytestmark = pytest.mark.slow

ENTITIES = (
    "card_products", "fx_rates", "accounts", "customers", "merchants",
    "transactions", "disputes",
)

# The dimensions whose -1 row is a *failed lookup* sentinel. `dim_decline_reason` is deliberately
# absent: its -1 row is the seeded `N/A` member, which every approved transaction resolves to
# legitimately, so "how many facts point at -1" is not a defect signal there. See 03_facts.sql.
UNKNOWN_MEMBER_DIMS = {
    "dim_account": "account_sk",
    "dim_customer": "customer_sk",
    "dim_merchant": "merchant_sk",
    "dim_card_product": "card_product_sk",
    "dim_currency": "currency_sk",
    "dim_decline_reason": "decline_reason_sk",
    "dim_date": "date_sk",
}

# Every foreign key on fact_transaction, with the dimension it points at.
FACT_TRANSACTION_FKS = {
    "date_sk": "dim_date",
    "account_sk": "dim_account",
    "customer_sk": "dim_customer",
    "merchant_sk": "dim_merchant",
    "card_product_sk": "dim_card_product",
    "currency_sk": "dim_currency",
    "decline_reason_sk": "dim_decline_reason",
}


@pytest.fixture(scope="module")
def warehouse(isolated_lake):
    """Bronze, silver and gold loaded once into the module's private lake."""
    from src.notebooks import nb_01_bronze_ingest as nb01
    from src.notebooks import nb_02_silver_transform as nb02

    # The Spark catalog is per session and this lake is per module, so a `dbo` registered by an
    # earlier module would still point at its lake. `drop_gold` refuses a mismatch rather than
    # deleting the wrong warehouse, so this is safe to call unconditionally.
    gold.drop_gold()
    for e in ENTITIES:
        nb01.ingest(entity=e, run_id="test-gold")
    for e in ENTITIES:
        nb02.transform(entity=e, run_id="test-gold")
    results = gold.load()
    yield results
    gold.drop_gold()


def q(sql: str) -> list[Row]:
    return get_spark().sql(sql).collect()


def one(sql: str):
    """The single value of a single-column, single-row query."""
    rows = q(sql)
    assert len(rows) == 1, f"expected one row, got {len(rows)}:\n{sql}"
    return rows[0][0]


# --------------------------------------------------------------------------------------
# Row counts: silver → gold
# --------------------------------------------------------------------------------------

def test_every_proc_reported_success(warehouse):
    """The procs' own account of the load, read back from stg.load_log rather than from the return
    value — `gold.load()` returning ten results proves Python ran, not that the SQL committed."""
    rows = q("SELECT proc_name, status, rows_inserted FROM stg.load_log ORDER BY proc_name")
    assert len(rows) == 10, f"expected 10 procs to log, got {len(rows)}: {[r[0] for r in rows]}"
    bad = [(r["proc_name"], r["status"]) for r in rows if r["status"] != "SUCCEEDED"]
    assert not bad, f"procs did not succeed: {bad}"


@pytest.mark.parametrize(
    ("fact", "stage"),
    [("fact_transaction", "fact_transaction"), ("fact_dispute", "fact_dispute")],
)
def test_fact_counts_tie_exactly_to_staging(warehouse, fact, stage):
    """A fact load resolves keys; it does not filter. So this is equality, not a tolerance.

    An inner join to a dimension that is missing a key would silently drop rows here, which is the
    classic way a star schema loses money — and the reason every lookup in the procs is a LEFT JOIN
    with a COALESCE to -1 rather than an inner join.
    """
    assert one(f"SELECT COUNT(*) FROM dbo.{fact}") == one(f"SELECT COUNT(*) FROM stg.{stage}")


@pytest.mark.parametrize(
    ("dim", "stage", "key"),
    [
        ("dim_account", "dim_account", "account_sk"),
        ("dim_customer", "dim_customer", "customer_sk"),
        ("dim_merchant", "dim_merchant", "merchant_sk"),
        ("dim_card_product", "dim_card_product", "card_product_sk"),
    ],
)
def test_dimension_counts_are_staging_plus_one_unknown_member(warehouse, dim, stage, key):
    """Exactly one row more than silver: the -1 member. Not "at least one" — a second sentinel, or a
    duplicated version, would satisfy a `>=` and break every point-in-time join."""
    assert one(f"SELECT COUNT(*) FROM dbo.{dim}") == one(f"SELECT COUNT(*) FROM stg.{stage}") + 1


def test_fx_rates_carry_no_unknown_member(warehouse):
    """`dim_fx_rate` is the one loaded dimension with no sentinel, because nothing holds a surrogate
    key to it: the gold load resolves a rate into `fact_transaction.fx_rate` as a value. A sentinel
    row would be an unreferenced row asserting that an unknown currency-day exists."""
    assert one("SELECT COUNT(*) FROM dbo.dim_fx_rate") == one("SELECT COUNT(*) FROM stg.dim_fx_rate")


@pytest.mark.parametrize(("dim", "key"), sorted(UNKNOWN_MEMBER_DIMS.items()))
def test_unknown_member_exists_exactly_once(warehouse, dim, key):
    assert one(f"SELECT COUNT(*) FROM dbo.{dim} WHERE {key} = -1") == 1


@pytest.mark.parametrize(
    "spec", [s for s in gold.STAGING if s.mode == "full"], ids=lambda s: s.silver
)
def test_full_mode_staging_tables_hold_the_whole_silver_table(warehouse, spec):
    """The staging contract, asserted rather than commented.

    `src/lib/gold.py` declares each mirror as `full` or `batch`, and five of the seven are `full`.
    Two procs' correctness arguments rest on that declaration — `03_sp_load_dim_account.sql` explains
    why it still omits `WHEN NOT MATCHED BY SOURCE` despite full staging making the branch safe, and
    `06_sp_load_dim_fx_rate.sql` computes validity intervals with LEAD across the whole rate history
    and names this test. Neither file can check it, because the mode is set in Python.

    Note what this does *not* assert: that the `batch` mirrors hold one batch. The fixture calls
    `gold.load()` with no batch ids, which over-stages them deliberately (see `fill_staging`), so at
    this moment they hold everything too. Asserting a subset would pass vacuously here and the
    documented default would go unexercised either way.
    """
    from src.runtime.context import Layer, read_table

    silver_rows = read_table(Layer.SILVER, spec.silver).count()
    assert one(f"SELECT COUNT(*) FROM {spec.staging}") == silver_rows, (
        f"{spec.staging} is declared mode='full' ({spec.why}) but does not hold all "
        f"{silver_rows} row(s) of silver {spec.silver!r}"
    )


def test_money_ties_exactly_between_staging_and_gold(warehouse):
    """In minor units, so this is integer arithmetic and an exact equality is meaningful. The same
    assertion on a float column would be a tolerance test dressed up as a reconciliation."""
    assert one("SELECT SUM(amount_minor) FROM dbo.fact_transaction") == one(
        "SELECT SUM(amount_minor) FROM stg.fact_transaction"
    )
    assert one("SELECT SUM(disputed_amount_minor) FROM dbo.fact_dispute") == one(
        "SELECT SUM(disputed_amount_minor) FROM stg.fact_dispute"
    )


# --------------------------------------------------------------------------------------
# Referential integrity — the thing NOT ENFORCED constraints do not give us
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(("fk", "dim"), sorted(FACT_TRANSACTION_FKS.items()))
def test_no_orphan_foreign_keys_on_fact_transaction(warehouse, fk, dim):
    """Fabric Warehouse accepts `FOREIGN KEY ... NOT ENFORCED` and nothing else, so the engine will
    happily serve an orphan and the optimiser will *trust* the declaration while doing it. The
    integrity guarantee is therefore the DQ layer plus this test, which is the honest version of
    "we have foreign keys"."""
    key = fk if dim != "dim_date" else "date_sk"
    orphans = one(
        f"SELECT COUNT(*) FROM dbo.fact_transaction f "
        f"LEFT JOIN dbo.{dim} d ON d.{key} = f.{fk} WHERE d.{key} IS NULL"
    )
    assert orphans == 0, f"fact_transaction.{fk} has {orphans} row(s) with no {dim} row"


@pytest.mark.parametrize(
    ("fk", "dim", "key"),
    [
        ("raised_date_sk", "dim_date", "date_sk"),
        ("resolved_date_sk", "dim_date", "date_sk"),
        ("account_sk", "dim_account", "account_sk"),
        ("merchant_sk", "dim_merchant", "merchant_sk"),
        ("currency_sk", "dim_currency", "currency_sk"),
    ],
)
def test_no_orphan_foreign_keys_on_fact_dispute(warehouse, fk, dim, key):
    orphans = one(
        f"SELECT COUNT(*) FROM dbo.fact_dispute f "
        f"LEFT JOIN dbo.{dim} d ON d.{key} = f.{fk} WHERE d.{key} IS NULL"
    )
    assert orphans == 0, f"fact_dispute.{fk} has {orphans} row(s) with no {dim} row"


def test_open_disputes_and_only_open_disputes_use_the_unknown_date(warehouse):
    """`resolved_date_sk = -1` iff the dispute is open — asserted in both directions.

    One direction alone is half a test: "every open dispute has -1" passes if *everything* has -1,
    and "every -1 is open" passes if nothing does.
    """
    assert one(
        "SELECT COUNT(*) FROM dbo.fact_dispute WHERE is_open = 1 AND resolved_date_sk <> -1"
    ) == 0
    assert one(
        "SELECT COUNT(*) FROM dbo.fact_dispute WHERE is_open = 0 AND resolved_date_sk = -1"
    ) == 0


# --------------------------------------------------------------------------------------
# The unknown member: accounted for by cause, not merely counted
# --------------------------------------------------------------------------------------

def test_scd2_dimensions_actually_carry_history(warehouse):
    """More versions than keys, and at least one closed version, for all three SCD2 dimensions.

    This is the direct regression test for the bronze defect described in the module docstring. It
    is phrased as `versions > keys` rather than as a row count because a row count would have to be
    updated whenever the generator's scale changed, and would then be updated *past* a regression.
    """
    for dim, key in [
        ("dim_account", "account_id"),
        ("dim_customer", "customer_id"),
        ("dim_merchant", "merchant_id"),
    ]:
        sk = UNKNOWN_MEMBER_DIMS[dim]
        row = q(
            f"SELECT COUNT(*) AS versions, COUNT(DISTINCT {key}) AS keys, "
            f"SUM(CASE WHEN is_current THEN 0 ELSE 1 END) AS closed "
            f"FROM dbo.{dim} WHERE {sk} <> -1"
        )[0]
        assert row["versions"] > row["keys"], (
            f"{dim} has {row['versions']} version(s) for {row['keys']} key(s) — no history at all. "
            "Bronze is probably not landing every snapshot; see nb_01's window-selection note."
        )
        assert row["closed"] > 0, f"{dim} has no closed versions"


# Each row is a dimension whose surrogate key the fact resolves by a *direct* point-in-time join on
# a natural key the fact itself carries. `customer_sk` is deliberately absent: proc 07 resolves it by
# chaining through the account, so it has no direct key to classify against — see the test below it.
#
# `max_before_first` is the tolerated share of facts predating their key's first dimension version,
# as a fraction of the fact table. Zero for the monthly snapshot feeds and 2% for the CDC feed, and
# that asymmetry is the point of parameterising it rather than using one number: see the test body.
_DIRECT_KEYS = [
    # sk, dim, natural key, nullable at source, max_before_first
    ("merchant_sk", "dim_merchant", "merchant_id", True, 0.0),
    ("account_sk", "dim_account", "account_id", False, 0.02),
]


@pytest.mark.parametrize(("sk", "dim", "key", "nullable", "max_before_first"), _DIRECT_KEYS)
def test_every_unknown_member_has_a_named_cause(warehouse, sk, dim, key, nullable, max_before_first):
    """Partition the `-1` facts by cause and require the residual to be zero.

    Three causes this repo has decided are correct:

    1. **The source key is null.** `merchant_id` is legitimately absent on ATM withdrawals, which
       `docs/data-contracts.md` contracts and the DQ rule exempts. There is no version to look for.
    2. **The fact predates the key's first version.** The `accounts` CDC extract starts at the
       extract window, so a transaction on an account opened before it has no covering version.
       Bounded, not pinned — the size of this depends on how much of an account's life precedes the
       window, which is a generator parameter. Zero for `merchants`, where the first snapshot lands
       on the first day of the fact history; that alignment is a coincidence of the generator's
       parameters rather than a property of the design, so pinning it at exactly zero is what makes
       a future change to the fact span fail *here* instead of as a quiet slice of revenue
       attributed to an unknown merchant.
    3. **A hole in the chain.** `_op = 'D'` closes a version and inserts no successor, so a fact
       after the delete falls outside every interval — deliberately, because the entity did not
       exist then. `src/lib/scd2.py` states this as the reason the contiguity invariant is "no
       overlaps, and no gaps except where a delete closed the chain", and a delete is the only thing
       that can open one. Structurally zero for the snapshot feeds, which carry no `_op` column.

    And two that are defects, asserted to be absent:

    - **The key has no version at all.** Silver loaded the feed, so every key in it has at least one
      version. A fact referencing a key the dimension has never heard of means the dimension load
      dropped keys the fact load can see.
    - **A version covers the instant, and the proc still returned -1.** This is the residual that
      matters, and it is why the query below re-runs the proc's own point-in-time join rather than
      reasoning about first/last version dates: anything that reaches this bucket is a join the proc
      got wrong, and no amount of counting rows would find it.

    This test exists because its absence cost real hours. Bronze was discarding all but the newest
    monthly snapshot, so the dimensions had no history for the point-in-time join to land in, and
    78% of `fact_transaction` pointed at the unknown merchant. Every other assertion in this file
    passed while that was true — one current row per key is exactly what a reviewer checks.
    """
    fact_total = one("SELECT COUNT(*) FROM dbo.fact_transaction")
    row = q(f"""
        WITH miss AS (
            SELECT f.transaction_id, s.{key} AS src_key, s.auth_ts
              FROM dbo.fact_transaction f
              JOIN stg.fact_transaction s ON s.transaction_id = f.transaction_id
             WHERE f.{sk} = -1
        ),
        span AS (
            SELECT {key} AS src_key, MIN(valid_from) AS first_from
              FROM dbo.{dim} WHERE {sk} <> -1 GROUP BY {key}
        ),
        -- The proc's own join, replayed. The `<> -1` matters: the unknown member is seeded with
        -- valid_from 1900-01-01 and valid_to 9999-12-31, so without it every instant is "covered".
        covered AS (
            SELECT DISTINCT m.transaction_id
              FROM miss m
              JOIN dbo.{dim} d
                ON d.{key} = m.src_key
               AND d.{sk} <> -1
               AND m.auth_ts >= d.valid_from
               AND m.auth_ts <  d.valid_to
        )
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN m.src_key IS NULL THEN 1 ELSE 0 END) AS source_key_null,
               SUM(CASE WHEN m.src_key IS NOT NULL AND sp.src_key IS NULL THEN 1 ELSE 0 END)
                   AS key_absent_from_dimension,
               SUM(CASE WHEN c.transaction_id IS NOT NULL THEN 1 ELSE 0 END)
                   AS covered_but_unresolved,
               SUM(CASE WHEN m.src_key IS NOT NULL AND sp.src_key IS NOT NULL
                         AND c.transaction_id IS NULL AND m.auth_ts < sp.first_from
                        THEN 1 ELSE 0 END) AS before_first_version,
               SUM(CASE WHEN m.src_key IS NOT NULL AND sp.src_key IS NOT NULL
                         AND c.transaction_id IS NULL AND m.auth_ts >= sp.first_from
                        THEN 1 ELSE 0 END) AS hole_in_chain
          FROM miss m
          LEFT JOIN span sp ON sp.src_key = m.src_key
          LEFT JOIN covered c ON c.transaction_id = m.transaction_id
    """)[0].asDict()

    assert row["covered_but_unresolved"] == 0, (
        f"{sk}: {row['covered_but_unresolved']} fact(s) fall inside a version of {dim} and still "
        f"resolved to -1. The dimension has the history; proc 07's point-in-time join is wrong."
    )
    assert row["key_absent_from_dimension"] == 0, (
        f"{sk}: {row['key_absent_from_dimension']} fact(s) reference a key with no version at all "
        f"in {dim} — the dimension load dropped keys the fact load can see."
    )
    assert row["before_first_version"] <= max_before_first * fact_total, (
        f"{sk}: {row['before_first_version']} of {fact_total} fact(s) predate the first version of "
        f"their key in {dim}, above the tolerated {max_before_first:.0%}. If {dim} is fed by "
        "snapshots, check that bronze is landing every partition — a dimension built from only the "
        "newest snapshot has no interval old enough to cover an old fact."
    )
    assert row["total"] == (
        row["source_key_null"] + row["before_first_version"] + row["hole_in_chain"]
    ), f"{sk}: unknown members do not add up, so some cause is unaccounted for: {row}"

    if nullable:
        assert row["source_key_null"] > 0, (
            f"no fact has a null {key}, so the contracted ATM case is not being exercised and "
            "cause (1) of this test is vacuous — check the generator"
        )
    else:
        assert row["source_key_null"] == 0, f"{key} is not nullable at source but has nulls: {row}"
        assert row["hole_in_chain"] > 0, (
            f"{dim} has no facts falling in a chain hole, so cause (3) is vacuous here — the "
            "accounts feed is supposed to contain logical deletes (see docs/data-contracts.md)"
        )


def test_customer_resolution_fails_exactly_where_account_resolution_fails(warehouse):
    """`customer_sk = -1` iff `account_sk = -1`, in both directions.

    The transaction carries no `customer_id`: proc 07 resolves the customer from whoever the
    *then-current account version* said owned the account, so customer resolution is strictly
    downstream of account resolution and has no natural key of its own to classify against. That
    chain gives it two extra ways to fail, and this is the test that says neither happens:

    - `customer_sk = -1` where the account resolved — the account named an owner, and
      `dim_customer` had no version covering the instant. Given monthly snapshots and a customer who
      cannot be deleted, that should be impossible; if it starts happening, `dim_customer`'s history
      has developed a hole and the named-cause test above will never see it, because it does not
      look at `dim_customer`.
    - `account_sk` resolved to a version whose `customer_id` is the seeded `UNKNOWN` — a real
      account pointing at the unknown customer, which is a silver referential-integrity failure
      wearing a surrogate key.

    Asserted as set equality rather than as equal counts, because two different rowsets of the same
    size is exactly the failure this is meant to catch.
    """
    both, a_only, c_only = q("""
        SELECT SUM(CASE WHEN account_sk = -1 AND customer_sk = -1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN account_sk = -1 AND customer_sk <> -1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN account_sk <> -1 AND customer_sk = -1 THEN 1 ELSE 0 END)
          FROM dbo.fact_transaction
    """)[0]
    assert a_only == 0, (
        f"{a_only} fact(s) resolved a customer from an unresolved account — the chain in proc 07 "
        "produced a customer for an account it could not find"
    )
    assert c_only == 0, (
        f"{c_only} fact(s) resolved an account but not its customer. Either dim_customer's history "
        "has a hole, or an account version names a customer_id that dim_customer does not contain."
    )
    assert both > 0, (
        "no fact has an unresolved account, so this test is vacuous — it is asserting a "
        "correspondence between two empty sets"
    )


def test_declined_transactions_all_resolve_a_decline_reason(warehouse):
    """The -1 on `decline_reason_sk` is the seeded `N/A` member, so the meaningful assertion is not
    "how many are -1" but "is it -1 in exactly the right rows" — both directions again."""
    assert one("""
        SELECT COUNT(*) FROM dbo.fact_transaction f JOIN stg.fact_transaction s
          ON s.transaction_id = f.transaction_id
         WHERE s.status = 'DECLINED' AND f.decline_reason_sk = -1
    """) == 0
    assert one("""
        SELECT COUNT(*) FROM dbo.fact_transaction f JOIN stg.fact_transaction s
          ON s.transaction_id = f.transaction_id
         WHERE s.status <> 'DECLINED' AND f.decline_reason_sk <> -1
    """) == 0
    assert one("SELECT decline_reason_code FROM dbo.dim_decline_reason WHERE decline_reason_sk = -1") == "N/A"


# --------------------------------------------------------------------------------------
# The aggregate: additive measures must tie to the fact they summarise
# --------------------------------------------------------------------------------------

def test_no_fact_has_a_null_gbp_amount(warehouse):
    """Required by `09_sp_load_agg_merchant_daily.sql`, which names this test.

    `attempted_amount_gbp_minor` sums `amount_gbp_minor`, and SUM skips NULLs silently, so the
    aggregate understates by however much FX failed to resolve. The proc's header says the
    understatement is provably zero at `tiny` scale *because this assertion exists*. Without it that
    claim is an assumption in a comment.
    """
    assert one("SELECT COUNT(*) FROM dbo.fact_transaction WHERE amount_gbp_minor IS NULL") == 0


def test_additive_aggregate_measures_tie_to_the_fact_tables(warehouse):
    """Every additive column of `agg_merchant_daily`, summed, equals the same expression over the
    fact. This is what "additive" has to mean to be worth pre-aggregating."""
    agg = q("""
        SELECT SUM(attempt_count) AS attempts, SUM(approved_count) AS approved,
               SUM(declined_count) AS declined,
               SUM(attempted_amount_gbp_minor) AS attempted_gbp,
               SUM(approved_amount_gbp_minor) AS approved_gbp,
               SUM(dispute_count) AS disputes,
               SUM(disputed_amount_gbp_minor) AS disputed_gbp
          FROM dbo.agg_merchant_daily
    """)[0].asDict()
    fact = q("""
        SELECT COUNT(*) AS attempts, SUM(is_approved) AS approved, SUM(is_declined) AS declined,
               SUM(amount_gbp_minor) AS attempted_gbp,
               SUM(CASE WHEN is_approved = 1 THEN amount_gbp_minor ELSE 0 END) AS approved_gbp
          FROM dbo.fact_transaction
    """)[0].asDict()
    disp = q("""
        SELECT COUNT(*) AS disputes, SUM(disputed_amount_gbp_minor) AS disputed_gbp
          FROM dbo.fact_dispute
    """)[0].asDict()
    for col, want in {**fact, **disp}.items():
        assert agg[col] == want, f"agg_merchant_daily.{col} sums to {agg[col]}, fact says {want}"


def test_aggregate_grain_is_one_row_per_merchant_day(warehouse):
    assert one("SELECT COUNT(*) FROM dbo.agg_merchant_daily") == one(
        "SELECT COUNT(*) FROM (SELECT DISTINCT date_sk, merchant_sk FROM dbo.agg_merchant_daily)"
    )


def test_distinct_account_count_is_not_additive_and_is_only_right_per_day(warehouse):
    """The one non-additive measure in the table, asserted to be non-additive.

    `semantic-model/` marks it day-grain-only, and that marking is only defensible if summing it
    across days genuinely differs from the distinct count over the period — otherwise the warning
    is noise a report author learns to ignore. So the test proves the hazard is real, then proves
    the measure is nonetheless correct at its own grain.
    """
    summed = one("SELECT SUM(distinct_account_count) FROM dbo.agg_merchant_daily")
    overall = one("SELECT COUNT(*) FROM (SELECT DISTINCT merchant_sk, account_sk FROM dbo.fact_transaction)")
    assert summed > overall, (
        f"summing distinct_account_count gives {summed} and the true distinct count is {overall} — "
        "if these were equal the non-additivity warning would be describing a hazard that does not "
        "exist at this scale, and the test would be asserting nothing"
    )
    mismatches = q("""
        SELECT a.date_sk, a.merchant_sk, a.distinct_account_count AS stored, f.n AS actual
          FROM dbo.agg_merchant_daily a
          JOIN (SELECT date_sk, merchant_sk, COUNT(DISTINCT account_sk) AS n
                  FROM dbo.fact_transaction GROUP BY date_sk, merchant_sk) f
            ON f.date_sk = a.date_sk AND f.merchant_sk = a.merchant_sk
         WHERE a.distinct_account_count <> f.n
         LIMIT 5
    """)
    assert not mismatches, f"distinct_account_count is wrong at its own grain: {mismatches}"


def test_dispute_only_merchant_days_are_present_with_zero_attempts(warehouse):
    """A merchant-day with a dispute raised but no authorisation attempt is a legitimate row, and
    `09_sp_load_agg_merchant_daily.sql` says so explicitly (the grain is a UNION of both sides, not
    the transaction side alone). If the union ever became a join this would find it."""
    from_union = one(
        "SELECT COUNT(*) FROM dbo.agg_merchant_daily WHERE attempt_count = 0 AND dispute_count > 0"
    )
    expected = one("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT d.raised_date_sk AS date_sk, d.merchant_sk FROM dbo.fact_dispute d
            EXCEPT
            SELECT DISTINCT f.date_sk, f.merchant_sk FROM dbo.fact_transaction f
        )
    """)
    assert from_union == expected


def test_no_negative_measures_survived_into_the_aggregate(warehouse):
    """Negative amounts are an injected defect that silver quarantines. If one reached gold it would
    show up as a merchant-day whose approved total exceeds its attempted total, or as a negative."""
    assert one("""
        SELECT COUNT(*) FROM dbo.agg_merchant_daily
         WHERE attempt_count < 0 OR approved_count < 0 OR declined_count < 0
            OR attempted_amount_gbp_minor < 0 OR approved_amount_gbp_minor < 0
            OR dispute_count < 0 OR disputed_amount_gbp_minor < 0
            OR approved_count > attempt_count
            OR approved_amount_gbp_minor > attempted_amount_gbp_minor
    """) == 0


# --------------------------------------------------------------------------------------
# The dashboard's arithmetic — verification item 8
# --------------------------------------------------------------------------------------
# `dashboard/build_dashboard.py` is a second implementation of the semantic model's measures, in SQL
# and Python instead of DAX, and the repo tolerates that duplication only because it is fenced. Two
# of the three fences are structural and live in `tests/test_dashboard.py`, which needs no Spark.
# This is the third: the numbers themselves, recomputed by a path that shares no expression with the
# dashboard's, against the warehouse this module has already loaded.
#
# "Shares no expression" is the whole requirement, and it is easy to get wrong. Re-running the
# dashboard's own SQL would prove Spark is deterministic. So each check below reaches the same
# figure through a different column: the approval count via `transaction_status` rather than
# `SUM(is_approved)`, and the average approved value via `AVG` over the rows rather than a sum
# divided by a count. Where the dashboard and the model agree by construction, a third path can
# still disagree with both — and that is the only arrangement in which agreement means anything.
#
# These live here rather than in `tests/test_dashboard.py`, despite being what that file is named
# after, because they need a loaded warehouse and this module already has one. Loading a second
# identical warehouse to satisfy a filename would add about two minutes to every run of the suite.

def _dash():
    """The dashboard's own read of this module's warehouse.

    Importing the module rather than shelling out to `make dashboard`: the target writes an HTML
    file, and what needs checking is the arithmetic behind it, not the markup. `sys.path` needs the
    `dashboard/` directory because it is a script directory and not a package — deliberately, since
    nothing should import from it except this test.
    """
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
    import build_dashboard as dash

    return dash, dash.read_gold()


def test_dashboard_authorisation_rate_agrees_with_an_independent_count(warehouse):
    """`[Authorisation Rate]` = `DIVIDE([Approved Transactions], [Attempts])`, checked against a
    count of `transaction_status = 'APPROVED'`.

    The dashboard sums `is_approved`; the model's measure sums the same column. So a third path has
    to avoid that column entirely, and `transaction_status` is the one that can — and it is not the
    same expression wearing a different name. There is no `APPROVED` status. `is_approved` is a
    *judgement*, made once in `07_sp_load_fact_transaction.sql`: AUTHORISED, CAPTURED and SETTLED
    are all approvals, because all three are attempts the issuer said yes to, and REVERSED is
    neither an approval nor a decline because it was authorised and then undone. That grouping is
    the kind of thing that gets quietly widened by someone adding a status, and summing the flag can
    never notice. Restating it here means a fourth approval-ish status has to be added in two
    places, and disagreeing about it fails this test rather than moving the headline rate.
    """
    dash, g = _dash()
    approved_by_status = one("""
        SELECT COUNT(*) FROM dbo.fact_transaction
         WHERE transaction_status IN ('AUTHORISED', 'CAPTURED', 'SETTLED')
    """)
    assert approved_by_status == g.txn["approved"], (
        f"{approved_by_status} transactions carry an approving status but SUM(is_approved) is "
        f"{g.txn['approved']} — the flag and the status disagree, so one of them is derived wrong. "
        "If a new status was added, 07_sp_load_fact_transaction.sql and this test both need to "
        "have an opinion about whether it is an approval"
    )

    attempts = one("SELECT COUNT(*) FROM dbo.fact_transaction")
    assert dash.divide(approved_by_status, attempts) == dash.divide(g.txn["approved"], attempts)
    rate = dash.divide(approved_by_status, attempts)
    assert 0.5 < rate < 1.0, (
        f"authorisation rate is {rate:.4f}. The generator targets the mid-80s, so anything outside "
        "this range means the rate is not measuring what its name says"
    )


def test_dashboard_average_approved_value_is_in_pounds_not_pence(warehouse):
    """`[Average Approved Value (GBP)]`, by `AVG` instead of SUM-over-COUNT — and the magnitude
    check that the 100x defect would have failed.

    This is the measure that caught the bug. Every money measure in the model once summed minor
    units under a `\\£#,0.00` format string, on the belief that the format string divided by 100; it
    does not, and cannot. Twenty-three structural tests passed throughout, because a format string
    that formats the wrong magnitude is consistent with every structural property there is. What
    failed was looking at the rendered page: it reported an average approved payment of £6,379.00.

    So this test asserts two different things, and the second is the one that matters. The first is
    that `AVG(amount_gbp_minor)` over approved rows agrees with the dashboard's
    `SUM(...) / COUNT(...)` — arithmetic, and it held before the fix as well. The second is that the
    result is a plausible card payment in pounds. The old model would have put £6,379.00 here and
    failed on that line, which is the property worth having: a test that fails on the actual defect
    rather than on a restatement of the code.
    """
    dash, g = _dash()
    avg_minor = one("""
        SELECT AVG(CAST(amount_gbp_minor AS DOUBLE))
          FROM dbo.fact_transaction
         WHERE transaction_status IN ('AUTHORISED', 'CAPTURED', 'SETTLED')
    """)
    by_avg = dash.pounds(avg_minor)
    by_sum = dash.divide(dash.pounds(g.txn["approved_gbp_minor"]), g.txn["approved"])

    # Float division of a sum against an average of the same rows; equal to well within a penny.
    assert abs(by_avg - by_sum) < 0.005, (
        f"AVG gives £{by_avg:.2f} and SUM/COUNT gives £{by_sum:.2f} for the same rows"
    )

    assert 5.0 < by_avg < 1000.0, (
        f"average approved value is £{by_avg:,.2f}, which is not a card payment. Minor units are "
        "pence: a figure a hundred times too large here means a money measure stopped dividing by "
        "100, or started dividing twice. See the minor-units note in "
        "semantic-model/definition/tables/dim_currency.tmdl"
    )
