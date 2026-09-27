"""Bronze ingest: reconciliation, provenance, idempotency and schema evolution.

Every test here runs against a private lake root (see `tests/conftest.py`) so the suite is
order-independent. The three that matter most are the last three: idempotency by watermark,
idempotency by batch replay, and schema evolution. Those are the properties a reviewer will
actually doubt.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pytest
from pyspark.sql import functions as F

from src.lib import watermark
from src.notebooks import nb_01_bronze_ingest as nb01
from src.runtime.context import Layer, get_spark, landing_path, read_table, table_exists
from tests.conftest import point_at

ENTITIES = ("card_products", "fx_rates", "accounts", "customers", "merchants",
            "transactions", "disputes")
AUDIT_COLUMNS = ("_batch_id", "_ingest_ts", "_source_file", "_row_hash")

# The month-10 boundary from the generator: files before this date genuinely lack `wallet_type`.
WALLET_BOUNDARY = "2026-07-29"


@pytest.fixture(scope="module")
def bronze(isolated_lake):
    """One cold ingest of all seven feeds, shared by the read-only tests below."""
    results = {e: nb01.ingest(entity=e, run_id="test-cold") for e in ENTITIES}
    assert all(r["status"] == "succeeded" for r in results.values()), results
    return results


def bronze_df(entity: str):
    return read_table(Layer.BRONZE, f"br_{entity}")


# ----------------------------------------------------------------------------------------
# Reconciliation: bronze must be a faithful copy of what landed
# ----------------------------------------------------------------------------------------

def _landing_count(spark, entity: str, fmt: str, options: dict) -> int:
    return spark.read.format(fmt).options(**options).load(landing_path(entity)).count()


@pytest.mark.parametrize(
    ("entity", "fmt", "options"),
    # `customers` and `merchants` are absent deliberately: they are full-snapshot feeds, so bronze
    # holds one snapshot while landing holds all of them. Their equivalent check is the test below.
    [
        ("transactions", "json", {}),
        ("disputes", "json", {}),
        ("accounts", "csv", {"header": "true"}),
        ("fx_rates", "csv", {"header": "true"}),
        ("card_products", "csv", {"header": "true"}),
    ],
)
def test_incremental_and_static_feeds_land_every_source_row(bronze, entity, fmt, options):
    """Bronze is append-only and filters nothing, so a first full load must match landing exactly.

    A count that is merely *close* would mean the read is dropping rows — a malformed JSON line
    silently skipped, a CSV row lost to an unquoted delimiter. Bronze's entire value is being the
    layer you can trust to be complete, so this is an equality assertion, not a tolerance.
    """
    assert bronze_df(entity).count() == _landing_count(get_spark(), entity, fmt, options)


@pytest.mark.parametrize("entity", ["customers", "merchants"])
def test_full_snapshot_feeds_load_every_snapshot(bronze, entity):
    """A snapshot feed is not "the current state" to bronze — it is a sequence of observations.

    This test replaces one that asserted the opposite (only the newest partition lands), which was
    the codified form of a real defect: silver builds SCD2 history from the transitions *between*
    snapshots, so keeping only the newest gave `dim_merchant` and `dim_customer` one version per key
    and no history, and 78% of facts in gold then resolved to the unknown member. See `nb_01`'s
    window-selection note.

    Asserting the partition *set*, not just its size, because a count of four says nothing about
    whether the four are the right four — an off-by-one in the window would satisfy a count.
    """
    df = bronze_df(entity)
    landed = {
        r["d"] for r in df.select(F.date_format("ingest_date", "yyyy-MM-dd").alias("d")).distinct().collect()
    }
    available = {
        p.name.split("=")[1] for p in Path(landing_path(entity)).iterdir() if p.is_dir()
    }
    assert landed == available
    assert len(available) > 1, (
        f"{entity} has only {len(available)} landing snapshot(s), so this test cannot distinguish "
        "'every snapshot' from 'the newest snapshot' — check the generator's span."
    )


# ----------------------------------------------------------------------------------------
# Provenance
# ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("entity", ENTITIES)
def test_audit_columns_are_present_and_populated(bronze, entity):
    df = bronze_df(entity)
    missing = set(AUDIT_COLUMNS) - set(df.columns)
    assert not missing, f"{entity} missing audit columns: {sorted(missing)}"
    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in AUDIT_COLUMNS
    ]).first().asDict()
    assert all(v == 0 for v in nulls.values()), f"{entity} has null audit columns: {nulls}"


def test_source_file_points_at_a_real_file(bronze):
    """`_source_file` exists so "row 4,812,119 is wrong" becomes a file you can open. That only
    holds if the recorded path resolves — a relative or mangled path is worse than no column."""
    paths = [r["_source_file"] for r in
             bronze_df("transactions").select("_source_file").distinct().limit(5).collect()]
    assert paths
    for p in paths:
        # Hadoop renders local paths as `file:/a/b`, a single slash — not the `file:///a/b` a URL
        # parser would expect. `urlparse().path` handles both; stripping a literal "file://" does not.
        assert Path(urlparse(p).path).is_file(), p


def test_row_hash_ignores_arrival_metadata(bronze):
    """A customer unchanged between two monthly snapshots must hash identically.

    This is the load-bearing property behind SCD2: if `ingest_date` or `_snapshot_date` fed the hash,
    every snapshot would look like a change and silver would write history that never happened. The
    test reads landing directly because bronze only holds the newest snapshot by design.
    """
    spark = get_spark()
    raw = spark.read.option("header", "true").csv(landing_path("customers"))
    cfg = nb01.load_config("customers")
    hashed = nb01.add_audit_columns(raw, cfg, "test-batch")

    per_customer = hashed.groupBy("customer_id").agg(
        F.countDistinct("_snapshot_date").alias("snapshots"),
        F.countDistinct("_row_hash").alias("hashes"),
    )
    multi = per_customer.filter(F.col("snapshots") > 1)
    assert multi.count() > 0, "no customer appears in more than one snapshot — test is vacuous"
    # Unchanged customers dominate: most must collapse to a single hash across their snapshots.
    stable = multi.filter(F.col("hashes") == 1).count()
    assert stable / multi.count() > 0.9, "row hash is reacting to arrival metadata, not content"
    # ...and the ones the generator does change must be visible as more than one hash.
    assert multi.filter(F.col("hashes") > 1).count() > 0, "no attribute change is detectable"


def test_row_hash_is_insensitive_to_column_order(bronze):
    """Hashing over a sorted column list means a source reordering its CSV columns does not
    invalidate every row hash in the table — which would otherwise present as every dimension row
    changing on the same day for no reason."""
    spark = get_spark()
    raw = spark.read.option("header", "true").csv(landing_path("fx_rates"))
    cfg = nb01.load_config("fx_rates")
    a = nb01.add_audit_columns(raw, cfg, "b")
    b = nb01.add_audit_columns(raw.select(*reversed(raw.columns)), cfg, "b")
    key = ["rate_date", "from_currency"]
    joined = a.select(*key, F.col("_row_hash").alias("ha")).join(
        b.select(*key, F.col("_row_hash").alias("hb")), key, "inner")
    assert joined.filter(F.col("ha") != F.col("hb")).count() == 0


# ----------------------------------------------------------------------------------------
# Idempotency — the two mechanisms, tested separately
# ----------------------------------------------------------------------------------------

def test_rerun_is_a_noop_while_the_watermark_is_current(bronze):
    """Mechanism 1: the watermark decides what to read."""
    before = {e: bronze_df(e).count() for e in ENTITIES}
    second = {e: nb01.ingest(entity=e, run_id="test-rerun") for e in ENTITIES}
    assert all(r["status"] == "skipped" for r in second.values()), second
    assert {e: bronze_df(e).count() for e in ENTITIES} == before


def test_forced_reload_replaces_its_batch_rather_than_duplicating_it(bronze):
    """Mechanism 2: `_batch_id` decides what a retry replaces.

    This is the case a watermark cannot cover — a run that died after writing half its rows never
    advanced the watermark, so the next attempt re-reads the same window. Without delete-by-batch
    that attempt double-counts, and the derived batch id is what makes the delete possible: a
    clock-based id would produce a new batch every time and never match anything to remove.
    """
    before = {e: (bronze_df(e).count(), bronze_df(e).select("_batch_id").distinct().count())
              for e in ENTITIES}
    for e in ENTITIES:
        assert nb01.ingest(entity=e, run_id="test-replay", force_reload=True)["status"] == "succeeded"
    after = {e: (bronze_df(e).count(), bronze_df(e).select("_batch_id").distinct().count())
             for e in ENTITIES}
    assert after == before


def test_batch_id_is_derived_from_the_window_not_the_clock():
    """The property the replay test depends on, asserted directly so its failure is unambiguous."""
    first = nb01.derive_batch_id("transactions", ["2026-05-24", "2026-09-20"])
    second = nb01.derive_batch_id("transactions", ["2026-05-24", "2026-09-20"])
    assert first == second == "transactions|2026-05-24|2026-09-20"
    assert nb01.derive_batch_id("transactions", ["2026-05-24", "2026-09-21"]) != first


def test_watermark_advances_to_the_newest_partition_loaded(bronze):
    assert watermark.get("transactions") == max(
        p.name.split("=")[1] for p in Path(landing_path("transactions")).iterdir() if p.is_dir()
    )


# ----------------------------------------------------------------------------------------
# Schema evolution
# ----------------------------------------------------------------------------------------

@pytest.mark.slow
@pytest.mark.uses_isolated_lake
def test_bronze_absorbs_a_new_source_column_mid_feed(tmp_path_factory):
    """`wallet_type` appears at month 10 and bronze must widen rather than fail.

    The load has to happen in **two passes** for this to test anything. Point Spark at the whole feed
    at once and JSON schema inference merges the column in across every file before bronze ever sees
    a schema change — the test would pass while proving nothing. So: load up to the day before the
    boundary, assert the column is genuinely absent, then load the remainder and assert it appeared
    with nulls for the earlier rows.

    The null-ness of the earlier rows is the real assertion. A pipeline that "handled" evolution by
    reloading the feed would show `wallet_type` populated throughout, which is a different and much
    more expensive behaviour than the one being claimed.
    """
    from src.notebooks import nb_99_seed_metadata as nb99

    point_at(tmp_path_factory.mktemp("lake_evo"))
    nb99.main()

    day_before = "2026-07-28"
    first = nb01.ingest(entity="transactions", until_date=day_before, run_id="evo-1")
    assert first["status"] == "succeeded"
    pass_one = bronze_df("transactions")
    assert "wallet_type" not in pass_one.columns, (
        "wallet_type present before the boundary — the generator's two-pass write has regressed, "
        "and this test can no longer prove anything"
    )
    rows_before = pass_one.count()

    second = nb01.ingest(entity="transactions", run_id="evo-2")
    assert second["status"] == "succeeded"
    evolved = bronze_df("transactions")

    assert "wallet_type" in evolved.columns, "bronze did not absorb the new column"
    assert evolved.count() == rows_before + second["rows"], "row count changed beyond the new batch"

    pre = evolved.filter(F.col("ingest_date") <= day_before)
    post = evolved.filter(F.col("ingest_date") > day_before)
    assert pre.filter(F.col("wallet_type").isNotNull()).count() == 0, (
        "pre-boundary rows have a wallet_type — the feed was reloaded rather than evolved"
    )
    assert post.filter(F.col("wallet_type").isNotNull()).count() > 0


@pytest.mark.uses_isolated_lake
def test_evolution_is_opt_in_per_feed(isolated_lake):
    """Only feeds whose config permits it get `mergeSchema`. An unexpected column on a feed that is
    not supposed to change is a source-system incident, and widening the table silently would hide
    it."""
    assert nb01.load_config("transactions")["allow_schema_evolution"] is True
    for entity in ("accounts", "customers", "merchants", "fx_rates", "card_products", "disputes"):
        assert nb01.load_config(entity)["allow_schema_evolution"] is False, entity


# ----------------------------------------------------------------------------------------
# Config is authoritative
# ----------------------------------------------------------------------------------------

def test_unknown_entity_names_the_configured_feeds(isolated_lake):
    with pytest.raises(ValueError, match="no meta_source_config row"):
        nb01.ingest(entity="chargebacks")


def test_missing_entity_parameter_is_rejected(isolated_lake):
    with pytest.raises(ValueError, match="`entity` is required"):
        nb01.ingest(entity="")


def test_unknown_load_type_is_rejected(isolated_lake):
    cfg = nb01.load_config("transactions") | {"load_type": "streaming"}
    with pytest.raises(ValueError, match="unknown load_type"):
        nb01.select_window(cfg, "", False)


def test_every_step_is_recorded_in_the_run_log(bronze):
    """Silence is not success: a step that ran must leave a row, and a skipped step must be
    distinguishable from one that never ran at all."""
    assert table_exists(Layer.META, "meta_run_log")
    log = read_table(Layer.META, "meta_run_log").filter(F.col("run_id") == "test-cold")
    assert {r["entity"] for r in log.select("entity").collect()} == set(ENTITIES)
    assert {r["status"] for r in log.select("status").collect()} == {"succeeded"}
    assert {r["layer"] for r in log.select("layer").collect()} == {"bronze"}
    assert log.filter(F.col("rows_written") <= 0).count() == 0

    skips = read_table(Layer.META, "meta_run_log").filter(F.col("status") == "skipped")
    assert skips.count() == 0 or skips.filter(F.col("error_message").isNull()).count() == 0, (
        "a skipped step must record why it skipped"
    )
