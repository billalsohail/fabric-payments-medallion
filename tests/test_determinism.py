"""Generation must be reproducible, or every other guarantee in the repo is unfalsifiable.

The idempotency test reruns the whole pipeline and asserts identical results. That test is only
meaningful if the *input* is identical too — so this file pins the generator itself.

Checksums are over logical content, not file bytes: Parquet embeds writer metadata and Spark names
part-files with UUIDs, so byte comparison would fail on a dataset that is in fact identical. The
aggregate is order-independent (a sum of per-row hashes), because partition assignment is not part
of the contract.
"""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.runtime.context import get_spark, landing_path

ENTITIES = {
    "transactions": ("json", None),
    "disputes": ("json", None),
    "accounts": ("csv", None),
    "customers": ("csv", None),
    "fx_rates": ("csv", None),
    "card_products": ("csv", None),
    "merchants": ("parquet", None),
}


def _read(entity: str, fmt: str) -> DataFrame:
    spark = get_spark()
    path = landing_path(entity)
    if fmt == "json":
        return spark.read.json(path)
    if fmt == "csv":
        return spark.read.option("header", "true").csv(path)
    return spark.read.parquet(path)


def _checksum(df: DataFrame) -> tuple[int, int, tuple[str, ...]]:
    """(row count, order-independent content hash, column names)."""
    cols = tuple(sorted(df.columns))
    row_hash = F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in cols])
    agg = df.select(
        F.count("*").alias("n"),
        # Sum of hashes: commutative, so independent of partitioning and task order.
        F.sum(row_hash.cast("decimal(38,0)")).alias("h"),
    ).first()
    return agg["n"], int(agg["h"]), cols


def _snapshot() -> dict[str, tuple[int, int, tuple[str, ...]]]:
    return {e: _checksum(_read(e, fmt)) for e, (fmt, _) in ENTITIES.items()}


@pytest.mark.slow
def test_generator_is_deterministic():
    """Regenerate from scratch with the same seed; every feed must be logically identical.

    Uses the same default parameters as the landed dataset, so this leaves the landing zone in the
    state the other tests expect rather than a different one.
    """
    before = _snapshot()

    from src.notebooks import nb_00_generate_landing_data as gen

    assert gen.PARAMS["overwrite"] is True, "a partial regeneration would make this vacuous"
    gen.main()

    after = _snapshot()

    assert set(before) == set(after)
    for entity in sorted(before):
        n_b, h_b, c_b = before[entity]
        n_a, h_a, c_a = after[entity]
        assert c_b == c_a, f"{entity}: column set changed between runs"
        assert n_b == n_a, f"{entity}: row count {n_b} -> {n_a}"
        assert h_b == h_a, f"{entity}: content hash differs — generation is not deterministic"


def test_no_feed_is_empty():
    """A silently empty feed would make most assertions in the suite pass trivially."""
    for entity, (fmt, _) in ENTITIES.items():
        assert _read(entity, fmt).count() > 0, f"{entity} landed empty"
