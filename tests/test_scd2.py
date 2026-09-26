"""SCD2 invariants — plan verification item #4.

The invariants are asserted against the *real* generated CDC feed rather than a hand-built frame,
because the interesting cases are the ones a hand-built frame would forget to include: a key that
changes five times, a key deleted and then reactivated, and ~32% of rows being updates that change
nothing. A fixture that only contains the cases the author remembered is a fixture that agrees with
the implementation by construction.

Two hand-built cases *are* here, at the bottom: out-of-order CDC and a delete-only batch. Those
cannot be drawn from the generator because the generator does not produce them, and they are exactly
where an SCD2 implementation is most likely to be quietly wrong.
"""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.lib import scd2
from src.runtime.context import Layer, read_table

ENTITY = "accounts"
TABLE = "dim_account"
KEYS = ["account_id"]
EFFECTIVE = "_change_ts"


# `_change_ts` is a *string* in bronze — bronze appends the source bytes and casts nothing, so the
# contract's `timestamp` type only exists from silver onwards. These two helpers keep every
# comparison in this file on the timestamp side of that boundary rather than comparing a collected
# datetime against an ISO string, which is how the same instant ends up with two values.
SOURCE_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _ts(value: str) -> datetime:
    return datetime.strptime(value, SOURCE_TS_FORMAT)


def _effective():
    return F.col(EFFECTIVE).cast("timestamp")


def _midpoint(cdc) -> datetime:
    """An instant with roughly half the feed either side of it, for split-load tests."""
    bounds = cdc.select(F.min(_effective()).alias("lo"), F.max(_effective()).alias("hi")).first()
    return bounds["lo"] + (bounds["hi"] - bounds["lo"]) / 2



@pytest.fixture(scope="module")
def cdc(isolated_lake):
    """The accounts CDC feed, loaded to bronze in the module's private lake."""
    from src.notebooks import nb_01_bronze_ingest as nb01

    nb01.ingest(entity=ENTITY, run_id="test-scd2")
    return read_table(Layer.BRONZE, "br_accounts")


@pytest.fixture(scope="module")
def dim(cdc):
    """``dim_account`` after one full SCD2 load."""
    batch_id = cdc.select("_batch_id").first()["_batch_id"]
    scd2.merge(cdc, table=TABLE, keys=KEYS, effective_from_col=EFFECTIVE,
               batch_id=batch_id, op_col="_op")
    return read_table(Layer.SILVER, TABLE)


@pytest.fixture(scope="module")
def chain(dim):
    """Each version with its successor's ``valid_from``, for interval assertions."""
    w = Window.partitionBy(*KEYS).orderBy(scd2.VALID_FROM)
    return dim.withColumn("_next_from", F.lead(scd2.VALID_FROM).over(w))


def _deleted_keys(cdc):
    return cdc.filter("_op = 'D'").select(*KEYS).distinct()


# --------------------------------------------------------------------------------------
# Core invariants
# --------------------------------------------------------------------------------------

def test_exactly_one_current_version_per_live_key(dim, cdc):
    """The invariant everyone checks, and on its own the weakest of the set.

    It is necessary but nowhere near sufficient: a merge that collapses a key's five changes into
    one row satisfies it perfectly while destroying the history that SCD2 exists to keep. That is
    what the contiguity and multi-version tests below are for.
    """
    multi = dim.filter(F.col(scd2.IS_CURRENT)).groupBy(*KEYS).count().filter("count > 1")
    assert multi.count() == 0, multi.take(5)

    # Every key that is not terminally deleted must *have* a current version.
    last_op = (
        cdc.withColumn("_rn", F.row_number().over(
            Window.partitionBy(*KEYS).orderBy(F.desc(EFFECTIVE))))
        .filter("_rn = 1")
    )
    live = last_op.filter("_op <> 'D'").select(*KEYS)
    missing = live.join(dim.filter(F.col(scd2.IS_CURRENT)), KEYS, "left_anti")
    assert missing.count() == 0, missing.take(5)


def test_no_overlapping_versions(chain):
    """No instant may resolve to two versions of the same key — otherwise the fact join fans out."""
    overlaps = chain.filter(
        F.col("_next_from").isNotNull() & (F.col(scd2.VALID_TO) > F.col("_next_from"))
    )
    assert overlaps.count() == 0, overlaps.take(5)


def test_gaps_exist_only_where_a_delete_closed_the_chain(chain, cdc):
    """The honest form of the contiguity invariant.

    Asserting "no gaps at all" would be the stronger-sounding and weaker statement, because it is
    only achievable by pretending a deleted-then-reactivated account existed continuously. A gap is
    correct exactly when a delete caused it, so that is what is asserted — and the count is asserted
    non-zero as well, since a suite that tolerates zero gaps is not testing this feed at all.
    """
    gaps = chain.filter(
        F.col("_next_from").isNotNull() & (F.col(scd2.VALID_TO) < F.col("_next_from"))
    )
    assert gaps.count() > 0, "the accounts feed should contain reactivated keys"
    unexplained = gaps.select(*KEYS).distinct().join(_deleted_keys(cdc), KEYS, "left_anti")
    assert unexplained.count() == 0, unexplained.take(5)


def test_no_zero_width_versions(dim):
    """A version nothing can ever join to is junk in a dimension, not a record of anything.

    These appeared when a delete was written as a version of its own: ``valid_from == valid_to``,
    inert under any effective-dated join, and pure row-count inflation. A delete closes its
    predecessor instead.
    """
    zero = dim.filter(F.col(scd2.VALID_FROM) >= F.col(scd2.VALID_TO))
    assert zero.count() == 0, zero.take(5)


def test_deleted_keys_have_no_current_version(dim, cdc):
    terminal = (
        cdc.withColumn("_rn", F.row_number().over(
            Window.partitionBy(*KEYS).orderBy(F.desc(EFFECTIVE))))
        .filter("_rn = 1 AND _op = 'D'")
        .select(*KEYS)
    )
    assert terminal.count() > 0, "the feed should contain terminal deletes"
    still_open = terminal.join(dim.filter(F.col(scd2.IS_CURRENT)), KEYS, "inner")
    assert still_open.count() == 0, still_open.take(5)


def test_every_observable_state_survives_into_the_dimension(dim, cdc):
    """History is not collapsed to the latest state.

    This is the test that fails if ``build_versions`` is replaced with "take the newest row per key
    and merge it", which is what a single ``MERGE`` does and which passes every other test in this
    file.

    "Observable" is doing real work in the name. The CDC feed contains keys with two changes at the
    *same* ``_change_ts`` — a source system applying two updates inside one minute, which is
    ordinary for a batch extract. A half-open interval chain cannot represent two states at one
    instant without a zero-width version, and a zero-width version is inert under the effective-dated
    join that the dimension exists to serve: no transaction timestamp can ever fall inside it. So one
    of the two is correctly dropped, and the invariant that holds is about states that *are*
    distinguishable by a point-in-time lookup. Asserting the stronger "every state in the feed
    appears" would be asserting something untrue of any correct implementation.
    """
    attrs = [c for c in scd2.payload_columns(cdc, KEYS, EFFECTIVE, "_op") if c not in KEYS]
    observable = (
        cdc.filter("_op <> 'D'")
        .withColumn("_at_ts", F.count("*").over(Window.partitionBy(*KEYS, EFFECTIVE)))
        .filter("_at_ts = 1")
        .select(*KEYS, scd2._hash(attrs).alias("_h"))
        .distinct()
    )
    present = dim.select(*KEYS, scd2.HASH_COL).distinct()
    lost = observable.join(
        present, (observable.account_id == present.account_id)
        & (observable._h == present[scd2.HASH_COL]), "left_anti",
    )
    assert lost.count() == 0, lost.take(5)

    # The above would still pass if every key had exactly one version and the feed were flat, so
    # assert the feed actually exercises multi-version keys — otherwise the test is vacuous.
    busiest = dim.groupBy(*KEYS).count().orderBy(F.desc("count")).first()
    assert busiest["count"] >= 4, f"feed is too flat to prove anything: max {busiest['count']} versions"


def test_simultaneous_changes_collapse_to_one_version(dim, cdc):
    """Two CDC rows at the same instant produce one version, not a zero-width pair.

    Covered separately from the invariant above because it is the only place where the dimension
    deliberately holds *less* than the feed, and a reader is entitled to see that stated rather than
    inferred from a carve-out in another test.
    """
    collisions = (
        cdc.filter("_op <> 'D'")
        .groupBy(*KEYS, EFFECTIVE).count()
        .filter("count > 1")
    )
    assert collisions.count() > 0, "the feed should contain same-instant changes"
    at_instant = (
        collisions.drop("count")
        .join(dim, (collisions.account_id == dim.account_id)
              & (collisions._change_ts == dim[scd2.VALID_FROM]), "inner")
        .groupBy(collisions.account_id, collisions._change_ts).count()
        .filter("count > 1")
    )
    assert at_instant.count() == 0, at_instant.take(5)


def test_no_op_changes_do_not_create_versions(dim, cdc):
    """The hash is load-bearing: ~1 in 3 rows in this feed changes nothing."""
    assert dim.count() < cdc.count(), (
        "every CDC row became a version — hash change-detection is not filtering no-ops"
    )


def test_a_transaction_joins_to_the_then_current_version(dim, cdc):
    """The reason the dimension is effective-dated at all — plan verification item #4, last clause.

    Picks a key with several versions and asserts that an instant inside its second interval resolves
    to exactly one version, and specifically *not* the current one. A dimension that overwrote in
    place would resolve it to today's risk band, which is how a fraud-rate trend becomes an artefact
    of the overwrite.
    """
    counts = dim.groupBy(*KEYS).count().filter("count >= 3").orderBy(*KEYS)
    key = counts.first()["account_id"]
    versions = (
        dim.filter(F.col("account_id") == key)
        .orderBy(scd2.VALID_FROM)
        .select("risk_band", scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT)
        .collect()
    )
    second = versions[1]
    # An instant strictly inside the second interval.
    probe = second[scd2.VALID_FROM]
    resolved = dim.filter(
        (F.col("account_id") == key)
        & (F.col(scd2.VALID_FROM) <= F.lit(probe))
        & (F.col(scd2.VALID_TO) > F.lit(probe))
    ).collect()
    assert len(resolved) == 1, resolved
    assert resolved[0]["risk_band"] == second["risk_band"]
    assert not resolved[0][scd2.IS_CURRENT], "probe resolved to the current version, not the historic one"


# --------------------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------------------

def test_replaying_a_batch_is_a_no_op(dim, cdc):
    """The most important test in the file.

    Not merely "row counts match" — also that nothing was *closed*. A second run that re-closes the
    rows it already closed leaves the same count while corrupting every ``valid_to`` it touches, so
    the intervals are compared directly.

    They are compared against a snapshot **collected before the merge**, not against the ``dim``
    frame. ``read_table`` hands back a lazy reader over a Delta path: re-evaluated after the merge it
    returns the table's new state, so a before/after comparison written the obvious way compares the
    new state against itself and passes no matter what the merge did.
    """
    intervals_before = {
        (r["account_id"], r[scd2.VALID_FROM], r[scd2.VALID_TO], r[scd2.IS_CURRENT])
        for r in dim.select(*KEYS, scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT).collect()
    }
    before = len(intervals_before)
    open_before = sum(1 for i in intervals_before if i[3])
    batch_id = cdc.select("_batch_id").first()["_batch_id"]

    stats = scd2.merge(cdc, table=TABLE, keys=KEYS, effective_from_col=EFFECTIVE,
                       batch_id=batch_id, op_col="_op")

    assert stats.versions_written == 0
    assert stats.keys_closed == 0
    assert stats.keys_deleted == 0
    # Deliberately not `== before`. `rows_skipped_replay` counts rows that reached the replay
    # anti-join and were recognised there; a key whose first replayed row still matches its open
    # version is dropped one step earlier, as a no-op. Both are correct rejections and both are why
    # nothing is written, so asserting that the two counts coincide would pin an implementation
    # detail — and specifically the one that changed when no-op suppression learned to look across
    # the batch boundary.
    assert 0 < stats.rows_skipped_replay <= before

    after = read_table(Layer.SILVER, TABLE)
    intervals_after = {
        (r["account_id"], r[scd2.VALID_FROM], r[scd2.VALID_TO], r[scd2.IS_CURRENT])
        for r in after.select(*KEYS, scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT).collect()
    }
    assert len(intervals_after) == before
    assert sum(1 for i in intervals_after if i[3]) == open_before
    assert intervals_after == intervals_before


# --------------------------------------------------------------------------------------
# Incremental loads and the cases the generator does not produce
# --------------------------------------------------------------------------------------

def _load_in_two_passes(cdc, table, split_ts):
    """Apply the feed as two sequential batches split at ``split_ts``."""
    early = cdc.filter(_effective() < F.lit(split_ts))
    late = cdc.filter(_effective() >= F.lit(split_ts))
    s1 = scd2.merge(early, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
                    batch_id="b1", op_col="_op")
    s2 = scd2.merge(late, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
                    batch_id="b2", op_col="_op")
    return s1, s2


def test_incremental_load_matches_a_single_load(cdc, dim):
    """Two sequential batches must produce the same dimension as one batch containing both.

    This is the test that catches a close-out applied to the wrong row, or a second batch that
    inserts a version without closing the first batch's open one. Path-independence is the property
    that makes a daily pipeline's output equal to a backfill's.
    """
    mid = _midpoint(cdc)
    s1, s2 = _load_in_two_passes(cdc, "dim_account_split", mid)
    assert s2.keys_closed > 0, "the second batch should have closed incumbents from the first"

    one_shot = dim.drop(scd2.BATCH_COL, scd2.UPDATED_COL)
    two_shot = read_table(Layer.SILVER, "dim_account_split").drop(scd2.BATCH_COL, scd2.UPDATED_COL)
    assert two_shot.count() == one_shot.count()
    assert one_shot.exceptAll(two_shot).count() == 0
    assert two_shot.exceptAll(one_shot).count() == 0


def test_a_delete_only_batch_closes_the_incumbent(cdc):
    """A batch carrying nothing but a ``D`` for a key must still close that key.

    The failure mode this guards is subtle and plausible: deletes are excluded from the rows to be
    inserted, so an implementation that derives "which keys to close" from the *insertable* rows sees
    an empty batch and closes nothing. The account then stays current forever, and no count anywhere
    looks wrong.
    """
    table = "dim_account_delonly"
    key = (
        cdc.withColumn("_rn", F.row_number().over(
            Window.partitionBy(*KEYS).orderBy(F.desc(EFFECTIVE))))
        .filter("_rn = 1 AND _op = 'D'")
        .select(*KEYS, EFFECTIVE)
        .first()
    )
    history = cdc.filter((F.col("account_id") == key["account_id"]) & (F.col("_op") != "D"))
    delete_row = cdc.filter(
        (F.col("account_id") == key["account_id"]) & (F.col("_op") == "D")
    ).orderBy(F.desc(EFFECTIVE)).limit(1)

    scd2.merge(history, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
               batch_id="b1", op_col="_op")
    assert read_table(Layer.SILVER, table).filter(F.col(scd2.IS_CURRENT)).count() == 1

    stats = scd2.merge(delete_row, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
                       batch_id="b2", op_col="_op")
    assert stats.versions_written == 0, "a delete must not insert a version"
    assert stats.keys_closed == 1, "a delete-only batch must still close the incumbent"
    assert stats.keys_deleted == 1

    after = read_table(Layer.SILVER, table)
    assert after.filter(F.col(scd2.IS_CURRENT)).count() == 0
    closed = after.orderBy(F.desc(scd2.VALID_FROM)).first()
    assert closed[scd2.VALID_TO] == _ts(key[EFFECTIVE])


def test_out_of_order_cdc_is_refused_not_silently_applied(cdc):
    """A change older than what is already applied must not invert an interval.

    Replaying the *first* batch's changes after the second has landed would write
    ``valid_to <= valid_from`` on the incumbent. The merge refuses those keys and warns. Refusing is
    the right call rather than attempting a mid-chain insert: correctly placing a late-arriving
    version means re-dating its neighbours, and a guess at that is worse than a logged refusal.
    """
    table = "dim_account_ooo"
    mid = _midpoint(cdc)
    early = cdc.filter(_effective() < F.lit(mid))
    late = cdc.filter(_effective() >= F.lit(mid))

    scd2.merge(late, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
               batch_id="b1", op_col="_op")
    before = read_table(Layer.SILVER, table)
    open_before = {
        r["account_id"]: r[scd2.VALID_TO]
        for r in before.filter(F.col(scd2.IS_CURRENT)).select("account_id", scd2.VALID_TO).collect()
    }

    scd2.merge(early, table=table, keys=KEYS, effective_from_col=EFFECTIVE,
               batch_id="b2", op_col="_op")
    after = read_table(Layer.SILVER, table)

    inverted = after.filter(F.col(scd2.VALID_FROM) >= F.col(scd2.VALID_TO))
    assert inverted.count() == 0, inverted.take(5)
    # The incumbents that existed before must still be open and still end at the sentinel.
    still = {
        r["account_id"]: r[scd2.VALID_TO]
        for r in after.filter(F.col(scd2.IS_CURRENT)).select("account_id", scd2.VALID_TO).collect()
    }
    for key, valid_to in open_before.items():
        assert still.get(key) == valid_to, f"{key} incumbent was re-dated by out-of-order CDC"


def test_a_source_rename_does_not_pass_silently(cdc):
    """A missing key column must fail loudly rather than produce an empty or wrong dimension."""
    renamed = cdc.withColumnRenamed("account_id", "accountId")
    with pytest.raises(Exception):
        scd2.merge(renamed, table="dim_account_renamed", keys=KEYS,
                   effective_from_col=EFFECTIVE, batch_id="b1", op_col="_op")
