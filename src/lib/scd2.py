"""Slowly-changing dimension, type 2 — generic over any entity.

Why SCD2 at all, in this domain
-------------------------------
An account's ``risk_band`` moves and a merchant's ``risk_score`` moves. If the dimension only holds
today's value, then last March's fraud rate is computed against today's risk segmentation, and the
trend it produces is an artefact of the overwrite rather than anything that happened. Attributing a
transaction to the version of the account that was current *when it was authorised* is the whole
point, and it is why the fact-to-dimension join in gold is effective-dated rather than a plain key
lookup.

The part most implementations get wrong
---------------------------------------
A single ``MERGE`` handles one change per key per batch. The ``accounts`` CDC feed emits up to five
changes for the same account in one load, and a merge that collapses those to "the latest one"
silently destroys the intermediate history — while still producing a dimension that passes every
obvious test, because one current row per key is exactly what you would check for.

So the batch is **versioned before it is merged**: within each key, rows are ordered by their
effective timestamp, consecutive no-op changes are dropped by hash, and ``valid_from`` /
``valid_to`` are computed with a window. The merge then only has to close the incumbent row and
insert an already-correct chain.

Interval convention
-------------------
Half-open: ``valid_from <= t < valid_to``. Contiguity is then exact — each version's ``valid_to``
*is* the next version's ``valid_from``, so "no gaps and no overlaps" is true by construction rather
than by rounding. Open versions carry the sentinel ``9999-12-31`` instead of ``NULL``, which keeps
the gold join a plain ``BETWEEN``-style predicate: on Fabric Warehouse a ``NULL``-tolerant join
condition is the difference between a merge join and a nested loop over the fact table.

Deletes
-------
``_op = 'D'`` closes the current version and inserts no successor. It never deletes a row. A fact
that references a since-deleted account still resolves, because it joins to the version that was
current at authorisation time — which is the reason a logical close is correct here and a physical
delete would quietly break history.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.runtime.context import Layer, delta_table, read_table, table_exists, write_table

log = logging.getLogger(__name__)

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"
HASH_COL = "_scd_hash"
BATCH_COL = "_batch_id"
UPDATED_COL = "_updated_ts"

SCD_COLUMNS = (VALID_FROM, VALID_TO, IS_CURRENT, HASH_COL, BATCH_COL, UPDATED_COL)

# Sentinel for "still open". See the interval convention above.
HIGH_DATE = datetime(9999, 12, 31, 23, 59, 59)

# Bronze's audit columns describe *arrival*, not the entity. They are excluded from the dimension
# and from the hash for the same reason bronze's own `_row_hash` excludes them: an unchanged
# customer re-arriving in next month's snapshot must not register as a change.
BRONZE_AUDIT = ("_batch_id", "_ingest_ts", "_source_file", "_row_hash", "ingest_date")


@dataclass
class Scd2Stats:
    table: str
    rows_in: int
    versions_written: int
    keys_closed: int
    keys_deleted: int
    rows_skipped_replay: int

    def __str__(self) -> str:
        return (
            f"{self.table}: {self.rows_in} in -> {self.versions_written} version(s), "
            f"{self.keys_closed} closed, {self.keys_deleted} logically deleted, "
            f"{self.rows_skipped_replay} replayed row(s) skipped"
        )


def payload_columns(df: DataFrame, keys: list[str], effective_from_col: str,
                    op_col: str | None) -> list[str]:
    """Business columns to carry into the dimension.

    ``effective_from_col`` and ``op_col`` are dropped: the first becomes ``valid_from`` and keeping
    both would let them disagree, and the second is CDC transport, not an attribute of the account.
    """
    drop = set(BRONZE_AUDIT) | {effective_from_col}
    if op_col:
        drop.add(op_col)
    return [c for c in df.columns if c not in drop]


def _hash(cols: list[str]):
    """Change-detection hash over the attribute values.

    ``coalesce`` to a sentinel rather than letting a null poison the hash, and cast to string so a
    type widening in silver does not manufacture a change across every row. The sentinel is a
    control character precisely because it cannot occur in the data.
    """
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in sorted(cols)])


# --------------------------------------------------------------------------------------
# Versioning the incoming batch
# --------------------------------------------------------------------------------------

def build_versions(
    updates: DataFrame,
    keys: list[str],
    effective_from_col: str,
    op_col: str | None = None,
    delete_op: str = "D",
    prior_hashes: DataFrame | None = None,
) -> DataFrame:
    """Turn a batch of changes into a contiguous version chain per key.

    Three things happen here, in order, and the order matters:

    1. **Collapse simultaneous rows.** Two changes with the same key and the same effective
       timestamp have no defensible ordering, so the last one wins deterministically (ties broken
       by hash, not by row order, so a reshuffle cannot change the answer).
    2. **Drop no-op changes.** A CDC row whose attributes are identical to the previous version is
       an update in the source's bookkeeping, not in the data. Keeping it would produce two
       adjacent versions that are indistinguishable except for their timestamps — history that
       never happened. Deletes are exempt: a delete is always a real event.

       "The previous version" has to mean the previous version *in the dimension*, not merely the
       previous row in this batch, which is what ``prior_hashes`` supplies. Without it the first row
       for a key in each batch has no predecessor to compare against and is always taken as a change,
       so a daily load whose first row for an account repeats yesterday's state writes a version that
       changes nothing and closes the incumbent to make room for it. Nothing about the result looks
       wrong — one current row per key, no overlaps, no gaps — but the dimension now records a change
       on a day when the source reported no change, and a two-batch load no longer equals a one-batch
       load of the same feed. With ~1 in 3 rows in this feed a no-op, that is not an edge case.
    3. **Date the intervals.** ``valid_to`` is the next version's ``valid_from``; the final version
       is open, unless it is a delete, in which case it closes at its own timestamp and nothing
       succeeds it.

    ``prior_hashes`` is ``keys + _scd_hash`` for the versions currently open in the target. Omit it
    for a first load, where by definition there is nothing to compare against.
    """
    attrs = [c for c in payload_columns(updates, keys, effective_from_col, op_col) if c not in keys]
    is_delete = (F.col(op_col) == delete_op) if op_col else F.lit(False)

    staged = (
        updates.withColumn(HASH_COL, _hash(attrs))
        .withColumn("__is_delete", is_delete)
        .withColumn(VALID_FROM, F.col(effective_from_col).cast("timestamp"))
    )

    # (1) one row per (key, instant)
    dedupe = Window.partitionBy(*keys, VALID_FROM).orderBy(F.col(HASH_COL).desc())
    staged = (
        staged.withColumn("__rn", F.row_number().over(dedupe))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )

    # (2) drop changes that changed nothing, comparing across the batch boundary
    ordered = Window.partitionBy(*keys).orderBy(VALID_FROM)
    if prior_hashes is not None:
        staged = staged.join(
            prior_hashes.select(*keys, F.col(HASH_COL).alias("__prior_hash")),
            on=keys, how="left",
        )
    else:
        staged = staged.withColumn("__prior_hash", F.lit(None).cast("long"))
    staged = (
        # A key's first row in this batch falls back to the state already open in the dimension.
        staged.withColumn(
            "__prev_hash", F.coalesce(F.lag(HASH_COL).over(ordered), F.col("__prior_hash"))
        )
        .filter(
            F.col("__is_delete")
            | F.col("__prev_hash").isNull()
            | (F.col("__prev_hash") != F.col(HASH_COL))
        )
        .drop("__prev_hash", "__prior_hash")
    )

    # (3) date the chain
    staged = staged.withColumn("__next_from", F.lead(VALID_FROM).over(ordered))
    return (
        staged.withColumn(
            VALID_TO,
            F.when(F.col("__next_from").isNotNull(), F.col("__next_from"))
            .when(F.col("__is_delete"), F.col(VALID_FROM))
            .otherwise(F.lit(HIGH_DATE)),
        )
        .withColumn(
            IS_CURRENT,
            F.col("__next_from").isNull() & ~F.col("__is_delete"),
        )
        .drop("__next_from")
    )


# --------------------------------------------------------------------------------------
# Merge
# --------------------------------------------------------------------------------------

def merge(
    updates: DataFrame,
    table: str,
    keys: list[str],
    effective_from_col: str,
    batch_id: str,
    op_col: str | None = None,
    delete_op: str = "D",
    layer: Layer = Layer.SILVER,
) -> Scd2Stats:
    """Apply a batch of changes to an SCD2 dimension.

    Idempotent: a replayed batch inserts no versions and closes nothing, so rerunning a completed
    load is a genuine no-op rather than a second history. This is the same property bronze gets from
    ``_batch_id``, restated for a layer where the unit of replacement is a version rather than a file.

    Two guards give it that property, and they are separate because deletes are not symmetric with
    changes. Inserts are guarded by an anti-join on ``(key, valid_from)`` — a version already recorded
    at that instant is a rerun. Deletes cannot be guarded that way, because a delete is never
    *inserted*; it is guarded by requiring the target to still have an open row older than the delete,
    which a replay no longer finds.
    """
    rows_in = updates.count()
    exists = table_exists(layer, table)
    target = read_table(layer, table) if exists else None
    # Seeds no-op suppression with what is already open, so the first row of a batch is judged
    # against the dimension rather than against nothing. See build_versions step (2).
    prior_hashes = (
        target.filter(F.col(IS_CURRENT)).select(*keys, HASH_COL) if target is not None else None
    )
    versions = build_versions(
        updates, keys, effective_from_col, op_col, delete_op, prior_hashes=prior_hashes
    ).cache()
    payload = payload_columns(updates, keys, effective_from_col, op_col)
    version_cols = [*payload, HASH_COL, VALID_FROM, VALID_TO, IS_CURRENT]

    final = (
        versions.select(*version_cols, "__is_delete")
        .withColumn(BATCH_COL, F.lit(batch_id))
        .withColumn(UPDATED_COL, F.current_timestamp())
    )
    insert_cols = [*version_cols, BATCH_COL, UPDATED_COL]

    # A delete is a close-out, never a row in the dimension. Inserting one would put a version in the
    # table whose attributes describe an entity that no longer exists — and because `lead` already
    # dated the predecessor to end at the delete's timestamp, the close has happened without it.
    # Where a delete is followed by later activity for the same key, the result is a genuine *gap* in
    # the chain: the entity did not exist in that interval, and a dimension that filled it would be
    # asserting otherwise. So the contiguity invariant is "no overlaps, and no gaps except where a
    # delete closed the chain" — not "no gaps", which would be a weaker statement dressed up as a
    # stronger one.
    to_insert = final.filter(~F.col("__is_delete")).drop("__is_delete")
    deletes = final.filter("__is_delete").select(*keys, VALID_FROM)

    if not exists:
        write_table(to_insert, layer, table, mode="overwrite")
        stats = Scd2Stats(
            table, rows_in, to_insert.count(), 0, deletes.select(*keys).distinct().count(), 0
        )
        log.info("scd2 %s (first load)", stats)
        versions.unpersist()
        return stats

    new_versions = (
        to_insert.join(
            target.select(*keys, VALID_FROM).withColumn("__seen", F.lit(True)),
            on=[*keys, VALID_FROM], how="left",
        )
        .filter(F.col("__seen").isNull())
        .drop("__seen")
        .cache()
    )
    n_new = new_versions.count()
    skipped = to_insert.count() - n_new

    # A delete that has already been applied leaves a version ending exactly at its instant, so an
    # anti-join on `valid_to` is the delete-side equivalent of the `valid_from` replay guard above.
    # It has to be a separate guard because a delete is never inserted, so the insert guard cannot
    # see it — the asymmetry the docstring describes.
    closed_instants = target.select(*keys, VALID_TO).distinct().alias("c")
    new_deletes = (
        deletes.alias("d")
        .join(
            closed_instants,
            on=[
                *[F.col(f"d.{k}") == F.col(f"c.{k}") for k in keys],
                F.col(f"d.{VALID_FROM}") == F.col(f"c.{VALID_TO}"),
            ],
            how="left_anti",
        )
        .select(*keys, VALID_FROM)
        .cache()
    )
    keys_deleted = new_deletes.select(*keys).distinct().count()

    # Which keys need their incumbent closed, and at what instant. Derived from the batch's *unapplied*
    # work — new versions plus not-yet-applied deletes — rather than from every row it carries.
    # Deletes must be in scope at all because a batch carrying nothing but a `D` for a key must still
    # close that key's open row; that is the whole point of a logical delete. But restricting to
    # unapplied work is what keeps a plain rerun quiet: derived from every row, a replay's earliest
    # change for each key predates the incumbent it already produced, and every key in the feed would
    # be reported as out-of-order CDC on a load that did nothing at all. A warning that fires on every
    # rerun is a warning nobody reads the day it is true.
    close_at = (
        new_versions.select(*keys, VALID_FROM)
        .unionByName(new_deletes)
        .groupBy(*keys)
        .agg(F.min(VALID_FROM).alias("__close_at"))
        .join(
            target.filter(F.col(IS_CURRENT)).select(*keys, F.col(VALID_FROM).alias("__cur_from")),
            on=keys, how="inner",
        )
    )
    # An incumbent that already starts at or after the close instant means CDC arrived out of order:
    # a change older than what has already been applied. Closing on it would write valid_to <=
    # valid_from and produce an inverted interval, so it is refused — and logged, because silently
    # dropping out-of-order history is how a dimension becomes quietly wrong.
    out_of_order = close_at.filter(F.col("__cur_from") >= F.col("__close_at"))
    n_out_of_order = out_of_order.count()
    if n_out_of_order:
        log.warning(
            "scd2 %s: %d key(s) had an open version at or after the incoming change instant — "
            "out-of-order CDC, not closed. Investigate the source's ordering guarantee.",
            table, n_out_of_order,
        )
    closers = close_at.filter(F.col("__cur_from") < F.col("__close_at")).drop("__cur_from").cache()
    n_close = closers.count()

    if n_new == 0 and n_close == 0:
        stats = Scd2Stats(table, rows_in, 0, 0, 0, skipped)
        log.info("scd2 %s (no-op)", stats)
        for d in (versions, new_versions, closers, new_deletes):
            d.unpersist()
        return stats

    # Delta cannot both update an existing row and insert a new one from the same source row, so the
    # source is unioned with itself: closers carry the merge keys and match, version rows carry nulls
    # in those keys and therefore never match. Per-key merge columns rather than one concatenated key
    # because a separator that can occur in the data is a collision waiting to be someone's incident.
    mk = {k: f"__mk_{k}" for k in keys}
    types = {c: to_insert.schema[c].dataType for c in insert_cols}
    source_cols = [*mk.values(), *insert_cols, "__close_at"]

    closers_src = closers.select(
        *[F.col(k).alias(mk[k]) for k in keys],
        *[F.lit(None).cast(types[c]).alias(c) for c in insert_cols],
        F.col("__close_at"),
    ).select(*source_cols)
    inserters_src = new_versions.select(
        *[F.lit(None).cast(types[k]).alias(mk[k]) for k in keys],
        *insert_cols,
        F.lit(None).cast("timestamp").alias("__close_at"),
    ).select(*source_cols)

    source = closers_src.unionByName(inserters_src)
    condition = " AND ".join(f"t.{k} = s.{mk[k]}" for k in keys) + f" AND t.{IS_CURRENT}"

    (
        delta_table(layer, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdate(set={
            VALID_TO: "s.__close_at",
            IS_CURRENT: F.lit(False),
            UPDATED_COL: F.current_timestamp(),
        })
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in insert_cols})
        .execute()
    )

    stats = Scd2Stats(table, rows_in, n_new, n_close, keys_deleted, skipped)
    log.info("scd2 %s", stats)
    for d in (versions, new_versions, closers, new_deletes):
        d.unpersist()
    return stats
