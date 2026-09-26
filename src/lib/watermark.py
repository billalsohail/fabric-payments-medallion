"""Watermark state for incremental loads.

One row per **(entity, layer)** in `meta_watermark`, holding the highest `ingest_date` successfully
processed by that layer. Stored as a **string**, not a date or a timestamp: the same table has to be
readable from a Data Factory pipeline expression, where everything is a string, and a watermark that
only works from Spark is not usable by the orchestrator that is supposed to drive it.

Why `layer` is a column and not part of the key string. Bronze and silver both advance a watermark
for the same entity and they advance at different rates — silver can be a day behind bronze while a
DQ breach is investigated, and on a `--entities` partial run it routinely is. A single row per entity
would let whichever layer ran last overwrite the other's position, which shows up as silver silently
skipping a day. The obvious alternative, keying on `f"silver:{entity}"`, is the concatenated
composite key that `src/lib/scd2.py` refuses by name: a separator that can occur in the data is a
collision waiting to be someone's incident, and an entity called `silver:x` is not impossible, just
unlikely. So it is a real column, the merge matches on both, and the table stays partitioned by
`entity` alone — two rows per entity does not justify a second partition level.

The watermark is advanced only after a successful write. A failed run leaves it untouched, so the
next attempt re-reads the same window — which is safe precisely because bronze deletes and
reinserts by `_batch_id` rather than blindly appending. The two mechanisms answer different
questions: the watermark decides *what to read*, the batch id decides *what a retry replaces*.
"""
from __future__ import annotations

import logging
import time

from pyspark.sql import functions as F

from src.runtime.context import Layer, delta_table, get_spark, read_table, table_exists

log = logging.getLogger(__name__)

TABLE = "meta_watermark"

# Matched by class name rather than by import: `delta.exceptions` has moved between Delta versions,
# and a pinned import that is wrong on the Fabric runtime would turn a retryable conflict into a
# crash. The names themselves are stable across 2.x and 3.x.
_CONCURRENCY_ERRORS = frozenset({
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
})


BRONZE = "bronze"
SILVER = "silver"


def get(entity: str, layer: str = BRONZE) -> str | None:
    """Current watermark for an entity in one layer, or None if that layer has never loaded it."""
    if not table_exists(Layer.META, TABLE):
        return None
    row = (
        read_table(Layer.META, TABLE)
        .filter((F.col("entity") == entity) & (F.col("layer") == layer))
        .select("watermark_value")
        .first()
    )
    return row["watermark_value"] if row else None


MERGE_ATTEMPTS = 5
MERGE_BACKOFF_SEC = 0.4


def set(entity: str, value: str, batch_id: str, layer: str = BRONZE) -> None:  # noqa: A001 - mirrors get/set pairing
    """Advance the watermark. Call only after the corresponding write has committed.

    Concurrency, which this table gets more of than its seven rows suggest: every entity in a wave
    MERGEs here at the same time. Two things make that safe.

    1. **A literal partition predicate.** `meta_watermark` is partitioned by `entity`, and the merge
       condition repeats the entity as a literal alongside the join. `t.entity = s.entity` on its own
       is not enough — the value lives on the source side, so Delta cannot prune partitions at plan
       time and conservatively treats every file as read, which is precisely what turns two
       unrelated writers into a `ConcurrentAppendException`.
    2. **A bounded retry.** Optimistic concurrency means a conflict is a normal outcome, not an
       error; the correct response is to re-read and retry. Retrying is safe because the write is an
       idempotent upsert of one row — replaying it converges on the same state.

    Without (1), (2) alone would still eventually succeed but would serialise every wave through
    contention. Without (2), (1) alone leaves a narrow race on first insert into a partition that
    does not exist yet.
    """
    spark = get_spark()
    updates = spark.createDataFrame(
        [(entity, layer, value, batch_id)],
        "entity string, layer string, watermark_value string, last_batch_id string",
    ).withColumn("updated_ts", F.current_timestamp())
    condition = (
        f"t.entity = s.entity AND t.layer = s.layer AND t.entity = '{entity}'"
    )

    for attempt in range(1, MERGE_ATTEMPTS + 1):
        try:
            (
                delta_table(Layer.META, TABLE)
                .alias("t")
                .merge(updates.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            break
        except Exception as exc:  # noqa: BLE001 — narrowed by name below
            if type(exc).__name__ not in _CONCURRENCY_ERRORS or attempt == MERGE_ATTEMPTS:
                raise
            wait = MERGE_BACKOFF_SEC * attempt
            log.warning("watermark merge for %s hit %s (attempt %s/%s) — retrying in %.1fs",
                        entity, type(exc).__name__, attempt, MERGE_ATTEMPTS, wait)
            time.sleep(wait)
    log.info("watermark %s/%s -> %s (batch %s)", layer, entity, value, batch_id)


def reset(entity: str | None = None, layer: str | None = None) -> None:
    """Clear watermarks so the next run performs a full reload.

    Deliberately explicit and deliberately loud: an accidental watermark reset on a high-volume
    feed is an expensive mistake, and it is the kind that looks like success.

    Omitting `layer` clears every layer's position for the entity, which is the right default for a
    reset: resetting bronze without resetting silver would leave silver refusing to re-read rows
    bronze is about to re-ingest.
    """
    if not table_exists(Layer.META, TABLE):
        return
    clauses = []
    if entity is not None:
        clauses.append(f"entity = '{entity}'")
    if layer is not None:
        clauses.append(f"layer = '{layer}'")
    predicate = " AND ".join(clauses) if clauses else "true"
    log.warning("resetting watermark for %s (%s) — the next run will reload from the beginning",
                entity or "ALL ENTITIES", layer or "all layers")
    delta_table(Layer.META, TABLE).delete(predicate)
