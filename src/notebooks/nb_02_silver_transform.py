# %% [markdown]
# # nb_02 — Silver transform
#
# **One** notebook, all seven feeds, no per-entity branch — same claim as bronze, and a harder one to
# keep here, because silver is where the feeds actually differ. Every difference is a column in
# `meta_source_config`: `scd_type` chooses the write, `load_type` chooses the batching,
# `effective_from_column` chooses the ordering, `reprocess_window_days` chooses the scope.
#
# On Fabric this is a Notebook activity in the same `ForEach` as bronze, one wave later.
#
# ## What silver is
#
# Bronze is what arrived. Silver is **what is true, typed, deduplicated, and quality-gated** — the
# layer gold is allowed to trust. Five steps, and the order of them is the design:
#
# ```
#   scope  →  type gate  →  cast  →  dedupe  →  DQ gate  →  write
# ```
#
# **Type gate before cast.** `cast` in Spark is silent: `cast('2026-13-45' as date)` is `NULL`, not an
# error. A silver notebook that reads bronze, casts, and writes therefore converts every malformed
# source value into a well-typed null and reports a clean run — which is exactly the outcome bronze
# refuses to commit, landing one layer later. nb_01's header promises that "casting belongs in silver,
# where a failure can be quarantined with the rule that caught it"; `src/lib/contracts.py` is where
# that promise is kept, and it has to run *before* the cast, because after the cast the evidence is
# gone.
#
# **Dedupe before the DQ gate.** The `unique` rules in `meta_dq_rules` say "asserted after dedupe" in
# their own descriptions, and they have to: the feed contains 0.3% exact duplicate transactions by
# design, and the 90-day dispute window deliberately re-reads partitions in which the same
# `dispute_id` reappears with a changed status. Run the gate first and both are quarantined as
# uniqueness breaches — valid data rejected because the pipeline checked in the wrong order.
#
# **Cast before dedupe.** Duplicates are resolved by keeping the newest version of a key, and "newest"
# is a comparison on `effective_from_column`. On a string that comparison is lexical, which is
# accidentally correct for ISO-8601 and wrong for everything else; there is no reason to depend on it
# once the type gate has already made the cast safe.
#
# ## Batching: why a snapshot feed is loaded one snapshot at a time
#
# Bronze loads a `full_snapshot` feed as *the newest snapshot only* — deliberate, and covered by a
# test. Silver cannot inherit that, because across monthly runs bronze accumulates several snapshots
# and SCD2's whole purpose is the transitions between them. So silver processes a `full_snapshot`
# SCD2 feed **one snapshot per batch, in ascending order**: a loop of one on a cold start, and the
# correct history when there is more than one.
#
# The alternative — hand `scd2.merge` every snapshot at once — is wrong in two independent ways. The
# `unique` rule on `customer_id` is a per-snapshot statement, and eighteen snapshots of 50k customers
# would breach it 18x over. And the batch id, which is what makes a replay a no-op, would cover a
# window rather than a snapshot.
#
# CDC feeds go through as a single batch, because `scd2.build_versions` already versions multiple
# changes per key *within* a batch — that is the thing it exists for. The config field that decides
# between the two is `load_type`, and the justification is one line: a snapshot **is** a batch
# boundary, by definition.

# %%
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.lib import config, contracts, dq, run_log, scd2, watermark
from src.runtime import params
from src.runtime.context import (
    Layer,
    delta_table,
    read_table,
    table_exists,
    write_table,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nb_02")

# %% tags=["parameters"]
PARAMS = params.resolve({
    "entity": "",
    # Inclusive upper bound on `ingest_date`. Empty means "everything bronze has". This is the
    # backfill handle: stepping it forward month by month replays a snapshot feed's history in
    # order, which is how a cold-started lake acquires SCD2 transitions it could not have on day one.
    "until_date": "",
    "run_id": "",
    # Ignore the silver watermark and reprocess every partition bronze holds. Loud and logged.
    "force_reload": False,
})

ENTITY: str = PARAMS["entity"]

BRONZE_PREFIX = "br_"
SILVER_BATCH_COL = "_silver_batch_id"
SILVER_TS_COL = "_silver_ts"

# %% [markdown]
# ## Scope — which bronze partitions this run is responsible for
#
# Silver keeps its **own** watermark, in the same table as bronze's but on its own `layer` row. They
# advance at different rates: a DQ breach stops silver while bronze keeps landing files, and a
# `--entities` partial run does it routinely. One shared row per entity would let whichever layer ran
# last overwrite the other's position, and the visible symptom would be silver skipping a day.

# %%
def select_scope(cfg: dict, until_date: str = "", force_reload: bool = False) -> tuple[list[str], str | None]:
    """Bronze `ingest_date` partitions this run should process, ascending, and the watermark used.

    `static` feeds ignore the watermark entirely: a reference table that "rarely changes" is
    overwritten from whatever bronze currently holds, and a watermark would make the overwrite depend
    on load order.
    """
    entity = cfg["entity"]
    table = f"{BRONZE_PREFIX}{entity}"
    if not table_exists(Layer.BRONZE, table):
        return [], None

    # Formatted to a string **on the JVM side**, with `date_format`, rather than collected as a date
    # and converted in Python. Two reasons, and the second is the interesting one.
    #
    # Bronze types `ingest_date` as a `date` on purpose (nb_01's `read_landing`: an inferred type is a
    # type nobody chose), while a watermark is a string, because a Data Factory `Lookup` hands
    # pipeline expressions strings. Something has to convert, and doing it once at the boundary beats
    # leaving every comparison downstream to guess which side it is on.
    #
    # The obvious way to convert — `str(row["ingest_date"])` on the collected value — worked in a
    # single-threaded process and produced `java.util.GregorianCalendar[...]` under the orchestrator's
    # thread pool, which then went into `meta_watermark` and made the *next* run unparseable.
    # Reproduced and confirmed: green at `--parallelism 1`, broken at 4. The collected value came back
    # as a py4j handle to the JVM object instead of a `datetime.date`, so `str()` returned Java's
    # `toString()`. Whatever the mechanism inside py4j, the lesson generalises past it: a value that
    # has to be a string has no business being converted client-side when Spark can produce the string
    # itself. This is also the failure mode that argues for the run-twice test in CI — the first run of
    # this pipeline was clean, and the bug lived entirely in the second.
    available = sorted(
        r["p"]
        for r in read_table(Layer.BRONZE, table)
        .select(F.date_format("ingest_date", "yyyy-MM-dd").alias("p"))
        .distinct()
        .collect()
    )
    if until_date:
        available = [p for p in available if p <= until_date]
    if cfg["load_type"] == "static":
        return available, None

    wm = None if force_reload else watermark.get(entity, layer=watermark.SILVER)
    if wm is None:
        return available, None

    floor = wm
    window_days = cfg.get("reprocess_window_days")
    if window_days:
        # The rolling window, and the reason it is `>=` rather than `>`: a dispute that was
        # quarantined last week — or recorded as `not_evaluated` because its transaction had not yet
        # reached silver — is sitting in a partition at or below the watermark. Only re-reading it can
        # let it in. Absorbing status changes on already-loaded disputes is the same mechanism.
        floor = (date.fromisoformat(wm) - timedelta(days=int(window_days))).isoformat()
        return [p for p in available if p >= floor], wm
    return [p for p in available if p > floor], wm


def _batches(cfg: dict, partitions: list[str]) -> list[list[str]]:
    """Group the scope into the units silver merges one at a time.

    One batch per snapshot for SCD2 snapshot feeds (see the header); one batch for everything else.
    """
    if cfg["scd_type"] == 2 and cfg["load_type"] == "full_snapshot":
        return [[p] for p in partitions]
    return [partitions]


def batch_id_for(entity: str, partitions: list[str]) -> str:
    """Deterministic, like bronze's — a replayed window produces the same id and therefore the same
    idempotent write.

    The high date is deliberately the **last** `|` segment: `dq._as_of_from_batch_id` recovers the
    as-of date for the `freshness` rule from exactly there, so a batch id that put anything after it
    would silently disable freshness checking rather than fail.
    """
    return f"{entity}|silver|{partitions[0]}|{partitions[-1]}"


# %% [markdown]
# ## Dedupe
#
# The grain is `merge_keys` — except for a CDC feed feeding an SCD2 dimension, where it is
# `merge_keys + effective_from_column`. That exception is the whole difference between a dimension
# with history and a dimension without one: in `accounts`, `account_id` identifies an *account*, not a
# row, and the feed emits up to five changes for one account in a single load. Deduplicating on the
# merge key alone would keep the newest and silently discard the history SCD2 exists to record —
# producing a dimension that passes every obvious test, because one current row per key is exactly
# what a reviewer would check.
#
# For a snapshot feed the merge key *is* the grain, because silver processes one snapshot per batch.
# For `disputes` it is the grain on purpose: the 90-day window re-reads partitions where the same
# `dispute_id` reappears with a changed status, and keeping the newest is how that status change is
# absorbed.

# %%
def dedupe_grain(cfg: dict) -> list[str]:
    keys = list(cfg["merge_keys"])
    is_cdc_dimension = cfg["scd_type"] == 2 and cfg["load_type"] != "full_snapshot"
    if is_cdc_dimension and cfg["effective_from_column"]:
        keys.append(cfg["effective_from_column"])
    return keys


def dedupe(df: DataFrame, cfg: dict) -> tuple[DataFrame, int]:
    """Keep one row per grain — the newest by `effective_from_column`, then by bronze's `_row_hash`.

    The hash tiebreak is not cosmetic. Two rows that are identical on the grain *and* on the ordering
    column have no defensible ordering, so without a deterministic tiebreak the survivor depends on
    partitioning and a rerun of the same input can produce a different silver row. Ordering by a
    value, not by row position, is what makes the load reproducible.
    """
    grain = dedupe_grain(cfg)
    missing = [k for k in grain if k not in df.columns]
    if missing:
        raise ValueError(
            f"{cfg['entity']}: dedupe grain column(s) {missing} not in bronze. Available: "
            f"{sorted(df.columns)}. A source rename must fail here rather than change the grain."
        )
    order = cfg["effective_from_column"] or "_ingest_ts"
    window = Window.partitionBy(*grain).orderBy(F.col(order).desc(), F.col("_row_hash").desc())
    ranked = df.withColumn("__rn", F.row_number().over(window)).cache()
    kept = ranked.filter("__rn = 1").drop("__rn")
    rows_deduped = ranked.filter("__rn > 1").count()
    kept = kept.localCheckpoint(eager=True) if rows_deduped else kept
    ranked.unpersist()
    return kept, rows_deduped


# %% [markdown]
# ## Writes — three shapes, chosen by `scd_type`
#
# | `scd_type` | Write | Feeds |
# |---|---|---|
# | `2` | `scd2.merge` — version chain, logical deletes | `dim_account`, `dim_customer`, `dim_merchant` |
# | `0` | MERGE on `merge_keys`, update only when `_row_hash` differs | `fact_transaction`, `fact_dispute`, `dim_fx_rate` |
# | `1` | Overwrite | `dim_card_product` |
#
# The `_row_hash` guard on the `scd_type = 0` merge is what makes a replay free rather than merely
# harmless: bronze already computed a hash over the business columns, so an unchanged row matches,
# compares equal, and is not rewritten. Without the guard a rerun would touch every row in the
# table — rewriting files, invalidating statistics, and overwriting `_silver_ts` so the lineage says
# every row changed today.
#
# Silver is written **unpartitioned**, `fact_transaction` included. See `docs/design-decisions.md`
# #10: at this volume partitioning buys pruning that is worth less than the small-file pressure it
# creates, and the honest answer to "what would you change at 100x" is better than a partition scheme
# chosen to look thorough.

# %%
def _with_silver_audit(df: DataFrame, batch_id: str) -> DataFrame:
    return df.withColumn(SILVER_BATCH_COL, F.lit(batch_id)).withColumn(
        SILVER_TS_COL, F.current_timestamp()
    )


def write_silver(df: DataFrame, cfg: dict, batch_id: str) -> int:
    table = cfg["target_table"]
    keys = list(cfg["merge_keys"])
    scd_type = cfg["scd_type"]

    if scd_type == 2:
        # No silver audit columns here, and that is not an oversight: `scd2` derives its
        # change-detection hash from the payload columns, so a per-run timestamp in the payload would
        # make every row look changed on every run and manufacture a version a day. `scd2` writes its
        # own `_batch_id` and `_updated_ts` outside the hash for exactly this reason.
        stats = scd2.merge(
            df, table=table, keys=keys,
            effective_from_col=cfg["effective_from_column"],
            batch_id=batch_id, op_col=cfg["op_column"],
        )
        return stats.versions_written

    out = _with_silver_audit(df, batch_id)
    rows = out.count()

    if scd_type == 1:
        # Overwrite is the *whole* semantic of SCD1 here: `card_products` is a static reference feed,
        # and "the current list of card products" is the only thing the table is meant to assert.
        write_table(out, Layer.SILVER, table, mode="overwrite", merge_schema=True)
        return rows

    if scd_type != 0:
        raise ValueError(
            f"{cfg['entity']}: scd_type {scd_type!r} is not supported. Expected 0, 1 or 2."
        )

    if not table_exists(Layer.SILVER, table):
        write_table(out, Layer.SILVER, table, mode="overwrite", merge_schema=True)
        return rows

    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    handle = delta_table(Layer.SILVER, table)
    (
        handle.alias("t")
        .merge(out.alias("s"), condition)
        .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _rows_affected(handle, fallback=rows)


def _rows_affected(handle, fallback: int) -> int:
    """Rows the MERGE actually inserted or updated, read back from the Delta commit.

    Returning the source row count instead would be easier and would be a lie on every rerun: the
    `_row_hash` guard means a replay matches every row and changes none, so a notebook that reported
    its input size would log 49,708 rows written for a load that wrote nothing — and `meta_run_log`
    would then agree with itself about a pipeline that had stopped being idempotent. The number in the
    run log has to be able to say "nothing happened", or it cannot be used to check that something did.

    Falls back to the source count if the metrics are absent. Delta has reported these since 1.x, but a
    missing metric should degrade the reporting, not fail the load.
    """
    try:
        metrics = handle.history(1).select("operationMetrics").first()[0] or {}
    except Exception:  # noqa: BLE001 — telemetry must never be able to fail a load
        log.warning("could not read Delta operation metrics; reporting source row count")
        return fallback
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    updated = int(metrics.get("numTargetRowsUpdated", 0))
    return inserted + updated


# %% [markdown]
# ## Entry point

# %%
def transform(entity: str, run_id: str = "", until_date: str = "",
              force_reload: bool = False) -> dict:
    cfg = load_config(entity)
    config.require_enabled(cfg)

    run_id = run_id or f"local-{int(time.time())}"
    partitions, wm = select_scope(cfg, until_date, force_reload)

    if not partitions:
        reason = (
            f"no bronze partitions for {entity}"
            if wm is None
            else f"silver watermark {wm} is current"
        )
        run_log.skipped(run_id, "", entity, Layer.SILVER, "silver_transform", reason)
        return dict(entity=entity, status="skipped", reason=reason, rows=0)

    batches = _batches(cfg, partitions)
    log.info(
        "%s: %s partition(s) %s..%s in %s batch(es)  scd_type=%s load_type=%s watermark=%s",
        entity, len(partitions), partitions[0], partitions[-1], len(batches),
        cfg["scd_type"], cfg["load_type"], wm,
    )

    bronze = read_table(Layer.BRONZE, f"{BRONZE_PREFIX}{entity}")
    totals = dict(rows_read=0, rows_written=0, rows_quarantined=0, rows_deduped=0)
    last_batch = ""

    for window in batches:
        batch_id = batch_id_for(entity, window)
        last_batch = batch_id
        scoped = bronze.filter(F.col("ingest_date").cast("string").isin(*window))

        with run_log.step(run_id, batch_id, entity, Layer.SILVER, "silver_transform") as metrics:
            typed, cast_outcome = contracts.enforce(scoped, entity, run_id, batch_id)
            # A type contract breach is fatal on the same terms as any other rule: `warn` severity
            # with a rate threshold, so one unparseable timestamp is a quarantined row and a fifth of
            # the column failing is a feed incident. The check happens here rather than inside the
            # gate because deciding whether a run dies is the caller's business, not a library's.
            cast_outcome.raise_if_failed()

            deduped, rows_deduped = dedupe(typed, cfg)
            gate = dq.apply(deduped, entity, cfg["dq_rule_set"], run_id, batch_id)
            gate.raise_if_failed()

            rows_written = write_silver(gate.clean, cfg, batch_id)

            metrics.rows_read = cast_outcome.rows_in
            metrics.rows_written = rows_written
            metrics.rows_quarantined = cast_outcome.rows_quarantined + gate.rows_quarantined
            metrics.rows_deduped = rows_deduped

        totals["rows_read"] += cast_outcome.rows_in
        totals["rows_written"] += rows_written
        totals["rows_quarantined"] += metrics.rows_quarantined
        totals["rows_deduped"] += rows_deduped

    if cfg["load_type"] != "static":
        # Advanced once, after every batch has committed. Advancing per batch would leave a partial
        # history behind if batch three of eighteen failed — and worse, it would leave it looking
        # complete. The watermark is the high-water mark of *processed* partitions, not of attempted
        # ones.
        watermark.set(entity, partitions[-1], last_batch, layer=watermark.SILVER)

    return dict(
        entity=entity, status="succeeded", rows=totals["rows_written"], batch_id=last_batch,
        partitions=len(partitions), batches=len(batches), watermark=partitions[-1],
        **totals,
    )


# %%
# Re-exported for the same reason bronze re-exports it: on Fabric a notebook cannot import another
# notebook, only `%run` it, so a helper two notebooks share is library code. See src/lib/config.py.
load_config = config.load_source_config


def main() -> None:
    result = transform(
        entity=ENTITY,
        run_id=PARAMS["run_id"],
        until_date=PARAMS["until_date"],
        force_reload=PARAMS["force_reload"],
    )
    log.info("silver %s: %s", result["status"], result)


if __name__ == "__main__":
    main()
