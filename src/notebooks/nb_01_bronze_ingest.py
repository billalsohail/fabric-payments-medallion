# %% [markdown]
# # nb_01 — Bronze ingest
#
# **One** notebook ingests all seven feeds. There is no per-entity branch anywhere in this file:
# every difference between `transactions` (JSON, incremental, schema-evolving) and `card_products`
# (CSV, static) is a column in `meta_source_config`. That is the whole point of the control plane,
# and it is the claim to test — an eighth feed must be a config row, not an edit to this notebook.
#
# On Fabric this is a Notebook activity inside a `ForEach` over the `Lookup` output of
# `meta_source_config`, parameterised on `entity`. `orchestration/run.py` is the local stand-in for
# that graph.
#
# ## What bronze is, and what it deliberately is not
#
# Bronze is **append-only, source-faithful, and free of business logic**. No casting, no renaming,
# no filtering, no deduplication. CSV lands as all-strings because that is what CSV *is*; typing it
# here would mean a malformed date becomes a silent `NULL` in the layer that is supposed to be the
# reproducible record of what arrived. Casting belongs in silver, where a failure can be quarantined
# with the rule that caught it. Bronze's only job is: everything that arrived, exactly as it
# arrived, plus provenance.
#
# ## Idempotency — two mechanisms, two different questions
#
# 1. **The watermark** decides *what to read*. Partitions at or below it are already in bronze.
# 2. **`_batch_id`** decides *what a retry replaces*. A batch id is derived from the window being
#    loaded, not from the clock, so re-running the same window produces the same id; the write then
#    deletes that batch and reinserts it.
#
# Both are needed. The watermark alone makes the happy path a no-op but cannot repair a run that died
# after writing half its rows — the watermark never advanced, so the next attempt re-reads the same
# window and, without (2), would double-count. A clock-based batch id would make (2) useless for
# exactly the case it exists for. This is why `batch_id` is deterministic, and it is the single
# most important design decision in this file.

# %%
from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.lib import config, run_log, watermark
from src.runtime import params
from src.runtime.context import (
    Layer,
    delta_table,
    get_spark,
    landing_path,
    list_landing_partitions,
    read_table,
    table_exists,
    write_table,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nb_01")

# %% tags=["parameters"]
PARAMS = params.resolve({
    # The only required parameter. Everything else about the feed comes from config.
    "entity": "",
    # Inclusive upper bound on ingest_date. Empty means "everything available". Exists for backfills
    # and for the schema-evolution test, which has to load the pre-drift window first to prove that
    # bronze *absorbs* the new column rather than being handed it pre-merged by JSON schema
    # inference across the whole feed at once.
    "until_date": "",
    # Override the derived batch id. Used by replay tooling; normally left empty.
    "batch_id": "",
    "run_id": "",
    # Ignore the watermark and re-read every available partition. Loud, deliberate, and logged.
    "force_reload": False,
})

ENTITY: str = PARAMS["entity"]
BRONZE_PREFIX = "br_"

# %% [markdown]
# ## Config lookup
#
# A missing or disabled config row is a hard error, not a skip. "The entity I was asked to load is
# not configured" is a deployment mistake; silently succeeding on it is how a feed goes missing for
# a week without anyone noticing.

# %%
# Re-exported rather than defined here: silver reads the same config table, and on Fabric a
# notebook cannot import another notebook — only `%run` it. See src/lib/config.py.
load_config = config.load_source_config


# %% [markdown]
# ## Window selection
#
# `load_type` is resolved to a candidate partition list, and *then* the watermark filters it. Doing
# it in that order keeps all three load types on one code path:
#
# | `load_type` | Candidates | Effect once the watermark filter is applied |
# |---|---|---|
# | `incremental_watermark` | every available partition | only genuinely new days |
# | `full_snapshot` | the newest partition only | the latest snapshot, once |
# | `static` | every available partition | everything on first load, nothing after |
#
# `full_snapshot` takes only the newest partition because older snapshots of master data are
# superseded, not additive — loading all of them would put four copies of every customer in bronze
# for a 120-day feed, and silver would then have to guess which is current.

# %%
def select_window(cfg: dict, until_date: str, force_reload: bool) -> tuple[list[str], str | None]:
    entity = cfg["entity"]
    available = list_landing_partitions(entity)
    if until_date:
        available = [p for p in available if p <= until_date]
    if not available:
        return [], None

    load_type = cfg["load_type"]
    if load_type == "full_snapshot":
        candidates = [available[-1]]
    elif load_type in ("incremental_watermark", "static"):
        candidates = available
    else:
        raise ValueError(
            f"unknown load_type {load_type!r} for {entity}. "
            "Expected one of: incremental_watermark, full_snapshot, static."
        )

    wm = None if force_reload else watermark.get(entity)
    if wm is not None:
        candidates = [p for p in candidates if p > wm]
    return candidates, wm


def derive_batch_id(entity: str, partitions: list[str]) -> str:
    """Batch id from the window, never from the clock — see the idempotency note at the top."""
    return f"{entity}|{partitions[0]}|{partitions[-1]}"


# %% [markdown]
# ## Read
#
# Partitions are loaded by explicit path with `basePath` set to the entity root. Without `basePath`,
# pointing Spark at `.../ingest_date=2026-06-01` loses the partition column entirely — the value
# lives in the directory name, and Spark only recovers it when it knows where partitioning started.
# The alternative, loading the root and filtering, reads schema metadata for every day in the feed
# on every run; that is the cost this avoids.

# %%
def read_landing(cfg: dict, partitions: list[str]) -> DataFrame:
    spark = get_spark()
    root = landing_path(cfg["entity"])
    paths = [landing_path(cfg["entity"], p) for p in partitions]
    reader = spark.read.format(cfg["source_format"]).options(**cfg["read_options"])
    reader = reader.option("basePath", root)
    df = reader.load(paths)

    # Spark *infers* the partition column's type from the directory names, which for
    # `ingest_date=2026-08-02` happens to give a date. Relying on that would contradict the rule
    # applied to every other schema in this repo: an inferred type is a type nobody chose. One
    # partition directory with an unexpected name would flip the column to string and the next Delta
    # write would fail on schema mismatch, in a layer whose purpose is stability. So it is cast
    # explicitly. This is not business logic sneaking into bronze — `ingest_date` is arrival metadata
    # the platform created, not a source field, and typing our own metadata is provenance hygiene.
    partition_col = cfg.get("partition_column")
    if partition_col and partition_col in df.columns:
        df = df.withColumn(partition_col, F.col(partition_col).cast("date"))
    return df


# %% [markdown]
# ## Audit columns
#
# Four, each earning its place:
#
# - **`_batch_id`** — the replay unit. Delete-by-batch is what makes a retry safe.
# - **`_ingest_ts`** — when bronze saw it, as distinct from `ingest_date` (when the source produced
#   it) and from `auth_ts` (when the business event happened). Three different times; conflating
#   any two of them is how late-arriving data gets reported in the wrong period.
# - **`_source_file`** — the file a row came from, via Spark's `_metadata`. This is the column that
#   turns "row 4,812,119 is wrong" into a file you can open.
# - **`_row_hash`** — content hash over business columns only, computed once here and reused by
#   SCD2 in silver for change detection. Computing it in bronze means the hash is over the source
#   bytes as they arrived, before any transformation could quietly alter it.
#
# The hash excludes `_`-prefixed columns (source control columns like `_op` and `_change_ts`, and
# these audit columns) and the partition column. Both exclusions are load-bearing: two monthly
# snapshots of an unchanged customer arrive on different `ingest_date`s, and if arrival metadata fed
# the hash, SCD2 would see a change every month and write history that never happened.

# %%
def business_columns(df: DataFrame, cfg: dict) -> list[str]:
    partition_col = cfg.get("partition_column")
    return sorted(
        c for c in df.columns if not c.startswith("_") and c != partition_col
    )


def add_audit_columns(df: DataFrame, cfg: dict, batch_id: str) -> DataFrame:
    cols = business_columns(df, cfg)
    if not cols:
        raise ValueError(
            f"{cfg['entity']}: no business columns found after excluding control and partition "
            "columns — the read almost certainly failed (wrong source_format or read_options)."
        )
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn(
            "_row_hash",
            # Null and the literal string "null" must not collide, hence the sentinel rather than
            # a plain cast. A hash collision between "absent" and "the word null" is exactly the
            # kind of bug that surfaces as one wrong SCD2 row six months later.
            F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in cols]),
        )
    )


# %% [markdown]
# ## Write
#
# Delete-then-append, scoped to this batch id. `mergeSchema` is enabled only for feeds whose config
# says the source may evolve — an unexpected new column on a feed that is not supposed to change is
# a source-system incident, and the pipeline should fail loudly rather than widen the table and
# carry on.

# %%
def write_bronze(df: DataFrame, cfg: dict, batch_id: str) -> int:
    table = f"{BRONZE_PREFIX}{cfg['entity']}"
    partition_col = cfg.get("partition_column")

    df = df.cache()
    rows = df.count()

    if table_exists(Layer.BRONZE, table):
        # Idempotent replay: remove any prior attempt at this exact window before inserting.
        deleted = (
            read_table(Layer.BRONZE, table).filter(F.col("_batch_id") == batch_id).count()
        )
        if deleted:
            log.warning(
                "%s: batch %s already present with %s rows — replacing (retry or forced reload)",
                table, batch_id, deleted,
            )
            delta_table(Layer.BRONZE, table).delete(F.col("_batch_id") == batch_id)

    write_table(
        df,
        Layer.BRONZE,
        table,
        mode="append",
        partition_by=[partition_col] if partition_col else None,
        merge_schema=bool(cfg.get("allow_schema_evolution")),
    )
    df.unpersist()
    return rows


# %% [markdown]
# ## Entry point

# %%
def ingest(entity: str, until_date: str = "", batch_id: str = "", run_id: str = "",
           force_reload: bool = False) -> dict:
    cfg = load_config(entity)
    config.require_enabled(cfg)

    run_id = run_id or f"local-{int(time.time())}"
    partitions, wm = select_window(cfg, until_date, force_reload)

    if not partitions:
        reason = (
            f"no landing partitions for {entity}"
            if wm is None
            else f"watermark {wm} is current"
        )
        run_log.skipped(run_id, "", entity, Layer.BRONZE, "bronze_ingest", reason)
        return dict(entity=entity, status="skipped", reason=reason, rows=0)

    batch = batch_id or derive_batch_id(entity, partitions)
    log.info(
        "%s: %s partition(s) %s..%s  load_type=%s watermark=%s batch=%s",
        entity, len(partitions), partitions[0], partitions[-1], cfg["load_type"], wm, batch,
    )

    with run_log.step(run_id, batch, entity, Layer.BRONZE, "bronze_ingest") as metrics:
        raw = read_landing(cfg, partitions)
        audited = add_audit_columns(raw, cfg, batch)
        rows = write_bronze(audited, cfg, batch)
        metrics.rows_read = rows
        metrics.rows_written = rows
        # Advanced only after the write has committed. A watermark ahead of the data is worse than
        # no watermark at all: it makes the missing rows invisible instead of merely absent.
        watermark.set(entity, partitions[-1], batch)

    return dict(
        entity=entity, status="succeeded", rows=rows, batch_id=batch,
        partitions=len(partitions), watermark=partitions[-1],
    )


def main() -> None:
    result = ingest(
        entity=ENTITY,
        until_date=PARAMS["until_date"],
        batch_id=PARAMS["batch_id"],
        run_id=PARAMS["run_id"],
        force_reload=PARAMS["force_reload"],
    )
    log.info("bronze %s: %s", result["status"], result)


if __name__ == "__main__":
    main()
