# %% [markdown]
# # nb_03 — Delta table maintenance
#
# `OPTIMIZE` and `VACUUM` over the lakehouse Delta tables, plus a V-Order report. Every other
# notebook in this repo moves data; this one only changes how that data is stored, and it is the one
# notebook that is safe to run at any time without reference to a watermark.
#
# ## Why it is separate from `pl_master`
#
# It is not a pipeline stage. Bronze and silver run when data arrives; maintenance runs on a clock,
# because the thing it fixes accumulates with *elapsed time* rather than with rows —
# `docs/cost-and-capacity.md` §5 measures that. Bolting it onto the end of `pl_master` would tie
# compaction to ingest frequency, which is the wrong variable, and would make every ingest run pay
# for it. On Fabric this is a scheduled notebook activity in its own pipeline; locally it is
# `make maintain`.
#
# ## Scope: the lakehouse, and nothing else
#
# `wh_gold` is a Warehouse. It manages its own storage and there is no `OPTIMIZE` for a user to run
# against it, so gold is out of scope — and it is worth being precise about why that is a *design*
# boundary rather than an omission. **On this laptop gold is Delta too**: the local substrate puts
# `dbo`, `sec` and `stg` under `_onelake/gold/` and Delta would happily compact them. Doing so would
# be exercising a capability the deployment target does not have, which is the same mistake
# `tools/fabric_tsql_lint.py` exists to prevent in the other direction — local SQL Server accepting
# T-SQL that Fabric would reject. The shim already encodes the boundary: `Layer` has no `GOLD`
# member, so `Layer("gold")` raises rather than returning something maintainable.
#
# ## The finding this notebook exists to report
#
# **`OPTIMIZE` bin-packs files within a partition and never across partitions.** A partition holds
# its own directory of Parquet files, so merging two partitions' files is not a compaction — it is a
# repartition. That makes `OPTIMIZE` powerless against the exact fragmentation this repo actually
# has: bronze writes one file per `ingest_date`, so `br_disputes` holds 95 files across 95 partitions
# and every one of them is already as compacted as a partition can be. Measured, on this lake:
# `numFilesRemoved=0`, `partitionsOptimized=0`, file count unchanged at 95.
#
# So a maintenance job that simply ran `OPTIMIZE` over bronze would report success, cost a job, and
# change nothing — which is worse than not running it, because it would retire the problem from
# anybody's list. This notebook therefore **diagnoses** that shape and names the actual fix, which is
# the partition grain in `meta_source_config` rather than anything a maintenance job can do. Deciding
# a table's partition column is a design change and stays a human's call; see `report()`.
#
# Where `OPTIMIZE` does earn its keep is the unpartitioned append-only tables, and there it is
# dramatic: `meta_run_log` compacts 48 files to 1 and its live size falls from 200,293 to 6,010
# bytes. A third of a megabyte of that table was Parquet footers and dictionaries, one set per file.
#
# ## Order of operations, which is load-bearing
#
# `OPTIMIZE` first, then `VACUUM`. Compaction writes new files and tombstones the ones it replaced;
# only `VACUUM` deletes the tombstoned files from storage. Run in the other order and every run
# leaves its own compaction debris behind for the next one to find.
#
# With the default 168-hour retention the two are nonetheless **pipelined across runs, not within
# one**: a tombstone minutes old is inside the retention window, so this run's `VACUUM` reclaims the
# *previous* run's compaction rather than its own. That is correct and deliberate — the retention
# window is what lets a concurrent reader finish a scan against files the writer has already
# replaced. Passing `--vacuum-retain-hours 0` collapses the two into one run and is how the local
# demonstration shows the reclaim immediately; it is also unsafe on a live capacity, so it warns.
#
# ## V-Order is reported, never set
#
# V-Order is Fabric's write-time Parquet optimisation, and it is **not** an OSS Delta feature. Asking
# for it locally does not degrade, it fails outright:
#
# ```
# [DELTA_UNKNOWN_CONFIGURATION] Unknown configuration was specified:
# delta.parquet.vorder.enabled
# ```
#
# So this notebook reads the property and reports it rather than setting it, which is honest on both
# substrates and happens to be the right call on Fabric too: V-Order pays for itself on tables a
# Direct Lake model reads, and no table in this lakehouse is one. The semantic model reads `wh_gold`.
# The property would become worth setting the moment a model read a lakehouse table directly — and
# the default is the part worth knowing, because it is the opposite of what most people assume:
# V-Order is **disabled by default in all newly created workspaces**
# ([Delta Lake table optimization and V-Order](https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order),
# ms.date 2026-03-01), since the default favours write-heavy engineering over read-heavy serving.

# %%
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.runtime import params
from src.runtime.context import (
    Layer,
    delta_table,
    get_spark,
    list_tables,
    read_table,
    table_exists,
    vacuum_dry_run,
    write_table,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nb_03")

PARAMS = params.resolve({
    "layers": "bronze,silver,meta,quarantine",
    "tables": "",
    "actions": "optimize,vacuum",
    "vacuum_retain_hours": 168.0,
    "small_file_mib": 16.0,
    "dry_run": False,
    "run_id": "",
})

LOG_TABLE = "meta_maintenance_log"

# Delta's own default retention. Named rather than inlined because two different things below have to
# agree about it: the warning that fires when a caller goes under it, and the docstring that explains
# why going under it is a choice and not a tuning knob.
SAFE_RETAIN_HOURS = 168.0

MIB = 1024 * 1024

# %% [markdown]
# ## Measuring a table
#
# `DESCRIBE DETAIL` reads the Delta log and never touches the data, so the whole report costs
# metadata reads. `numFiles` and `sizeInBytes` describe the **live snapshot** — the files the current
# version references — which is the number Direct Lake guardrails are evaluated against. What is
# still on disk behind it is `VACUUM`'s business, and is reported separately because conflating the
# two is how a table looks fine while its storage bill does not.

# %%
@dataclass
class TableState:
    layer: str
    name: str
    files: int
    bytes: int
    partition_columns: list[str]
    properties: dict[str, str]

    @property
    def partitioned(self) -> bool:
        return bool(self.partition_columns)

    @property
    def avg_file_bytes(self) -> float:
        return self.bytes / self.files if self.files else 0.0


def describe(layer: Layer, name: str) -> TableState:
    row = delta_table(layer, name).detail().first().asDict()
    return TableState(
        layer=layer.value,
        name=name,
        files=int(row.get("numFiles") or 0),
        bytes=int(row.get("sizeInBytes") or 0),
        partition_columns=list(row.get("partitionColumns") or []),
        properties=dict(row.get("properties") or {}),
    )


# %% [markdown]
# ## OPTIMIZE, and the diagnosis for when it cannot help
#
# `executeCompaction()` returns the metrics Delta actually recorded, which is why this reports rather
# than predicts. Two of those numbers do the work: `numFilesRemoved` is how many files were read and
# replaced, and `partitionsOptimized` is how many partitions it touched — reported as `1` for an
# unpartitioned table, which Delta treats as a single implicit partition.
#
# There is no `OPTIMIZE ... DRY RUN` in Delta. Under `--dry-run` this therefore reports the table's
# current state and skips compaction; it does not estimate what compaction would do, because an
# estimate of a bin-packing decision is a guess dressed as a measurement.

# %%
@dataclass
class ActionResult:
    action: str
    status: str
    before: TableState
    after: TableState | None = None
    files_compacted: int = 0
    files_written: int = 0
    partitions_optimized: int = 0
    stale_files_removed: int = 0
    retain_hours: float | None = None
    advisory: str = ""
    error: str = ""
    seconds: float = 0.0
    extra: dict[str, str] = field(default_factory=dict)


def compact(layer: Layer, name: str, before: TableState, dry_run: bool) -> ActionResult:
    if dry_run:
        return ActionResult("optimize", "dry_run", before, after=before,
                            advisory="Delta has no OPTIMIZE dry run; state reported, nothing done")
    started = time.time()
    result = delta_table(layer, name).optimize().executeCompaction()
    row = result.first()
    metrics = row["metrics"].asDict() if row is not None else {}
    after = describe(layer, name)
    return ActionResult(
        "optimize", "applied", before, after,
        files_compacted=int(metrics.get("numFilesRemoved") or 0),
        files_written=int(metrics.get("numFilesAdded") or 0),
        partitions_optimized=int(metrics.get("partitionsOptimized") or 0),
        seconds=time.time() - started,
    )


def diagnose(result: ActionResult, small_file_bytes: float) -> str:
    """Name the fragmentation `OPTIMIZE` provably cannot fix, and only that one.

    All four conditions are load-bearing. The table must be **partitioned**, because for an
    unpartitioned table `OPTIMIZE` can always merge whatever it finds and a zero result means there
    was nothing to merge. It must hold **more than one file**, or there is no fragmentation at all —
    `br_card_products` is one 5 KiB file in one partition, which is small but not fragmented, and an
    advisory there would be noise that teaches a reader to ignore the column. Compaction must have
    **removed nothing**, which is the evidence rather than the theory. And the files must be
    **small**, since a partition holding one large file is the desired end state, not a defect.
    """
    after = result.after
    if after is None or result.status != "applied":
        return result.advisory
    if not (after.partitioned and after.files > 1 and result.files_compacted == 0
            and after.avg_file_bytes < small_file_bytes):
        return result.advisory
    return (
        f"{after.files} files across {after.partition_columns[0]} partitions averaging "
        f"{after.avg_file_bytes / 1024:.0f} KiB; OPTIMIZE cannot merge across partition boundaries, "
        f"so the fix is the partition grain in meta_source_config, not maintenance"
    )


# %% [markdown]
# ## VACUUM
#
# The file count comes from `VACUUM ... DRY RUN`, which is Delta's own answer to "what is stale" and
# is the reason `vacuum_dry_run` exists in the shim: the dry run has no Python API, only SQL, and the
# two substrates spell a table differently in a statement. The real vacuum then runs, so the reported
# number is the dry run's answer from a moment earlier rather than a return value — stated plainly
# because a concurrent writer could add a tombstone between the two, and a number that is *nearly*
# the truth should say which direction it can be wrong in.
#
# Live file count and live bytes are unchanged by `VACUUM` by definition: it deletes files no version
# in the retention window references. A `VACUUM` that changed `numFiles` would be a bug in Delta.

# %%
def vacuum_table(layer: Layer, name: str, before: TableState, retain_hours: float,
                 dry_run: bool) -> ActionResult:
    started = time.time()
    stale = vacuum_dry_run(layer, name, retain_hours)
    if dry_run:
        return ActionResult("vacuum", "dry_run", before, after=before,
                            stale_files_removed=stale, retain_hours=retain_hours,
                            seconds=time.time() - started)
    delta_table(layer, name).vacuum(retain_hours)
    return ActionResult("vacuum", "applied", before, after=describe(layer, name),
                        stale_files_removed=stale, retain_hours=retain_hours,
                        seconds=time.time() - started)


# %% [markdown]
# ## V-Order status
#
# A read, never a write — see the header. Reported per table so that "nothing here is V-Ordered" is a
# measured statement about this lake rather than an assumption about the platform.

# %%
VORDER_PROPERTY = "delta.parquet.vorder.enabled"


def vorder_status(state: TableState) -> str:
    value = state.properties.get(VORDER_PROPERTY)
    if value is None:
        return "unset"
    return "enabled" if str(value).lower() == "true" else "disabled"


# %% [markdown]
# ## The maintenance log
#
# Its own table rather than `meta_run_log`, for the same reason gold writes `stg.load_log`:
# `meta_run_log` counts **rows**, and nothing here moves a row. Recording 48 compacted files in a
# column named `rows_written` would make the reconciliation in `orchestration/run.py` read a
# maintenance sweep as a load. One row per (table, action), appended, so a scheduled job leaves a
# history that can be diffed across weeks — which is the only way to notice that compaction has
# started taking longer than it used to.
#
# **This notebook carries no copy of that table's schema.** `nb_99_seed_metadata` owns it, as it owns
# every other control-plane schema, and the column list is read back from the table at write time.
# That is a deliberate departure from the precedent set by `src/lib/run_log.py`, which holds a second
# copy of `meta_run_log`'s schema as a DDL string with nothing asserting the two agree. Projecting
# into the schema Delta actually has means the seeder decides both the names and the order: a column
# added there and not populated here lands as `NULL` instead of shifting every value one place left,
# and a column produced here that the seeder does not know about raises rather than being dropped.

# %%
def log_rows(run_id: str, results: list[ActionResult]) -> int:
    if not results:
        return 0
    if not table_exists(Layer.META, LOG_TABLE):
        raise RuntimeError(
            f"{LOG_TABLE} does not exist — run `make seed` first. This notebook deliberately holds "
            "no second copy of its schema; nb_99_seed_metadata owns it."
        )
    spark = get_spark()
    schema = read_table(Layer.META, LOG_TABLE).schema
    records = []
    for r in results:
        ended = time.time()
        after = r.after or r.before
        records.append({
            "run_id": run_id,
            "layer": r.before.layer,
            "table_name": r.before.name,
            "action": r.action,
            "status": r.status,
            "files_before": r.before.files,
            "files_after": after.files,
            "bytes_before": r.before.bytes,
            "bytes_after": after.bytes,
            "files_compacted": r.files_compacted,
            "files_written": r.files_written,
            "partitions_optimized": r.partitions_optimized,
            "stale_files_removed": r.stale_files_removed,
            "retain_hours": r.retain_hours,
            "vorder": vorder_status(after),
            "advisory": r.advisory or None,
            "error_message": r.error or None,
            "started_ts": datetime.fromtimestamp(ended - r.seconds, tz=timezone.utc),
            "ended_ts": datetime.fromtimestamp(ended, tz=timezone.utc),
            "duration_sec": round(r.seconds, 3),
        })
    unknown = set(records[0]) - {f.name for f in schema.fields}
    if unknown:
        raise RuntimeError(
            f"{LOG_TABLE} has no column(s) {sorted(unknown)}. Add them in nb_99_seed_metadata and "
            "reseed; silently dropping a measurement is worse than failing the sweep that took it."
        )
    rows = [tuple(rec.get(f.name) for f in schema.fields) for rec in records]
    write_table(spark.createDataFrame(rows, schema), Layer.META, LOG_TABLE, mode="append")
    return len(rows)


# %% [markdown]
# ## Entry point
#
# A failed action is recorded and the sweep continues to the next table, then the exit code reports
# it — the same failure policy as `orchestration/run.py`, and for the same reason: one unmaintainable
# table should not leave the other thirty-nine unmaintained. An **advisory is not a failure**; it is
# a finding that needs a design decision, and failing the job on it would train somebody to pass a
# flag that suppresses it.

# %%
def resolve_layers(spec: str) -> list[Layer]:
    """`Layer(name)` raises on anything that is not a lakehouse layer, `gold` included."""
    names = [s.strip() for s in spec.split(",") if s.strip()]
    if not names:
        raise ValueError("parameter `layers` resolved to nothing")
    try:
        return [Layer(n) for n in names]
    except ValueError as exc:
        valid = ", ".join(sorted(m.value for m in Layer if m is not Layer.LANDING))
        raise ValueError(
            f"{exc}. Maintainable layers: {valid}. Gold is deliberately absent — it is a Warehouse "
            "on Fabric and manages its own storage; see the header of this notebook."
        ) from None


def maintain(layers: str = "bronze,silver,meta,quarantine", tables: str = "",
             actions: str = "optimize,vacuum", vacuum_retain_hours: float = SAFE_RETAIN_HOURS,
             small_file_mib: float = 16.0, dry_run: bool = False,
             run_id: str = "") -> tuple[str, list[ActionResult]]:
    run_id = run_id or f"maint-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    # `retain_hours` lands in a DoubleType column, and Spark's local-data path rejects an `int` for
    # one rather than widening it. The CLI never trips this — `params.resolve` coerces against the
    # float default — so it is only reachable from a Python caller, which is exactly what the tests
    # are. Coerced here, at the one place the value enters, rather than at the write.
    vacuum_retain_hours = float(vacuum_retain_hours)
    wanted_actions = [a.strip() for a in actions.split(",") if a.strip()]
    unknown = set(wanted_actions) - {"optimize", "vacuum"}
    if unknown:
        raise ValueError(f"unknown action(s): {sorted(unknown)}. Valid: optimize, vacuum")
    only = {t.strip() for t in tables.split(",") if t.strip()}
    small_file_bytes = small_file_mib * MIB

    if vacuum_retain_hours < SAFE_RETAIN_HOURS and "vacuum" in wanted_actions and not dry_run:
        log.warning(
            "vacuum retention %.1fh is below Delta's %.0fh default. This deletes files that readers "
            "with an in-flight scan may still need and ends time travel before that point. Correct "
            "for a local demonstration; on a live capacity it is a decision, not a default.",
            vacuum_retain_hours, SAFE_RETAIN_HOURS,
        )

    results: list[ActionResult] = []
    for layer in resolve_layers(layers):
        names = [n for n in list_tables(layer) if not only or n in only]
        if not names:
            log.info("%s: no tables to maintain", layer.value)
            continue
        log.info("── %s: %s table(s)", layer.value, len(names))
        for name in names:
            if not table_exists(layer, name):  # pragma: no cover — a directory without a valid log
                continue
            before = describe(layer, name)
            for action in wanted_actions:
                try:
                    if action == "optimize":
                        res = compact(layer, name, before, dry_run)
                        res.advisory = diagnose(res, small_file_bytes)
                    else:
                        res = vacuum_table(layer, name, before, vacuum_retain_hours, dry_run)
                except Exception as exc:  # noqa: BLE001 — recorded per table, never aborts the sweep
                    log.error("%s.%s %s failed: %s", layer.value, name, action, exc)
                    res = ActionResult(action, "failed", before, error=f"{type(exc).__name__}: {exc}")
                results.append(res)
                # Later actions see the table as the earlier one left it: VACUUM's `files_before`
                # must be the post-compaction count, or the log would imply it reclaimed live files.
                if res.after is not None:
                    before = res.after

    logged = log_rows(run_id, results)
    log.info("run %s: %s action(s) recorded in %s", run_id, logged, LOG_TABLE)
    return run_id, results


def report(results: list[ActionResult]) -> int:
    if not results:
        print("\nno tables matched — nothing to maintain")
        return 0
    width = max([len(f"{r.before.layer}.{r.before.name}") for r in results] + [5])
    print()
    header = (f"{'table'.ljust(width)}  {'action':<9} {'status':<8} {'files':>12} "
              f"{'live bytes':>20} {'stale':>7}  vorder")
    print(header)
    print("-" * len(header))
    for r in results:
        after = r.after or r.before
        files = (f"{r.before.files:,}" if after.files == r.before.files
                 else f"{r.before.files:,}→{after.files:,}")
        size = (f"{r.before.bytes:,}" if after.bytes == r.before.bytes
                else f"{r.before.bytes:,}→{after.bytes:,}")
        stale = f"{r.stale_files_removed:,}" if r.action == "vacuum" else ""
        print(f"{f'{r.before.layer}.{r.before.name}'.ljust(width)}  {r.action:<9} {r.status:<8} "
              f"{files:>12} {size:>20} {stale:>7}  {vorder_status(after)}")

    advisories = [r for r in results if r.advisory and r.status == "applied"]
    failed = [r for r in results if r.status == "failed"]
    compacted = sum(r.files_compacted for r in results)
    written = sum(r.files_written for r in results)
    reclaimed = sum(r.stale_files_removed for r in results)
    print()
    print(f"{len(results)} action(s): {compacted:,} file(s) compacted into {written:,}, "
          f"{reclaimed:,} stale file(s) {'identified' if any(r.status == 'dry_run' for r in results) else 'removed'}, "
          f"{len(failed)} failed")

    if advisories:
        print(f"\n{len(advisories)} table(s) that OPTIMIZE cannot help — each needs a design "
              "decision, not a maintenance run:")
        for r in sorted(advisories, key=lambda r: -(r.after or r.before).files):
            print(f"  {r.before.layer}.{r.before.name}: {r.advisory}")
    for r in failed:
        print(f"  FAILED {r.before.layer}.{r.before.name} {r.action}: {r.error}")
    return 1 if failed else 0


def main() -> int:
    if not table_exists(Layer.META, "meta_source_config"):
        log.warning("the control plane is unseeded; maintenance will still run over whatever "
                    "Delta tables exist")
    _run_id, results = maintain(
        layers=PARAMS["layers"],
        tables=PARAMS["tables"],
        actions=PARAMS["actions"],
        vacuum_retain_hours=float(PARAMS["vacuum_retain_hours"]),
        small_file_mib=float(PARAMS["small_file_mib"]),
        dry_run=bool(PARAMS["dry_run"]),
        run_id=PARAMS["run_id"],
    )
    return report(results)


if __name__ == "__main__":
    raise SystemExit(main())
