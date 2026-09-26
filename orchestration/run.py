"""Local metadata-driven pipeline driver — the stand-in for the Fabric `pl_master` pipeline.

This is a deliberate 1:1 mirror of the Data Factory graph, not a convenience script:

| Here | Fabric `pl_master` |
|---|---|
| `read_table(META, "meta_source_config")` filtered on `enabled` | **Lookup** activity |
| grouping by `priority` into sequential waves | chained **ForEach** activities |
| `ThreadPoolExecutor` inside a wave | ForEach with `isSequential = false`, `batchCount = N` |
| `nb_01_bronze_ingest.ingest(entity=...)` | **Notebook** activity, parameterised |
| `nb_02_silver_transform.transform(entity=...)` | **Notebook** activity, parameterised |
| retry with backoff | activity `retry` / `retryIntervalInSeconds` |
| the reconciliation at the end | **Stored procedure** activity (`sp_load_*` on `wh_gold`) |

Because the mapping is explicit, `docs/fabric-deployment.md` can describe the pipeline JSON by
pointing at this file rather than by hand-waving.

## Waves, not a flat fan-out

Entities run in `priority` order, and a wave only starts when the previous one has fully finished.
That ordering is not cosmetic: `transactions` carries a `referential` DQ rule against
`dim_merchant`, so running the fact concurrently with its dimension would quarantine valid rows and
present as bad source data. Parallelism is available *within* a wave, where the entities genuinely
do not depend on each other.

## Failure policy

A failed entity does not stop its wave — the other feeds in it still complete, because in a
payments platform a broken FX file should not also cost you the day's transactions. It *does* stop
the next wave from starting, since downstream waves depend on upstream ones. The process exits
non-zero and prints a table of what failed, which is what CI asserts on.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from pyspark.sql import functions as F

from src.runtime.context import Layer, get_spark, read_table, table_exists

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pl_master")

STAGES = ("bronze", "silver")


@dataclass
class Outcome:
    entity: str
    stage: str
    status: str
    rows: int = 0
    detail: str = ""


def load_plan(only: list[str] | None) -> list[dict]:
    """The Lookup activity: read the control plane, keep what is enabled, order by wave."""
    if not table_exists(Layer.META, "meta_source_config"):
        raise SystemExit(
            "meta_source_config does not exist. Seed the control plane first:\n"
            "  make seed   (or: python -m src.notebooks.nb_99_seed_metadata)"
        )
    rows = (
        read_table(Layer.META, "meta_source_config")
        .filter(F.col("enabled"))
        .orderBy("priority", "entity")
        .collect()
    )
    plan = [r.asDict() for r in rows]
    if only:
        wanted = set(only)
        unknown = wanted - {p["entity"] for p in plan}
        if unknown:
            raise SystemExit(
                f"unknown or disabled entities: {sorted(unknown)}. "
                f"Enabled: {sorted(p['entity'] for p in plan)}"
            )
        plan = [p for p in plan if p["entity"] in wanted]
    return plan


def waves(plan: list[dict]) -> list[tuple[int, list[dict]]]:
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in plan:
        grouped[row["priority"]].append(row)
    return sorted(grouped.items())


def _run_with_retry(fn, entity: str, stage: str, attempts: int, backoff_sec: float) -> Outcome:
    """Retry is only safe because both stages are idempotent by batch id — see nb_01's header.

    Without that property a retry would double-count, and this function would be the bug rather
    than the resilience.
    """
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            result = fn(entity)
            return Outcome(
                entity=entity,
                stage=stage,
                status=result.get("status", "succeeded"),
                rows=int(result.get("rows") or 0),
                detail=result.get("reason", "") or result.get("batch_id", ""),
            )
        except Exception as exc:  # noqa: BLE001 — recorded per entity, never allowed to abort a wave
            last = exc
            if attempt < attempts:
                wait = backoff_sec * attempt
                log.warning("%s/%s attempt %s/%s failed (%s) — retrying in %.0fs",
                            entity, stage, attempt, attempts, exc, wait)
                time.sleep(wait)
    return Outcome(entity=entity, stage=stage, status="failed",
                   detail=f"{type(last).__name__}: {last}")


def run_stage(stage: str, plan: list[dict], parallelism: int, attempts: int,
              backoff_sec: float, until_date: str, force_reload: bool) -> list[Outcome]:
    if stage == "bronze":
        from src.notebooks import nb_01_bronze_ingest as nb

        def call(entity: str) -> dict:
            return nb.ingest(entity=entity, until_date=until_date, force_reload=force_reload,
                             run_id=RUN_ID)
    elif stage == "silver":
        try:
            from src.notebooks import nb_02_silver_transform as nb  # noqa: PLC0415
        except ImportError:
            log.warning("silver stage not implemented yet — skipping")
            return []

        def call(entity: str) -> dict:
            return nb.transform(entity=entity, run_id=RUN_ID)
    else:  # pragma: no cover — guarded by argparse choices
        raise ValueError(stage)

    outcomes: list[Outcome] = []
    for priority, group in waves(plan):
        entities = [row["entity"] for row in group]
        log.info("── %s wave %s: %s", stage, priority, ", ".join(entities))
        # A wave narrower than the pool gets a pool the size of the wave; spinning up eight threads
        # for two entities just makes the logs harder to read.
        with ThreadPoolExecutor(max_workers=min(parallelism, len(entities))) as pool:
            futures = {
                pool.submit(_run_with_retry, call, e, stage, attempts, backoff_sec): e
                for e in entities
            }
            wave_outcomes = [f.result() for f in as_completed(futures)]
        outcomes.extend(sorted(wave_outcomes, key=lambda o: o.entity))

        failed = [o.entity for o in wave_outcomes if o.status == "failed"]
        if failed:
            log.error("wave %s had failures (%s) — not starting later waves, since they depend "
                      "on this one", priority, ", ".join(sorted(failed)))
            break
    return outcomes


def reconcile(run_id: str, dispatched: list[Outcome]) -> list[Outcome]:
    """Cross-check the log against what was dispatched.

    `meta_run_log` is append-only and written when a step *ends*, so a step killed mid-flight leaves
    no row. Reporting only what the log contains would therefore silently drop exactly the failures
    worth knowing about. Anything dispatched without a matching row is reported as `unknown`.
    """
    if not table_exists(Layer.META, "meta_run_log"):
        return dispatched
    logged = {
        (r["entity"], r["layer"])
        for r in read_table(Layer.META, "meta_run_log")
        .filter(F.col("run_id") == run_id)
        .select("entity", "layer")
        .collect()
    }
    out = []
    for o in dispatched:
        if o.status != "failed" and (o.entity, o.stage) not in logged:
            out.append(Outcome(o.entity, o.stage, "unknown", o.rows,
                               "dispatched but absent from meta_run_log"))
        else:
            out.append(o)
    return out


def report(outcomes: list[Outcome]) -> int:
    width = max([len(o.entity) for o in outcomes] + [6])
    print()
    print(f"{'entity'.ljust(width)}  {'stage':<7} {'status':<10} {'rows':>9}  detail")
    print("-" * (width + 42))
    for o in outcomes:
        print(f"{o.entity.ljust(width)}  {o.stage:<7} {o.status:<10} {o.rows:>9,}  {o.detail[:60]}")
    bad = [o for o in outcomes if o.status in ("failed", "unknown")]
    total = sum(o.rows for o in outcomes)
    print()
    print(f"{len(outcomes)} step(s), {total:,} rows, {len(bad)} not successful")
    return 1 if bad else 0


def main(argv: list[str] | None = None) -> int:
    global RUN_ID  # noqa: PLW0603 — one run id per process, read by the stage closures

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stages", default="bronze,silver",
                    help="comma-separated subset of: " + ",".join(STAGES))
    ap.add_argument("--entities", default="",
                    help="comma-separated subset of entities; default is every enabled feed")
    ap.add_argument("--parallelism", type=int, default=4,
                    help="max concurrent entities within a wave (Fabric: ForEach batchCount)")
    ap.add_argument("--attempts", type=int, default=2, help="attempts per entity per stage")
    ap.add_argument("--backoff-sec", type=float, default=3.0)
    ap.add_argument("--until-date", default="",
                    help="inclusive ingest_date cap, for backfills and the schema-evolution test")
    ap.add_argument("--force-reload", action="store_true",
                    help="ignore watermarks and re-read every available partition")
    ap.add_argument("--run-id", default="")
    args = ap.parse_args(argv)

    RUN_ID = args.run_id or f"run-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = set(stages) - set(STAGES)
    if unknown:
        raise SystemExit(f"unknown stage(s): {sorted(unknown)}. Valid: {list(STAGES)}")

    only = [e.strip() for e in args.entities.split(",") if e.strip()] or None
    plan = load_plan(only)
    log.info("run %s: %s entit(ies), stages=%s, parallelism=%s",
             RUN_ID, len(plan), stages, args.parallelism)

    get_spark()  # build the session once, on the main thread, before any worker touches it

    outcomes: list[Outcome] = []
    for stage in stages:
        stage_outcomes = run_stage(stage, plan, args.parallelism, args.attempts,
                                   args.backoff_sec, args.until_date, args.force_reload)
        outcomes.extend(stage_outcomes)
        if any(o.status == "failed" for o in stage_outcomes):
            log.error("stage %s failed — later stages not attempted", stage)
            break

    return report(reconcile(RUN_ID, outcomes))


RUN_ID = ""

if __name__ == "__main__":
    sys.exit(main())
