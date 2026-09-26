"""Structured run logging into `meta_run_log`.

**Append-only, one row per completed step.** The obvious alternative — insert a `running` row and
MERGE it to a terminal state — was rejected: it costs a MERGE per step, makes the log mutable (so
history can be rewritten by a bug), and buys in-flight visibility that on Fabric already comes from
the pipeline monitor. An immutable log that is cheap to write is worth more than a live one that
is expensive and rewritable.

The consequence is stated rather than hidden: a step killed mid-flight (driver OOM, capacity
throttle) leaves *no* row at all. `orchestration/run.py` therefore reconciles what it dispatched
against what the log contains, so a missing row is reported as `unknown` rather than passing as
success. Silence is not success.
"""
from __future__ import annotations

import logging
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.runtime.context import Layer, get_spark, write_table

log = logging.getLogger(__name__)

TABLE = "meta_run_log"

SCHEMA = (
    "run_id string, batch_id string, entity string, layer string, step string, status string, "
    "started_ts timestamp, ended_ts timestamp, duration_sec int, rows_read long, "
    "rows_written long, rows_quarantined long, error_message string"
)


@dataclass
class StepMetrics:
    """Mutable handle a step fills in as it goes; read once when the step ends."""

    rows_read: int | None = None
    rows_written: int | None = None
    rows_quarantined: int | None = None
    notes: dict[str, str] = field(default_factory=dict)


def _layer_name(layer) -> str:
    """`str(Layer.BRONZE)` on a str/Enum mixin yields "Layer.BRONZE", not "bronze". Take `.value`
    so both the log table and the console use the layer names the rest of the repo uses."""
    return getattr(layer, "value", str(layer))


def _write(run_id, batch_id, entity, layer, step, status, started, ended,
           metrics: StepMetrics, error: str | None) -> None:
    """Append one terminal row. Timestamps are passed as `datetime` objects rather than built with
    `from_unixtime`, which takes seconds as a bigint and would silently truncate the fractional part
    of a `time.time()` float — durations under a second would all log as zero."""
    spark = get_spark()
    row = [(
        run_id, batch_id, entity, _layer_name(layer), step, status,
        datetime.fromtimestamp(started, tz=timezone.utc),
        datetime.fromtimestamp(ended, tz=timezone.utc),
        int(round(ended - started)),
        metrics.rows_read, metrics.rows_written, metrics.rows_quarantined,
        error,
    )]
    write_table(spark.createDataFrame(row, SCHEMA), Layer.META, TABLE, mode="append")


@contextmanager
def step(run_id: str, batch_id: str, entity: str, layer, step: str):
    """Record one pipeline step. Yields a `StepMetrics` for the body to populate.

    A raised exception is logged as `failed` and then **re-raised**. Swallowing it would turn a
    failed DQ gate into a green run, which is the single failure mode this whole layer exists to
    prevent.
    """
    metrics = StepMetrics()
    started = time.time()
    try:
        yield metrics
    except BaseException as exc:  # noqa: BLE001 - recorded, then re-raised unchanged
        message = f"{type(exc).__name__}: {exc}"
        log.error("step failed  %s/%s/%s: %s", entity, _layer_name(layer), step, message)
        log.debug("%s", traceback.format_exc())
        _write(run_id, batch_id, entity, layer, step, "failed",
               started, time.time(), metrics, message[:4000])
        raise
    _write(run_id, batch_id, entity, layer, step, "succeeded",
           started, time.time(), metrics, None)
    log.info("step ok      %s/%s/%s  read=%s written=%s quarantined=%s (%.1fs)",
             entity, _layer_name(layer), step, metrics.rows_read, metrics.rows_written,
             metrics.rows_quarantined, time.time() - started)


def skipped(run_id: str, batch_id: str, entity: str, layer, step_name: str, reason: str) -> None:
    """Record a step that correctly did nothing — an up-to-date watermark, a disabled feed.

    Logged explicitly rather than omitted, so "no new data" and "never ran" are distinguishable
    after the fact. They are very different incidents.
    """
    now = time.time()
    _write(run_id, batch_id, entity, layer, step_name, "skipped", now, now,
           StepMetrics(), reason)
    log.info("step skipped %s/%s/%s: %s", entity, _layer_name(layer), step_name, reason)
