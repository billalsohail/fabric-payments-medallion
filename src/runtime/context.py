"""Execution-context shim: the same transformation code runs locally and on Microsoft Fabric.

Why this exists
---------------
Every notebook in ``src/notebooks`` is written as Fabric notebook code. Nothing in them knows
whether it is running against a Fabric Lakehouse or a directory on a laptop, because they only ever
touch the helpers in this module. That is what makes "this is Fabric code" a verifiable statement
rather than a claim.

The mapping
-----------
===================  ==================================  =====================================
Concept              LOCAL                               FABRIC
===================  ==================================  =====================================
Spark session        built here, Delta configured         ambient ``spark`` from the kernel
Landing files        ``_onelake/files/landing/...``       ``Files/landing/...`` in OneLake
Delta table          path under ``_onelake/<layer>/``     ``lh_<layer>`` Lakehouse table
Gold (T-SQL)         SQL Server 2022 via pyodbc           ``wh_gold`` Warehouse endpoint
===================  ==================================  =====================================

Environment detection is explicit, never guessed silently: ``FABRIC_ENV=fabric`` forces Fabric mode,
and otherwise the presence of the ``notebookutils`` module (only installed inside Fabric Spark) is
the signal. The resolved mode is logged on first use so a run's output always states where it ran.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Layers. Values are the Fabric lakehouse item names, which is also the local directory name,
# so one enum drives both substrates.
class Layer(str, Enum):
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    META = "meta"
    QUARANTINE = "quarantine"


class Env(str, Enum):
    LOCAL = "local"
    FABRIC = "fabric"


def detect_env() -> Env:
    """Resolve the execution substrate. Explicit override wins over auto-detection."""
    forced = os.environ.get("FABRIC_ENV", "").strip().lower()
    if forced in (Env.FABRIC.value, Env.LOCAL.value):
        return Env(forced)
    try:
        import notebookutils  # noqa: F401  — only present inside a Fabric Spark kernel
        return Env.FABRIC
    except ImportError:
        return Env.LOCAL


@dataclass(frozen=True)
class RuntimeConfig:
    """Resolved, immutable runtime settings. Mirrors a Fabric Variable Library entry set."""

    env: Env = field(default_factory=detect_env)
    # Local root standing in for OneLake. Ignored on Fabric.
    onelake_root: Path = field(
        default_factory=lambda: Path(os.environ.get("ONELAKE_ROOT", "_onelake")).resolve()
    )
    # Fabric lakehouse item name prefix: lh_bronze, lh_silver, lh_meta.
    lakehouse_prefix: str = os.environ.get("FABRIC_LAKEHOUSE_PREFIX", "lh_")
    # Laptop-sized shuffle. On Fabric this is left to the runtime/pool defaults.
    local_shuffle_partitions: int = int(os.environ.get("LOCAL_SHUFFLE_PARTITIONS", "8"))
    local_driver_memory: str = os.environ.get("LOCAL_DRIVER_MEMORY", "4g")

    @property
    def is_fabric(self) -> bool:
        return self.env is Env.FABRIC


@lru_cache(maxsize=1)
def config() -> RuntimeConfig:
    cfg = RuntimeConfig()
    logger.info("runtime resolved: env=%s onelake_root=%s", cfg.env.value, cfg.onelake_root)
    return cfg


# --------------------------------------------------------------------------------------
# Spark session
# --------------------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_spark():
    """Return the SparkSession.

    On Fabric the kernel has already created one and ``getActiveSession()`` returns it — we must not
    build our own or we would lose the lakehouse bindings. Locally we construct an equivalent
    session with Delta wired up and laptop-appropriate sizing.
    """
    from pyspark.sql import SparkSession

    cfg = config()
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if cfg.is_fabric:  # pragma: no cover — cannot be exercised off-tenant
        raise RuntimeError(
            "FABRIC_ENV=fabric but no active SparkSession. Inside Fabric the kernel provides one; "
            "this module must not create a session on a Fabric cluster."
        )

    from delta import configure_spark_with_delta_pip

    # Spark launches Python workers via `python3` from PATH unless told otherwise, which on this
    # machine is 3.14 while the venv driver is 3.11 — Spark refuses to run across minor versions.
    # Pinning both to the interpreter that imported us keeps `uv run` and a bare venv equivalent.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName("fabric-payments-medallion")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", cfg.local_driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.local_shuffle_partitions))
        # UTC everywhere: timestamps in the contracts are UTC, and a laptop in BST would
        # otherwise silently shift settlement dates.
        .config("spark.sql.session.timeZone", "UTC")
        # Fabric writes Delta with these on; keep parity so behaviour matches.
        .config("spark.databricks.delta.autoCompact.enabled", "false")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        # Keeps VACUUM tests able to use a zero retention window.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "ERROR"))
    return spark


# --------------------------------------------------------------------------------------
# Path / table resolution
# --------------------------------------------------------------------------------------

def landing_path(entity: str, ingest_date: date | str | None = None) -> str:
    """Location of a landing partition for a source entity.

    Fabric: ``Files/landing/<entity>/ingest_date=YYYY-MM-DD``
    Local:  ``<onelake_root>/files/landing/<entity>/ingest_date=YYYY-MM-DD``

    Omitting ``ingest_date`` returns the entity root, which is how a full-history read is expressed.
    """
    cfg = config()
    suffix = f"{entity}" if ingest_date is None else f"{entity}/ingest_date={ingest_date}"
    if cfg.is_fabric:
        return f"Files/landing/{suffix}"
    return str(cfg.onelake_root / "files" / "landing" / suffix)


def table_ref(layer: Layer | str, name: str) -> str:
    """Identifier used to read/write a Delta table.

    Fabric returns a catalog table name (``lh_silver.dim_account``) because Fabric lakehouse tables
    are registered in the workspace metastore. Local returns a filesystem path, since there is no
    metastore worth standing up for this.

    Callers should prefer :func:`read_table` / :func:`write_table` over using this directly.
    """
    layer = Layer(layer) if not isinstance(layer, Layer) else layer
    cfg = config()
    if cfg.is_fabric:
        return f"{cfg.lakehouse_prefix}{layer.value}.{name}"
    return str(cfg.onelake_root / layer.value / name)


def table_exists(layer: Layer | str, name: str) -> bool:
    cfg = config()
    ref = table_ref(layer, name)
    if cfg.is_fabric:  # pragma: no cover
        return get_spark().catalog.tableExists(ref)
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(get_spark(), ref)


def read_table(layer: Layer | str, name: str):
    """Read a Delta table from a medallion layer."""
    cfg = config()
    ref = table_ref(layer, name)
    if cfg.is_fabric:  # pragma: no cover
        return get_spark().read.table(ref)
    return get_spark().read.format("delta").load(ref)


def write_table(
    df,
    layer: Layer | str,
    name: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
    merge_schema: bool = False,
) -> None:
    """Write a DataFrame as a Delta table into a medallion layer.

    ``merge_schema`` is how the month-10 ``wallet_type`` column is absorbed in bronze without a
    pipeline failure — see docs/data-contracts.md.
    """
    cfg = config()
    ref = table_ref(layer, name)
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if cfg.is_fabric:  # pragma: no cover
        writer.saveAsTable(ref)
    else:
        writer.save(ref)


def delta_table(layer: Layer | str, name: str):
    """Return a ``DeltaTable`` handle, for MERGE / DELETE operations."""
    from delta.tables import DeltaTable

    cfg = config()
    ref = table_ref(layer, name)
    if cfg.is_fabric:  # pragma: no cover
        return DeltaTable.forName(get_spark(), ref)
    return DeltaTable.forPath(get_spark(), ref)


def reset_caches() -> None:
    """Drop memoised config/session. Tests use this when they change environment variables."""
    config.cache_clear()
    get_spark.cache_clear()
