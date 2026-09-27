"""Delta maintenance: what `OPTIMIZE` fixes, what it provably cannot, and the layer it refuses.

Three of these tests pin claims that are made in prose elsewhere in the repo and would otherwise be
taken on trust:

* `OPTIMIZE` bin-packs **within** a partition and never across, so a table holding one file per
  partition is already fully compacted and no amount of maintenance will help it. That is the whole
  of bronze here, and `docs/cost-and-capacity.md` §5 is built on it.
* Gold is **not maintainable**, even though it is Delta on this laptop. On Fabric it is a Warehouse
  with no user-runnable `OPTIMIZE`, and compacting it locally would be exercising a capability the
  deployment target does not have — the same error `tools/fabric_tsql_lint.py` exists to prevent in
  the other direction.
* V-Order is **read and reported, never set**. OSS Delta rejects the property outright, so a
  notebook that set it would be shipping a line that cannot run on the substrate it was tested on.

The tables here are built directly rather than by running bronze ingest: the shapes that matter are
"one small file per partition" and "many files, no partitions", and constructing them takes a second
where a real ingest takes a minute.
"""
from __future__ import annotations

import pytest

from src.notebooks import nb_03_table_maintenance as nb03
from src.notebooks import nb_99_seed_metadata as nb99
from src.runtime.context import Layer, delta_table, get_spark, read_table, write_table

PARTITIONS = 8
APPENDS = 12


def _files(layer: Layer, name: str) -> int:
    return int(delta_table(layer, name).detail().first()["numFiles"])


def _fragment(layer: Layer, name: str, appends: int, partitioned: bool) -> None:
    """One separate write per call, because file count is a function of writes, not of rows."""
    spark = get_spark()
    for i in range(appends):
        if partitioned:
            df = spark.createDataFrame([(i, f"2026-01-{i + 1:02d}")], "n int, ingest_date string")
            write_table(df, layer, name, mode="append", partition_by=["ingest_date"])
        else:
            write_table(spark.createDataFrame([(i,)], "n int"), layer, name, mode="append")


@pytest.fixture(scope="module")
def lake(isolated_lake):
    """Three deliberately different fragmentation shapes, plus the seeded control plane."""
    # Many files, no partitions: the shape OPTIMIZE is for. `meta_run_log` is this in production.
    _fragment(Layer.QUARANTINE, "q_appends", APPENDS, partitioned=False)
    # One small file per partition: the shape OPTIMIZE cannot touch. All of bronze is this.
    _fragment(Layer.BRONZE, "br_probe", PARTITIONS, partitioned=True)
    # One partition, one file: small but *not* fragmented. `br_card_products` is this, and it must
    # not draw an advisory — an advisory on a healthy table teaches a reader to ignore the column.
    _fragment(Layer.BRONZE, "br_single", 1, partitioned=True)
    return isolated_lake


def _by_table(results, name, action):
    return next(r for r in results if r.before.name == name and r.action == action)


# ----------------------------------------------------------------------------------------
# The layer boundary
# ----------------------------------------------------------------------------------------

def test_gold_is_not_a_maintainable_layer() -> None:
    """The design boundary, asserted at the only place it could be crossed.

    Locally `_onelake/gold/` holds twenty Delta tables across `dbo`, `sec` and `stg`, and Delta would
    compact them happily. On Fabric gold is a Warehouse: there is no `OPTIMIZE` for a user to run
    against it, so a maintenance job that included gold would work on this laptop and have no
    meaning on the deployment target.
    """
    assert not hasattr(Layer, "GOLD"), (
        "Layer gained a GOLD member. The shim's enum is what makes gold unreachable from a notebook; "
        "adding it would make this notebook's refusal a convention instead of a type error."
    )
    for spec in ("gold", "bronze,gold", "GOLD"):
        with pytest.raises(ValueError, match="Warehouse") as exc:
            nb03.resolve_layers(spec)
        assert "manages its own storage" in str(exc.value), (
            "the refusal must say why, not just that: a reader who hits it needs to know gold is a "
            f"Warehouse rather than that they mistyped a layer name. Got: {exc.value}"
        )


def test_the_default_layers_are_every_delta_layer_and_only_those() -> None:
    """Read from the signature rather than `PARAMS`, which resolves against `sys.argv` under pytest.

    The interesting half is what is *absent*: landing holds files, not Delta tables, so a sweep that
    included it by default would spend its first minute on directories with no transaction log.
    """
    import inspect
    default = inspect.signature(nb03.maintain).parameters["layers"].default
    assert nb03.resolve_layers(default) == [
        Layer.BRONZE, Layer.SILVER, Layer.META, Layer.QUARANTINE,
    ]
    assert set(Layer) - {Layer.LANDING} == {
        Layer.BRONZE, Layer.SILVER, Layer.META, Layer.QUARANTINE,
    }, "a new Delta layer exists and the default sweep does not cover it"


def test_maintenance_is_not_a_pipeline_stage() -> None:
    """What it fixes accumulates with the calendar, not with rows — so it has its own schedule.

    Wiring it into `pl_master` would tie compaction to ingest frequency, which is the wrong variable,
    and make every load pay for it. Asserted because the tempting edit is a one-liner.
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "orchestration" / "run.py").read_text()
    assert "nb_03" not in src, (
        "orchestration/run.py now references nb_03_table_maintenance. Maintenance runs on a clock, "
        "not on arrival of data — see the header of the notebook and the Makefile's `maintain`."
    )


def test_an_unknown_action_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown action"):
        nb03.maintain(actions="optimize,zorder")


# ----------------------------------------------------------------------------------------
# What OPTIMIZE does, and the case where it provably cannot help
# ----------------------------------------------------------------------------------------

def test_optimize_compacts_an_unpartitioned_append_table(lake) -> None:
    assert _files(Layer.QUARANTINE, "q_appends") == APPENDS
    _, results = nb03.maintain(layers="quarantine", tables="q_appends", actions="optimize")
    r = _by_table(results, "q_appends", "optimize")
    assert r.status == "applied"
    assert r.before.files == APPENDS
    assert r.after.files == 1
    assert r.files_compacted == APPENDS
    assert r.files_written == 1
    # Unpartitioned: Delta reports the whole table as one implicit partition.
    assert r.partitions_optimized == 1
    assert not r.advisory


def test_compaction_reclaims_more_bytes_than_the_file_count_suggests(lake) -> None:
    """The finding worth carrying into a conversation: it is not only a file-count win.

    Every Parquet file carries its own footer, schema and dictionaries, so twelve one-row files hold
    twelve copies of that overhead and almost no data. Measured on the real lake, `meta_run_log` fell
    from 200,293 bytes to 6,010 — a 33x collapse in *live size* from a pure reorganisation.
    """
    _fragment(Layer.QUARANTINE, "q_bytes", APPENDS, partitioned=False)
    _, results = nb03.maintain(layers="quarantine", tables="q_bytes", actions="optimize")
    r = _by_table(results, "q_bytes", "optimize")
    assert r.after.bytes < r.before.bytes / 2, (
        f"{r.before.bytes} -> {r.after.bytes}: compacting {APPENDS} one-row files should collapse "
        "the per-file Parquet overhead, not just the file count"
    )


def test_optimize_cannot_merge_across_partitions_and_says_so(lake) -> None:
    """The headline finding. A partition is a directory; merging two of them is not a compaction.

    So one file per partition is the *end state* of bin-packing, not a defect it can fix, and a job
    that reported success here would retire the problem from somebody's list while changing nothing.
    """
    assert _files(Layer.BRONZE, "br_probe") == PARTITIONS
    _, results = nb03.maintain(layers="bronze", tables="br_probe", actions="optimize")
    r = _by_table(results, "br_probe", "optimize")
    assert r.status == "applied"
    assert r.files_compacted == 0
    assert r.files_written == 0
    assert r.partitions_optimized == 0
    assert r.after.files == PARTITIONS, "nothing should have moved"
    assert "meta_source_config" in r.advisory, (
        f"advisory must name the actual fix, which is the partition grain, not maintenance: "
        f"{r.advisory!r}"
    )


def test_a_single_file_partition_draws_no_advisory(lake) -> None:
    """Small is not the same as fragmented, and the guard that separates them is `files > 1`.

    `br_card_products` on the real lake is one 5 KiB file in one partition. It satisfies every other
    condition of the advisory — partitioned, nothing compacted, well under the threshold — and it is
    perfectly healthy. Without this guard the loudest finding in the report would be a false one.
    """
    _, results = nb03.maintain(layers="bronze", tables="br_single", actions="optimize")
    r = _by_table(results, "br_single", "optimize")
    assert r.after.files == 1
    assert not r.advisory, f"a one-file table is not fragmented: {r.advisory!r}"


def test_the_advisory_needs_every_one_of_its_four_conditions() -> None:
    """`diagnose` is pure, so the conditions can be varied one at a time rather than constructed.

    Each row below is the advisory case with exactly one condition broken, and each must stay silent:
    an unpartitioned table (OPTIMIZE can always merge what it finds there, so zero means nothing to
    merge), a single file (not fragmented), a compaction that did remove files (it is working), and
    files already above the threshold (that is the goal state).
    """
    def state(files, size, partitioned=True):
        return nb03.TableState(
            layer="bronze", name="t", files=files, bytes=size,
            partition_columns=["ingest_date"] if partitioned else [], properties={},
        )

    threshold = 16 * nb03.MIB
    frag = state(8, 8 * 4096)

    def result(before, after, compacted=0):
        return nb03.ActionResult("optimize", "applied", before, after, files_compacted=compacted)

    assert nb03.diagnose(result(frag, frag), threshold), "the advisory case itself must fire"
    assert not nb03.diagnose(result(frag, state(8, 8 * 4096, partitioned=False)), threshold)
    assert not nb03.diagnose(result(frag, state(1, 4096)), threshold)
    assert not nb03.diagnose(result(frag, frag, compacted=7), threshold)
    assert not nb03.diagnose(result(frag, state(8, 8 * 64 * nb03.MIB)), threshold)
    # A dry run has measured nothing, so it must not diagnose anything either.
    assert "dry run" in nb03.diagnose(
        nb03.ActionResult("optimize", "dry_run", frag, frag,
                          advisory="Delta has no OPTIMIZE dry run; state reported, nothing done"),
        threshold,
    )


# ----------------------------------------------------------------------------------------
# VACUUM
# ----------------------------------------------------------------------------------------

def test_vacuum_removes_stale_files_and_leaves_the_live_snapshot_alone(lake) -> None:
    """`VACUUM` changes what is on disk, never what the current version references.

    A vacuum that moved `numFiles` would be a bug in Delta, so this asserts both halves: the live
    count is untouched and the on-disk parquet count falls. Retention 0 is what makes the reclaim
    visible in one run; at the 168h default the tombstones are inside the window and the two
    operations pipeline across runs instead.
    """
    from pathlib import Path
    _fragment(Layer.QUARANTINE, "q_vac", APPENDS, partitioned=False)
    _, results = nb03.maintain(layers="quarantine", tables="q_vac",
                               actions="optimize,vacuum", vacuum_retain_hours=0)

    opt = _by_table(results, "q_vac", "optimize")
    vac = _by_table(results, "q_vac", "vacuum")

    # The ordering is load-bearing: OPTIMIZE tombstones the files VACUUM then reclaims, so VACUUM
    # must see the table as compaction left it. If these two disagreed, the log would imply the
    # vacuum had deleted live files.
    assert vac.before.files == opt.after.files == 1
    assert vac.after.files == 1, "VACUUM must not change the live snapshot"
    assert vac.after.bytes == vac.before.bytes
    assert vac.stale_files_removed >= APPENDS
    assert vac.retain_hours == 0

    root = Path(lake) / "quarantine" / "q_vac"
    on_disk = [p for p in root.rglob("*.parquet") if "_delta_log" not in p.parts]
    assert len(on_disk) == 1, f"stale files should be gone from storage, found {len(on_disk)}"


def test_a_dry_run_measures_and_changes_nothing(lake) -> None:
    _fragment(Layer.QUARANTINE, "q_dry", APPENDS, partitioned=False)
    before = delta_table(Layer.QUARANTINE, "q_dry").detail().first().asDict()
    _, results = nb03.maintain(layers="quarantine", tables="q_dry", dry_run=True,
                               vacuum_retain_hours=0)
    after = delta_table(Layer.QUARANTINE, "q_dry").detail().first().asDict()

    assert {r.status for r in results} == {"dry_run"}
    assert after["numFiles"] == before["numFiles"] == APPENDS
    assert after["sizeInBytes"] == before["sizeInBytes"]
    # A dry run still reports what a vacuum would find — that is the point of running one.
    assert _by_table(results, "q_dry", "vacuum").stale_files_removed >= 0
    # There is no OPTIMIZE dry run in Delta, and the notebook says so rather than estimating.
    assert "no OPTIMIZE dry run" in _by_table(results, "q_dry", "optimize").advisory


# ----------------------------------------------------------------------------------------
# V-Order, and the log
# ----------------------------------------------------------------------------------------

def test_vorder_is_reported_and_never_set(lake) -> None:
    """OSS Delta rejects `delta.parquet.vorder.enabled` outright — it is not a no-op.

    So a notebook that set it would contain a line that cannot execute on the substrate it was
    verified on. `unset` on every row is the correct local reading, and on Fabric the same column
    would report the property the workspace actually has.
    """
    _, results = nb03.maintain(layers="bronze", tables="br_single", actions="optimize")
    assert {nb03.vorder_status(r.after or r.before) for r in results} == {"unset"}

    from pathlib import Path
    src = (Path(nb03.__file__)).read_text()
    assert "ALTER TABLE" not in src and "setProperty" not in src, (
        "nb_03 must not write table properties: V-Order cannot be declared locally, and on Fabric "
        "no table in this lakehouse is read by Direct Lake, so setting it would be cost without "
        "benefit. The property is read from DESCRIBE DETAIL and reported."
    )

    with pytest.raises(Exception, match="vorder|UNKNOWN_CONFIGURATION"):
        get_spark().sql(
            f"ALTER TABLE delta.`{Path(lake) / 'bronze' / 'br_single'}` "
            f"SET TBLPROPERTIES ('{nb03.VORDER_PROPERTY}' = 'true')"
        )


def test_the_log_uses_the_seeders_schema_rather_than_a_second_copy(lake) -> None:
    """The parity `src/lib/run_log.py` does not have: one schema, and this test reads both ends.

    `meta_run_log` is described twice — a StructType in `nb_99_seed_metadata` and a DDL string in
    `src/lib/run_log.py` — with nothing asserting the two agree. `nb_03` holds no copy at all: it
    projects into whatever the seeded table declares, so the seeder decides both names and order.
    """
    run_id, results = nb03.maintain(layers="quarantine", tables="q_appends", actions="optimize",
                                    run_id="test-log")
    logged = read_table(Layer.META, nb03.LOG_TABLE)
    assert [f.name for f in logged.schema.fields] == [
        f.name for f in nb99.MAINTENANCE_LOG_SCHEMA.fields
    ]
    rows = logged.where(logged.run_id == run_id).collect()
    assert len(rows) == len(results) == 1
    row = rows[0].asDict()
    assert row["layer"] == "quarantine"
    assert row["table_name"] == "q_appends"
    assert row["action"] == "optimize"
    assert row["vorder"] == "unset"
    assert row["duration_sec"] >= 0


def test_a_measurement_the_seeder_has_no_column_for_fails_loudly(lake, monkeypatch) -> None:
    """Dropping a measurement silently is worse than failing the sweep that took it.

    Pointed at a table with a different shape, the write must refuse rather than discard the columns
    that table lacks — which is what would happen on a reseed that added a column here and not there.
    """
    monkeypatch.setattr(nb03, "LOG_TABLE", "meta_watermark")
    with pytest.raises(RuntimeError, match="no column"):
        nb03.maintain(layers="quarantine", tables="q_appends", actions="optimize")


def test_an_unseeded_log_table_is_refused(lake, monkeypatch) -> None:
    monkeypatch.setattr(nb03, "LOG_TABLE", "meta_not_seeded")
    with pytest.raises(RuntimeError, match="make seed"):
        nb03.maintain(layers="quarantine", tables="q_appends", actions="optimize")


def test_the_tables_filter_restricts_the_sweep(lake) -> None:
    _, results = nb03.maintain(layers="bronze", tables="br_single", actions="optimize")
    assert {r.before.name for r in results} == {"br_single"}
    _, none = nb03.maintain(layers="bronze", tables="br_does_not_exist", actions="optimize")
    assert none == []
