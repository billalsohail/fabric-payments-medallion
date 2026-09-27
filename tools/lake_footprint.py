"""Measure the local lake from its Delta logs, and do the Direct Lake guardrail arithmetic.

`docs/cost-and-capacity.md` is the one page in this repo whose argument is made of measurements
rather than of reasoning, and every number in its §2, §3, §5 and §6 comes from here. That page cites
this script by name so a reader who doubts a figure can regenerate it instead of taking it on trust —
which is the same bargain `tools/extract_dax.py --check` strikes with the TMDL, one step weaker: this
tool reports, it does not enforce. The lake is build output and is not in git, so there is nothing
stable for CI to diff against.

**Why it reads the `_delta_log` rather than the filesystem.** The number Direct Lake cares about is
files in the *current snapshot*, and a Delta directory contains every file any version ever
referenced. Counting `*.parquet` on disk answered a different question than the one the guardrails
ask, and getting that wrong is what the "on disk" column in `--vacuum` exists to make visible: at the
time of writing every bronze table held exactly 2.0x its live file count, because the pipeline ran
twice and nothing ever vacuumed. That ratio is the measured cost of the maintenance notebook named in
`docs/fabric-deployment.md` §9 item 6, and it is invisible to a filesystem walk.

**Why it replays every commit rather than reading the checkpoint.** A Delta checkpoint is a Parquet
file, `pyarrow` is not a dependency of this project, and adding one so a reporting script can skip
some JSON would be a poor trade. Replaying `add`/`remove` from version 0 gives the identical answer
while the JSON commits survive, which they do for `logRetentionDuration` — 30 days by default, and
this lake is rebuilt by `make run`. The replay asserts that version 0 is present and that no version
is missing, so the day that assumption stops holding this fails loudly rather than under-reporting.

**Row counts come from commit statistics, not from the data.** Each `add` carries a `stats` blob with
`numRecords`, so a row count costs no Parquet reads at all. Delta permits `stats` to be absent; a
table missing any is reported with `rows = None` rather than with a plausible-looking undercount.

Usage:

    python tools/lake_footprint.py               # the §2 baseline table
    python tools/lake_footprint.py --partitions  # the §5 bronze small-file profile
    python tools/lake_footprint.py --vacuum      # the §6 live-versus-on-disk ratio
    python tools/lake_footprint.py --guardrails  # the §3 F2 headroom arithmetic
    python tools/lake_footprint.py --all         # all four, in the order the page uses them
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAKE = ROOT / "_onelake"
TMDL_TABLES = ROOT / "semantic-model" / "definition" / "tables"

MIB = 1024 * 1024

# Direct Lake guardrails for the F2/F4/F8 row of the SKU table, from
# learn.microsoft.com/fabric/fundamentals/direct-lake-overview (ms.date 2026-09-02).
#
# Max memory is deliberately absent. That page states it is *not* a guardrail — it is the ceiling on
# how much data can be paged in, so exceeding it degrades rather than fails. Putting it in this dict
# would invite exactly the arithmetic that docs/cost-and-capacity.md §4 exists to correct.
F2_GUARDRAILS = {
    "Max model size on disk": (10 * 1024 * MIB, "bytes"),
    "Rows per table": (300_000_000, "rows"),
    "Parquet files per table": (1_000, "files"),
    "Row groups per table": (1_000, "row groups"),
}


@dataclass
class Table:
    """One Delta table's current snapshot, as its commit log describes it."""

    layer: str
    name: str
    path: Path
    rows: int | None
    live_files: int
    live_bytes: int
    on_disk_files: int
    partitions: int

    @property
    def rows_per_file(self) -> float | None:
        if self.rows is None or not self.live_files:
            return None
        return self.rows / self.live_files

    @property
    def kib_per_file(self) -> float | None:
        if not self.live_files:
            return None
        return self.live_bytes / self.live_files / 1024


def _replay(delta_log: Path) -> tuple[int | None, int, int, set[str]]:
    """Return (rows, files, bytes, partition dirs) for the current snapshot.

    Adds and removes are applied in version order, keyed on the file path exactly as the log records
    it. A `remove` for a path never added is a corrupt log rather than a no-op, so it raises.
    """
    commits = sorted(
        (int(p.stem), p) for p in delta_log.glob("*.json") if p.stem.isdigit()
    )
    if not commits:
        raise AssertionError(f"{delta_log} has no JSON commits to replay")
    versions = [v for v, _ in commits]
    if versions[0] != 0:
        raise AssertionError(
            f"{delta_log} starts at version {versions[0]}, not 0 — commits have been cleaned up, so "
            f"a replay would under-report. Read the checkpoint instead, or rebuild with `make run`."
        )
    missing = sorted(set(range(versions[-1] + 1)) - set(versions))
    if missing:
        raise AssertionError(
            f"{delta_log} is missing version(s) {missing}; the replay would be wrong"
        )

    live: dict[str, tuple[int, int | None]] = {}
    for _, commit in commits:
        for line in commit.read_text().splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if add := action.get("add"):
                stats = add.get("stats")
                rows = json.loads(stats).get("numRecords") if stats else None
                live[add["path"]] = (add["size"], rows)
            elif remove := action.get("remove"):
                if live.pop(remove["path"], None) is None:
                    raise AssertionError(f"{commit.name} removes {remove['path']}, never added")

    counts = [rows for _, rows in live.values()]
    total_rows = None if any(c is None for c in counts) else sum(counts)
    total_bytes = sum(size for size, _ in live.values())
    partitions = {p for path in live if "=" in path and (p := path.rsplit("/", 1)[0])}
    return total_rows, len(live), total_bytes, partitions


def collect() -> list[Table]:
    """Every Delta table under `_onelake/`, in layer then name order."""
    if not LAKE.exists():
        raise SystemExit(f"{LAKE} does not exist — run `make run` first")

    tables: list[Table] = []
    for delta_log in sorted(LAKE.rglob("_delta_log")):
        path = delta_log.parent
        rel = path.relative_to(LAKE).parts
        rows, live_files, live_bytes, partitions = _replay(delta_log)
        on_disk = sum(
            1 for p in path.rglob("*.parquet") if "_delta_log" not in p.relative_to(path).parts
        )
        tables.append(
            Table(
                layer="/".join(rel[:-1]),
                name=rel[-1],
                path=path,
                rows=rows,
                live_files=live_files,
                live_bytes=live_bytes,
                on_disk_files=on_disk,
                partitions=len(partitions),
            )
        )
    return sorted(tables, key=lambda t: (t.layer, t.name))


def _model_tables() -> set[str]:
    """The tables in the semantic model, derived from the TMDL rather than listed here.

    `gold/dbo` holds eleven tables and the model has ten: `dim_fx_rate` is joined in the Warehouse by
    `sp_load_fact_transaction` and never surfaces as a model table, which is also why it has no
    surrogate key. Reading the filenames keeps that difference from needing to be remembered in two
    places — add a table to the model and its footprint appears in `--guardrails` on the next run.
    """
    if not TMDL_TABLES.is_dir():
        raise SystemExit(f"{TMDL_TABLES} does not exist; cannot tell which gold tables are modelled")
    return {p.stem for p in TMDL_TABLES.glob("*.tmdl")}


def _num(value: float | None, fmt: str) -> str:
    return "—" if value is None else format(value, fmt)


def baseline(tables: list[Table]) -> None:
    """The §2 table: what the lake actually holds, per table, live."""
    print("## Measured baseline (live snapshot, from the Delta logs)\n")
    print("| Layer | Table | Rows | Live files | Size | Rows/file |")
    print("|---|---|---|---|---|---|")
    for t in tables:
        print(
            f"| {t.layer} | `{t.name}` | {_num(t.rows, ',')} | {t.live_files} | "
            f"{t.live_bytes / MIB:.2f} MiB | {_num(t.rows_per_file, ',.1f')} |"
        )

    model = _model_tables()
    modelled = [t for t in tables if t.layer == "gold/dbo" and t.name in model]
    rows = sum(t.rows or 0 for t in modelled)
    size = sum(t.live_bytes for t in modelled)
    files = sum(t.live_files for t in modelled)
    fact = next((t for t in modelled if t.name == "fact_transaction"), None)

    print(f"\n{len(modelled)} of the gold tables are in the semantic model:")
    print(f"  rows          {rows:,}")
    print(f"  size          {size / MIB:.2f} MiB")
    print(f"  live files    {files}, max {max(t.live_files for t in modelled)} in any one table")
    if fact and fact.rows:
        print(f"  gold fact     {fact.live_bytes / fact.rows:.1f} bytes/row compressed")


def partitions(tables: list[Table]) -> None:
    """The §5 profile: file count tracks partitions, and partitions track days."""
    print("## Bronze small-file profile\n")
    print("| Table | Rows | Live files | Partitions | Rows/file | KiB/file |")
    print("|---|---|---|---|---|---|")
    for t in (t for t in tables if t.layer == "bronze"):
        print(
            f"| `{t.name}` | {_num(t.rows, ',')} | {t.live_files} | {t.partitions or '—'} | "
            f"{_num(t.rows_per_file, ',.1f')} | {_num(t.kib_per_file, ',.0f')} |"
        )
    one_per = [t for t in tables if t.layer == "bronze" and t.partitions == t.live_files]
    print(
        f"\n{len(one_per)} of the bronze tables hold exactly one live file per partition, so their "
        f"file\ncount is a function of days ingested and not of rows. See "
        f"docs/cost-and-capacity.md §5."
    )


def vacuum(tables: list[Table]) -> None:
    """The §6 number: what the missing maintenance notebook costs in storage, today."""
    print("## Live versus on disk (what a VACUUM would reclaim)\n")
    print("| Layer | Table | Live files | On disk | Ratio |")
    print("|---|---|---|---|---|")
    for t in tables:
        ratio = t.on_disk_files / t.live_files if t.live_files else 0.0
        print(f"| {t.layer} | `{t.name}` | {t.live_files} | {t.on_disk_files} | {ratio:.1f}x |")
    live = sum(t.live_files for t in tables)
    disk = sum(t.on_disk_files for t in tables)
    print(
        f"\nWhole lake: {live} live, {disk} on disk, {disk / live:.2f}x. Every file above the live "
        f"count is\na tombstoned file no snapshot references, retained because this repo never runs "
        f"VACUUM\n(docs/fabric-deployment.md §9 item 6)."
    )


def guardrails(tables: list[Table]) -> None:
    """The §3 arithmetic: which guardrail binds first, and at what multiple of today's data."""
    model = _model_tables()
    modelled = [t for t in tables if t.layer == "gold/dbo" and t.name in model]
    if not modelled:
        raise SystemExit("no modelled gold tables found; has the gold load run?")

    size = sum(t.live_bytes for t in modelled)
    widest = max(modelled, key=lambda t: t.rows or 0)
    most_files = max(modelled, key=lambda t: t.live_files)

    measured = {
        "Max model size on disk": (size, f"{size / MIB:.2f} MiB"),
        "Rows per table": (widest.rows or 0, f"{widest.rows:,} (`{widest.name}`)"),
        "Parquet files per table": (most_files.live_files, f"{most_files.live_files} (worst table)"),
    }

    print("## F2 Direct Lake headroom\n")
    print("| Guardrail | F2 limit | Measured | Headroom |")
    print("|---|---|---|---|")
    for name, (limit, unit) in F2_GUARDRAILS.items():
        if name not in measured:
            print(f"| {name} | {limit:,} {unit} | ~1 per file | not volume-driven; see §3 |")
            continue
        value, shown = measured[name]
        # The file guardrail gets its multiple *and* a caveat. Gold is unpartitioned and its load
        # rewrites whole tables, so file count is a function of writes rather than of volume, and
        # reading 500x as "500x the data" is the misreading docs/cost-and-capacity.md §3 corrects.
        caveat = " (not volume-driven)" if name == "Parquet files per table" else ""
        print(f"| {name} | {limit:,} {unit} | {shown} | {limit / value:,.0f}x{caveat} |")

    binding = min(
        (F2_GUARDRAILS[n][0] / v, n)
        for n, (v, _) in measured.items()
        if n != "Parquet files per table"
    )
    print(f"\nFirst to bind on volume: {binding[1]}, at ~{binding[0]:,.0f}x today's data.")
    for mult in (100, 1_000):
        print(
            f"  at {mult:>5}x   model {size * mult / MIB:>10,.0f} MiB "
            f"({size * mult / F2_GUARDRAILS['Max model size on disk'][0]:>5.1%} of 10 GB)   "
            f"fact {(widest.rows or 0) * mult:>12,} rows "
            f"({(widest.rows or 0) * mult / F2_GUARDRAILS['Rows per table'][0]:>5.1%} of 300M)"
        )
    print(
        "\nGuardrails: learn.microsoft.com/fabric/fundamentals/direct-lake-overview "
        "(ms.date 2026-09-02).\nMax memory is not among them and is not a guardrail — see "
        "docs/cost-and-capacity.md §4."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--partitions", action="store_true", help="the bronze small-file profile")
    parser.add_argument("--vacuum", action="store_true", help="live versus on-disk file counts")
    parser.add_argument("--guardrails", action="store_true", help="F2 Direct Lake headroom")
    parser.add_argument("--all", action="store_true", help="every section, in page order")
    args = parser.parse_args()

    tables = collect()
    wanted = [
        (True, baseline) if args.all or not (args.partitions or args.vacuum or args.guardrails)
        else (False, baseline),
        (args.all or args.guardrails, guardrails),
        (args.all or args.partitions, partitions),
        (args.all or args.vacuum, vacuum),
    ]
    first = True
    for enabled, section in wanted:
        if not enabled:
            continue
        if not first:
            print("\n---\n")
        section(tables)
        first = False


if __name__ == "__main__":
    main()
