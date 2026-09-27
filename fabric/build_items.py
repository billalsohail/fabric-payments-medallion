"""Render `fabric/items/` into the layout Fabric git integration expects — UNVALIDATED.

**Nothing in `fabric/` has been round-tripped through a tenant.** This script has been run; what it
produces has never been synced. Read `fabric/README.md` before trusting a byte of it.

**Why this is a build step rather than a directory of committed files.** Fabric git integration
stores a notebook as `notebook-content.py` inside `<name>.Notebook/`, and a semantic model as a
`definition/` folder of TMDL. This repo already holds both of those things — in `src/notebooks/` as
jupytext percent-format Python, and in `semantic-model/definition/` as TMDL — and committing a second
copy under `fabric/items/` would create five notebook pairs and eleven TMDL pairs that nothing keeps
in agreement. A fact stated twice is a fact that can disagree with itself, so the git-integration
layout is treated as a **deployment artefact**: generated into `fabric/build/`, which is gitignored,
by the same argument that makes `semantic-model/measures.dax` generated from the TMDL rather than
hand-maintained beside it.

What *is* committed is the part that cannot be derived: the `.platform` descriptors, whose
`logicalId` GUIDs must be stable across deploys and are not computable from anything, and the
Variable Library, whose JSON is the only expression of `docs/fabric-deployment.md` §6 that a tenant
would read.

**The one format here that is not traceable to a Microsoft Learn page.** Learn documents that a
PySpark notebook is stored as `notebook-content.py` and that the file keeps notebook metadata,
markdown cells and code cells "as separate sections"
(`learn.microsoft.com/fabric/data-engineering/notebook-source-control-deployment`, ms.date
2026-03-05) — but it never prints the delimiter syntax that separates those sections. It shows it
only inside a screenshot. The marker strings in `_FABRIC_MARKERS` below are therefore the single part
of this deployment layer I could not read off a page, and they are isolated in one constant so that
the first real export from a tenant corrects them in one place. `tests/test_fabric_items.py` asserts
the round trip is self-consistent, which is a weaker claim than correct and is the strongest claim
available without a tenant.

Usage:

    python fabric/build_items.py            # render into fabric/build/
    python fabric/build_items.py --list     # say what would be built, and what is refused
"""

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ITEMS = ROOT / "fabric" / "items"
BUILD = ROOT / "fabric" / "build"
NOTEBOOKS = ROOT / "src" / "notebooks"
SEMANTIC_MODEL = ROOT / "semantic-model"

# The unverifiable part. See the module docstring: Learn shows these only in a screenshot.
_FABRIC_MARKERS = {
    "header": "# Fabric notebook source",
    "metadata": "# METADATA ********************",
    "cell": "# CELL ********************",
    "markdown": "# MARKDOWN ********************",
    "meta_prefix": "# META ",
}

# Notebook-level metadata. The default lakehouse is deliberately absent: binding one requires a
# workspace GUID and a lakehouse GUID that do not exist until the items do, and `src/runtime/
# context.py` names tables two-part (`lh_bronze.transactions`) precisely so that no notebook depends
# on *which* attached lakehouse is default. Attaching them at all is still a tenant-side step, and it
# is step 4 of the §1 runbook rather than a line in this file.
_KERNEL_META = {
    "kernel_info": {"name": "synapse_pyspark"},
    "language_info": {"name": "python"},
}
_CELL_META = {"language": "python", "language_group": "synapse_pyspark"}

PBISM_VERSION = "4.0"  # 4.0+ is what permits a TMDL `definition/` folder to be the definition:
# learn.microsoft.com/power-bi/developer/projects/projects-dataset (ms.date 2025-12-15). That page
# names `version` and no other required property, so this file carries `version` and nothing else.


@dataclass(frozen=True)
class Cell:
    """One jupytext percent cell: markdown prose, or code, possibly tagged as parameters."""

    kind: str  # "markdown" | "code"
    parameters: bool
    body: list[str]


@dataclass(frozen=True)
class Item:
    """A Fabric item, as its committed `.platform` describes it."""

    directory: Path
    type: str
    display_name: str
    logical_id: str

    @property
    def dir_name(self) -> str:
        return f"{self.display_name}.{self.type}"


def read_items() -> list[Item]:
    """Every item in `fabric/items/`, discovered from its descriptor rather than from a list here.

    The descriptor set is the inventory: add a directory with a `.platform` and it is built, which is
    what lets `tests/test_fabric_items.py` compare this against `docs/fabric-deployment.md` §2
    without either side holding a hand-written copy of the other.
    """
    items = []
    for platform in sorted(ITEMS.glob("*/.platform")):
        spec = json.loads(platform.read_text())
        meta, config = spec["metadata"], spec["config"]
        items.append(
            Item(
                directory=platform.parent,
                type=meta["type"],
                display_name=meta["displayName"],
                logical_id=config["logicalId"],
            )
        )
    if not items:
        raise SystemExit(f"{ITEMS} holds no .platform descriptors; nothing to build")
    return items


def parse_percent_cells(source: str) -> list[Cell]:
    """Split a jupytext percent-format notebook into cells.

    Only the three marker forms this repo actually uses are accepted — `# %%`, `# %% [markdown]` and
    `# %% tags=["parameters"]`. A fourth form is a silent behaviour change in the rendered notebook,
    so it raises instead of being guessed at.
    """
    cells: list[Cell] = []
    kind, parameters, body = None, False, []

    def flush() -> None:
        if kind is None:
            return
        while body and not body[-1].strip():
            body.pop()
        cells.append(Cell(kind=kind, parameters=parameters, body=list(body)))

    for lineno, line in enumerate(source.splitlines(), start=1):
        if line.startswith("# %%"):
            flush()
            marker = line.rstrip()
            body = []
            if marker == "# %%":
                kind, parameters = "code", False
            elif marker == "# %% [markdown]":
                kind, parameters = "markdown", False
            elif marker == '# %% tags=["parameters"]':
                kind, parameters = "code", True
            else:
                raise ValueError(
                    f"line {lineno}: unrecognised percent marker {marker!r}. Add it to "
                    f"parse_percent_cells deliberately; do not let it fall through as code."
                )
            continue
        if kind is None:
            if line.strip():
                raise ValueError(f"line {lineno}: content before the first percent marker: {line!r}")
            continue
        body.append(line)
    flush()
    return cells


def render_notebook(cells: list[Cell]) -> str:
    """Render cells in the `notebook-content.py` shape. The markers are the unverified part."""
    m = _FABRIC_MARKERS
    out = [m["header"], ""]

    def meta_block(payload: dict[str, object]) -> None:
        out.append(m["metadata"])
        out.append("")
        for line in json.dumps(payload, indent=2).splitlines():
            out.append(f"{m['meta_prefix']}{line}".rstrip())
        out.append("")

    meta_block(_KERNEL_META)
    for cell in cells:
        if cell.kind == "markdown":
            out.append(m["markdown"])
            out.append("")
            # A jupytext markdown cell is already `# `-prefixed prose, which is the same shape a
            # Fabric MARKDOWN section uses, so this is a copy rather than a translation.
            out.extend(cell.body)
            out.append("")
            continue
        out.append(m["cell"])
        out.append("")
        out.extend(cell.body)
        out.append("")
        cell_meta = dict(_CELL_META)
        if cell.parameters:
            cell_meta["tags"] = ["parameters"]
        meta_block(cell_meta)
    return "\n".join(out).rstrip() + "\n"


def build_notebook(item: Item, target: Path) -> list[str]:
    source = NOTEBOOKS / f"{item.display_name}.py"
    if not source.is_file():
        raise SystemExit(
            f"{item.dir_name} has no source: expected {source.relative_to(ROOT)}. A Notebook "
            f"descriptor whose displayName does not name a file in src/notebooks/ is a typo, not a "
            f"notebook."
        )
    cells = parse_percent_cells(source.read_text())
    (target / "notebook-content.py").write_text(render_notebook(cells))
    return [f"notebook-content.py ({len(cells)} cells from {source.relative_to(ROOT)})"]


def build_semantic_model(item: Item, target: Path) -> list[str]:
    definition = SEMANTIC_MODEL / "definition"
    if not definition.is_dir():
        raise SystemExit(f"{definition.relative_to(ROOT)} does not exist; cannot build {item.dir_name}")
    (target / "definition.pbism").write_text(
        json.dumps({"version": PBISM_VERSION}, indent=2) + "\n"
    )
    shutil.copytree(definition, target / "definition")
    tmdl = sorted(p.relative_to(definition).as_posix() for p in definition.rglob("*.tmdl"))
    return [f"definition.pbism (version {PBISM_VERSION})", f"definition/ ({len(tmdl)} TMDL files)"]


def build_verbatim(item: Item, target: Path) -> list[str]:
    """Copy an item whose definition files are committed rather than generated.

    The Variable Library is the only item in this class. Its JSON *is* the source — there is nothing
    in the repo it could be derived from, because `docs/fabric-deployment.md` §6 is a table in prose
    and a table in prose is documentation, not a definition.
    """
    copied = []
    for path in sorted(item.directory.rglob("*")):
        if path.name == ".platform" or path.is_dir():
            continue
        rel = path.relative_to(item.directory)
        (target / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target / rel)
        copied.append(rel.as_posix())
    return copied


def build_descriptor_only(item: Item, target: Path) -> list[str]:
    """A Lakehouse. Its `.platform` is the whole of its git representation.

    A lakehouse's contents are tables and files — data, not definition — so there is no definition
    file to generate and its absence here is correct rather than unfinished.
    """
    return []


def refuse_pipeline(item: Item, target: Path) -> list[str]:
    """A DataPipeline, deliberately built without its definition.

    `pipeline-content.json` is the one file in this layer I will not synthesise. Every other format
    here I could read off a Learn page and render from something this repo already contains; a
    pipeline definition is several hundred lines of activity JSON with typed dependency edges, and
    hand-writing it would produce a file that looks authoritative, has never parsed, and would be
    the single most misleading artefact in the repository. `orchestration/run.py` is the honest
    statement of what `pl_master` does, and §7 points at it for exactly this reason.

    The descriptor is still built, because the *item* is real and step 7 of the §1 runbook creates it
    in the tenant. What is missing is its body, and that is a disclosed gap.
    """
    return ["(no pipeline-content.json — synthesising one is refused; see the docstring)"]


BUILDERS = {
    "Lakehouse": build_descriptor_only,
    "Notebook": build_notebook,
    "SemanticModel": build_semantic_model,
    "DataPipeline": refuse_pipeline,
    "VariableLibrary": build_verbatim,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--list", action="store_true", help="say what would be built, build nothing")
    args = parser.parse_args()

    items = read_items()
    unknown = sorted({i.type for i in items} - BUILDERS.keys())
    if unknown:
        raise SystemExit(
            f"no builder for item type(s) {unknown}. Fabric's `type` is case-sensitive "
            f"(source-code-format, ms.date 2025-12-15), so check the casing before adding one."
        )

    if args.list:
        for item in items:
            print(f"{item.dir_name:<42} {BUILDERS[item.type].__name__}")
        print(f"\n{len(items)} items. No wh_gold.Warehouse: §4 deploys the warehouse by executing")
        print("src/warehouse/**.sql, because git integration would store it as a DacFx project.")
        return

    if BUILD.exists():
        shutil.rmtree(BUILD)
    for item in items:
        target = BUILD / item.dir_name
        target.mkdir(parents=True)
        shutil.copy2(item.directory / ".platform", target / ".platform")
        extra = BUILDERS[item.type](item, target)
        print(f"{item.dir_name}")
        for line in [".platform", *extra]:
            print(f"    {line}")
    print(f"\n{len(items)} items into {BUILD.relative_to(ROOT)}/ — UNVALIDATED, see fabric/README.md")


if __name__ == "__main__":
    main()
