"""The `fabric/` deployment layer's own claims, asserted where they can be.

`fabric/README.md` says plainly that nothing in that directory has been run against a tenant, and no
test here can change that. What these tests *can* do is close the gap between the two kinds of wrong
a hand-authored deployment layer can be. One kind needs a tenant to find: a marker syntax Fabric does
not accept, a `definition.pbism` missing a property. The other kind does not, and is the kind that
actually happens — a duplicated `logicalId`, a directory whose name stopped matching the
`displayName` inside it, a `type` that lost its capital letter, a variable the docs describe and the
JSON does not have. Every test below is in that second category.

The load-bearing ones:

1. **`logicalId` uniqueness.** Learn is explicit that a workspace cannot hold two items with the same
   one, and that copying an item directory means changing both the `logicalId` and the `displayName`
   (`fabric/cicd/git-integration/source-code-format`, ms.date 2025-12-15). Copying a directory is
   exactly how a thirteenth item would get added, so this is the failure mode most likely to occur.

2. **`type` casing.** That same page states the type is case-sensitive, and it is the one field here
   whose wrong value produces a deploy that fails at the tenant rather than at the keyboard.

3. **§6's table and `variables.json` agree.** `docs/fabric-deployment.md` §6 documents the Variable
   Library as a markdown table and `fabric/items/vl_payments.VariableLibrary/` implements it as JSON.
   That is a fact stated twice, so it needs something that fails when the two disagree — the same
   role `tools/extract_dax.py --check` plays for `measures.dax`.

4. **The §2 inventory and the descriptor set agree.** The deployment doc's item inventory is the
   page a reader trusts to say what exists; the descriptors are what would actually be deployed.

5. **The notebook render round-trips.** Parsing a generated `notebook-content.py` back out must
   recover the cells that went in. This proves the renderer is self-consistent. It does **not** prove
   Fabric would accept it, for the reason `fabric/README.md` §3 gives, and calling that proof would
   be the one dishonest sentence available in this file.

**What is deliberately asserted absent.** There is no `wh_gold.Warehouse` descriptor, no
`pipeline-content.json` and no `parameter.yml`, and each absence is a decision documented in
`docs/fabric-deployment.md` §4 or `fabric/README.md` §4. A decision nothing checks is a decision that
erodes, so the absences are tested as positively as the presences are.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ITEMS = ROOT / "fabric" / "items"
NOTEBOOKS = ROOT / "src" / "notebooks"
DEPLOY_DOC = ROOT / "docs" / "fabric-deployment.md"
VL = ITEMS / "vl_payments.VariableLibrary"

PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json"


def _load_builder():
    """Import `fabric/build_items.py` by path: `fabric/` is deployment code, not a package."""
    spec = importlib.util.spec_from_file_location("build_items", ROOT / "fabric" / "build_items.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Registered before execution because `@dataclass` resolves annotations through `sys.modules`,
    # and a module absent from it raises rather than degrading.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_items = _load_builder()


@pytest.fixture(scope="module")
def items():
    return build_items.read_items()


def test_every_descriptor_is_schema_2_0_with_the_documented_keys(items):
    """`.platform` shape, from source-code-format (ms.date 2025-12-15)."""
    for item in items:
        spec = json.loads((item.directory / ".platform").read_text())
        assert spec["version"] == "2.0", item.dir_name
        assert spec["$schema"] == PLATFORM_SCHEMA, item.dir_name
        assert set(spec) == {"$schema", "version", "metadata", "config"}, item.dir_name
        assert set(spec["metadata"]) == {"type", "displayName", "description"}, item.dir_name
        assert set(spec["config"]) == {"logicalId"}, item.dir_name
        assert spec["metadata"]["description"].strip(), f"{item.dir_name} has an empty description"


def test_logical_ids_are_unique_and_well_formed(items):
    """A workspace cannot hold two items with the same logicalId, and a copied directory would."""
    ids = [i.logical_id for i in items]
    assert len(set(ids)) == len(ids), f"duplicate logicalId among {[i.dir_name for i in items]}"
    for item in items:
        assert re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", item.logical_id
        ), f"{item.dir_name}: {item.logical_id!r} is not a lowercase GUID"


def test_directory_name_agrees_with_the_descriptor_inside_it(items):
    """`{display name}.{public facing type}` — and `type` is case-sensitive."""
    for item in items:
        assert item.directory.name == f"{item.display_name}.{item.type}", item.directory.name
        assert item.type in build_items.BUILDERS, f"{item.type!r}: unknown type, or wrong casing"
        assert item.type[0].isupper(), f"{item.type!r} is not in Fabric's casing"


def test_a_v2_directory_does_not_also_carry_the_v1_system_files(items):
    """source-code-format: a schema-2.0 directory must not hold the v1 metadata/config files."""
    for item in items:
        for legacy in ("item.metadata.json", "item.config.json"):
            assert not (item.directory / legacy).exists(), f"{item.dir_name} has {legacy}"


def test_every_notebook_descriptor_names_a_real_notebook(items):
    """A Notebook whose displayName does not name a file in src/notebooks/ is a typo."""
    for item in (i for i in items if i.type == "Notebook"):
        assert (NOTEBOOKS / f"{item.display_name}.py").is_file(), item.display_name
    described = {i.display_name for i in items if i.type == "Notebook"}
    on_disk = {p.stem for p in NOTEBOOKS.glob("nb_*.py")}
    assert described == on_disk, f"notebooks without descriptors: {sorted(on_disk - described)}"


# --- the absences, asserted as positively as the presences -------------------------------------


def test_the_warehouse_is_deliberately_not_an_item(items):
    """§4: git integration stores a warehouse as a DacFx project, so this repo deploys it by
    executing its own scripts. An item directory here would quietly reverse that decision."""
    assert not any(i.type == "Warehouse" for i in items)
    assert not (ITEMS / "wh_gold.Warehouse").exists()
    assert "Warehouse" not in build_items.BUILDERS, "a builder for it would invite the directory"
    assert (ROOT / "src" / "warehouse" / "ddl").is_dir(), "the authoritative scripts must still exist"
    assert list((ROOT / "src" / "warehouse" / "procs").glob("*.sql")), "and so must the procs"


def test_the_pipeline_is_an_item_without_a_body(items):
    """§4: the item is real; synthesising its activity JSON is refused."""
    pipeline = next(i for i in items if i.type == "DataPipeline")
    assert build_items.BUILDERS["DataPipeline"] is build_items.refuse_pipeline
    assert not (pipeline.directory / "pipeline-content.json").exists()
    assert (ROOT / "orchestration" / "run.py").is_file(), "§7 points at this file instead"


def test_there_is_no_parameter_yml():
    """fabric/README.md §4: nothing differs per stage inside an item definition, so nothing to
    substitute. A parameter file appearing without that changing is a contradiction."""
    assert not list((ROOT / "fabric").glob("**/parameter.y*ml"))


# --- the variable library, against the table that describes it ---------------------------------


def _doc_variable_table() -> dict[str, dict[str, str]]:
    """Parse §6's table out of the deployment doc: {variable: {stage: value}}."""
    text = DEPLOY_DOC.read_text()
    section = text.split("## 6. Variable library")[1].split("\n## ")[0]
    rows = {}
    for line in section.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")] if line.startswith("|") else []
        if len(cells) != 5 or cells[0] in ("Variable", "---"):
            continue
        name = cells[0].strip("`")
        rows[name] = {"dev": cells[1], "test": cells[2], "prod": cells[3]}
    assert rows, "§6's table did not parse; the test is now the thing that is wrong"
    return rows


def _documented_value(cell: str) -> str:
    """One §6 table cell as the value it denotes.

    The table writes values in backticks, and prod's `Scale` is the two-character literal `""`
    standing for the empty string — the notation the doc needs so the cell is not blank and the
    reader can see that a value is deliberately present and empty.
    """
    value = cell.strip().strip("`")
    return "" if value == '""' else value


def _vl() -> tuple[dict, dict, dict, dict]:
    read = lambda p: json.loads((VL / p).read_text())  # noqa: E731
    return (
        read("variables.json"),
        read("settings.json"),
        read("valueSets/test.json"),
        read("valueSets/prod.json"),
    )


def test_the_variable_library_has_exactly_the_documented_files():
    """variable-library-cicd (ms.date 2025-12-15): variables.json and settings.json are required,
    valueSets/ is optional. There is no dev.json, because the default set *is* dev."""
    present = sorted(p.relative_to(VL).as_posix() for p in VL.rglob("*") if p.is_file())
    assert present == [
        ".platform",
        "settings.json",
        "valueSets/prod.json",
        "valueSets/test.json",
        "variables.json",
    ]


def test_variable_defaults_are_the_dev_column_of_the_documented_table():
    documented = _doc_variable_table()
    variables, _, _, _ = _vl()
    defaults = {v["name"]: v["value"] for v in variables["variables"]}
    assert set(defaults) == set(documented), (
        f"§6 documents {sorted(set(documented) - set(defaults))} that the JSON lacks, and the JSON "
        f"has {sorted(set(defaults) - set(documented))} that §6 does not document"
    )
    for name, stages in documented.items():
        expected = _documented_value(stages["dev"])
        assert str(defaults[name]) == expected, f"{name}: default {defaults[name]!r} != dev {expected!r}"


def test_a_value_set_overrides_only_what_actually_differs():
    """A value set contains only non-default values. Repeating a default would be a second copy of
    it — the thing this repo's generated artefacts exist to avoid."""
    documented_rows = _doc_variable_table()
    variables, _, test_set, prod_set = _vl()
    defaults = {v["name"]: v["value"] for v in variables["variables"]}
    for stage, value_set in (("test", test_set), ("prod", prod_set)):
        assert value_set["name"] == stage
        overridden = {o["name"]: o["value"] for o in value_set["variableOverrides"]}
        differs = {n for n, s in documented_rows.items() if s[stage] != s["dev"]}
        assert set(overridden) == differs, f"{stage}: overrides {set(overridden)}, §6 differs on {differs}"
        for name, value in overridden.items():
            assert value != defaults[name], f"{stage}.{name} overrides with the default value"
            documented = _documented_value(documented_rows[name][stage])
            assert str(value) == documented, (
                f"{stage}.{name} is {value!r} in the value set and {documented!r} in §6's table"
            )


def test_prod_disables_the_generator_with_a_value_nb_00_rejects():
    """§6 used to say `Scale` is *unset* in prod, which the format cannot express: every variable
    needs a default and a value set can only override. The empty string is the honest encoding —
    nb_00 already raises on it, so prod is the stage where the generator cannot start."""
    _, _, _, prod_set = _vl()
    scale = next(o for o in prod_set["variableOverrides"] if o["name"] == "Scale")
    assert scale["value"] == ""

    source = (NOTEBOOKS / "nb_00_generate_landing_data.py").read_text()
    assert "if scale not in SCALES:" in source and "raise ValueError" in source, (
        "nb_00's scale guard is what makes an empty Scale safe; if it has gone, prod's value set is "
        "no longer a refusal and §6 needs rewriting again"
    )
    assert '"scale": ""' not in source, "nb_00 must not default to the value prod uses to disable it"


def test_value_set_order_lists_every_value_set():
    variables, settings, _, _ = _vl()
    on_disk = sorted(p.stem for p in (VL / "valueSets").glob("*.json"))
    assert sorted(settings["valueSetsOrder"]) == on_disk
    assert len(variables["variables"]) <= 1_000, "documented Variable Library limit"
    for v in variables["variables"]:
        assert v["type"] in {"Boolean", "DateTime", "Number", "Integer", "String", "ItemReference"}
        assert len(v.get("note", "")) <= 2_048, f"{v['name']}'s note exceeds the documented limit"


# --- the inventory in §2, and the render ---------------------------------------------------------


def test_the_documented_inventory_matches_the_descriptors(items):
    """§2 is the page a reader trusts for what exists; the descriptors are what would deploy."""
    section = DEPLOY_DOC.read_text().split("## 2. ")[1].split("\n## ")[0]
    mentioned = set(re.findall(r"^\| `([a-z0-9_]+)` \|", section, flags=re.M))
    described = {i.display_name for i in items}
    assert described <= mentioned, f"undocumented items: {sorted(described - mentioned)}"
    # The reverse is not equality: §2 lists wh_gold, which is deployed imperatively, and two items
    # it marks "Does not exist". Those are named in §4 and in fabric/README.md §4.
    assert mentioned - described == {"wh_gold", "rpt_payments", "env_payments"}, sorted(
        mentioned - described
    )


@pytest.mark.parametrize("notebook", sorted(p.name for p in NOTEBOOKS.glob("nb_*.py")))
def test_the_render_round_trips_back_to_the_same_cells(notebook):
    """Self-consistency, not correctness: fabric/README.md §3 is explicit about the difference."""
    source = (NOTEBOOKS / notebook).read_text()
    cells = build_items.parse_percent_cells(source)
    assert cells, notebook

    rendered = build_items.render_notebook(cells)
    markers = build_items._FABRIC_MARKERS
    assert rendered.startswith(markers["header"])
    assert rendered.count(markers["cell"]) == sum(1 for c in cells if c.kind == "code")
    assert rendered.count(markers["markdown"]) == sum(1 for c in cells if c.kind == "markdown")
    # One notebook-level metadata block, plus one per code cell.
    assert rendered.count(markers["metadata"]) == 1 + sum(1 for c in cells if c.kind == "code")

    # Cell output is never committed, so no rendered line may look like one.
    assert "# OUTPUT" not in rendered

    # Every code cell's body survives verbatim, in order. This is the property that would break if
    # the renderer ever started transforming code rather than relocating it.
    position = 0
    for cell in (c for c in cells if c.kind == "code"):
        body = "\n".join(cell.body)
        found = rendered.find(body, position)
        assert found != -1, f"{notebook}: a code cell did not survive the render"
        position = found + len(body)

    params = [c for c in cells if c.parameters]
    assert len(params) <= 1, f"{notebook} has {len(params)} parameter cells"
    if params:
        assert '"tags": [' in rendered and '"parameters"' in rendered


def test_the_marker_constants_are_the_disclosed_unverified_part():
    """fabric/README.md §3 says these are the one format not traceable to a Learn page. That claim
    is only true while they stay in one place — a second copy elsewhere would make the disclosure
    false, and a reader who trusted it would be misled by a document, not by a bug."""
    markers = set(build_items._FABRIC_MARKERS.values())
    readme = (ROOT / "fabric" / "README.md").read_text()
    for marker in markers:
        assert marker in readme or marker.strip() in readme, f"{marker!r} is not disclosed"
    builder_source = (ROOT / "fabric" / "build_items.py").read_text()
    for marker in markers:
        assert builder_source.count(f'"{marker}"') == 1, f"{marker!r} appears more than once"
