"""The one-way dependency graph, enforced rather than described.

`docs/architecture.md` §1 states that every import in this repo points one way, and lists the
consequences that depend on it. Until this file existed, that was a description of what the imports
happened to be on the day it was written — and §7 of that page named this test as the thing that
would turn it into a guarantee. This is that test.

Three of the rules here are the load-bearing ones, and each protects a claim made elsewhere:

1. **`src/runtime/` imports nothing from the project.** The shim is what makes "this is unmodified
   Fabric notebook code" a claim about a diff rather than an intention (`docs/design-decisions.md`
   #8). If it imported anything above it, it would depend on the code it exists to make portable.

2. **No notebook imports another notebook.** On Fabric, a notebook calling a notebook means
   `mssparkutils.notebook.run`, a second Spark session and a string-serialised return value. Shared
   code belongs in `src/lib/`, which is `%run`-free and unit-testable. Each notebook is a Fabric
   Notebook item with one callable entry point and a parameter list, because that is the only
   interface a Notebook activity can offer.

3. **`tools/` imports nothing from `src/`.** This is the one that matters most.
   `fabric_tsql_lint.py` is the only thing carrying the claim that the gold T-SQL would run
   on Fabric (`docs/gold-execution.md`), and a linter that shared code with the harness it
   checks could be satisfied by the same misunderstanding twice. Two independent readers of
   the same `.sql` file cannot collude; one reader asking itself twice can.

**What this test does not check.** It reads `import` statements, so it sees static structure and
nothing else. A lazy import inside a function is still an edge and is still caught — `ast.walk`
does not care about indentation — but a dynamic `importlib.import_module(name)` with a computed name
is invisible to it. `src/lib/gold.py` does use `importlib` to load the interpreted proc modules, and
that is deliberate and out of scope here: it loads `.sql` files, not Python modules. If a future
edge is hidden behind a computed module name, this test will pass and §1 of that document will be
wrong, which is worth knowing about the test rather than discovering about the repo.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# The layers, innermost first. An import may point at its own layer or any layer above it in this
# list (that is, at something more foundational); pointing downwards is the failure.
LAYERS: list[tuple[str, tuple[str, ...]]] = [
    ("src/runtime", ()),
    ("src/lib", ("src.runtime",)),
    ("src/notebooks", ("src.runtime", "src.lib")),
    ("orchestration", ("src.runtime", "src.lib", "src.notebooks")),
    ("dashboard", ("src.runtime", "src.lib", "tools")),
    ("tools", ()),
    # `fabric/` is the deployment layer, and `()` is a stronger claim than it looks. `build_items.py`
    # reads `src/notebooks/*.py` to re-render them in Fabric's own cell format — as *text*, with a
    # parser of its own, never by importing them. That is what lets it run without Spark, without a
    # warehouse, and without the shim; and it is why the one directory in this repo that has never
    # been executed against its target platform cannot drag any of the executed code along with it.
    ("fabric", ()),
]

# Packages that are part of this repo. An import of anything else is a third-party or stdlib import
# and is not this test's business.
INTERNAL = ("src", "orchestration", "tools", "dashboard", "tests", "fabric")

# Directories that hold no source of ours: virtualenvs, caches, and the local lake itself.
SKIP_DIRS = frozenset({".venv", "venv", "_onelake", "__pycache__", "build", ".git", ".ruff_cache",
                       ".pytest_cache", "site-packages"})


def _layer_of(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    for layer, _ in LAYERS:
        if rel.startswith(layer + "/"):
            return layer
    raise AssertionError(f"{rel} is in no declared layer — add it to LAYERS or move the file")


def _internal_imports(path: Path) -> list[tuple[str, int]]:
    """Every internal module this file imports, with the line number, including lazy ones."""
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in INTERNAL:
                    found.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            # `from __future__ import ...` and relative imports both have no internal root here;
            # this repo uses absolute imports throughout, which is why level > 0 is an error below.
            if node.level:
                found.append((f"<relative level {node.level}>", node.lineno))
            elif node.module and node.module.split(".")[0] in INTERNAL:
                found.append((node.module, node.lineno))
    return found


def _modules() -> list[Path]:
    """Every source file in a declared layer — and nothing a layer merely contains.

    The `SKIP_DIRS` filter is load-bearing here rather than tidy. `fabric/build/` holds generated
    `notebook-content.py` files whose imports are copies of the notebooks', and checking those would
    mean asserting the graph twice over the same edges while reporting failures against a path that
    is not in git and cannot be edited. The generator is the file under test; its output is not.
    """
    out: list[Path] = []
    for layer, _ in LAYERS:
        out.extend(
            sorted(
                p
                for p in (ROOT / layer).rglob("*.py")
                if not any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts)
            )
        )
    return out


ALL_MODULES = _modules()
MODULE_IDS = [p.relative_to(ROOT).as_posix() for p in ALL_MODULES]


def test_nothing_sits_outside_the_declared_layers() -> None:
    """A new package has to be placed in the graph deliberately, rather than escape it by default.

    This is the test that keeps the rest of the file honest: without it, adding `src/services/`
    would simply not be checked, and the graph would be enforced only over the parts of the repo
    that existed when it was written. A loose script at the repo root fails here for the same
    reason — being unimportable from anywhere is not the same as being in no layer.

    One exception, and it is asserted rather than assumed. `src/__init__.py` sits *above* every
    layer: it exists so that `src.runtime` is importable, and it belongs to no layer because there
    is no layer at that depth. It is excused only while it stays empty. Code written there would be
    imported by every layer and checked by none, which is the one position in this repo from which
    an upward edge would be invisible to the parametrised test below.
    """
    declared = {layer for layer, _ in LAYERS}
    markers: list[Path] = []
    outside: set[str] = set()
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT)
        if any(part in SKIP_DIRS for part in rel.parts) or rel.parts[0] == "tests":
            continue
        if len(rel.parts) == 1:
            outside.add(rel.name)
            continue
        package = f"{rel.parts[0]}/{rel.parts[1]}" if rel.parts[0] == "src" else rel.parts[0]
        if package in declared:
            continue
        if rel.name == "__init__.py":
            markers.append(path)  # a package marker above every layer; checked for emptiness below
            continue
        outside.add(package)

    assert not outside, (
        f"these are in no declared layer, so nothing checks which way their imports point: "
        f"{sorted(outside)}. Add each to LAYERS in this file with the layers it may depend on."
    )

    non_empty = {
        path.relative_to(ROOT).as_posix(): len(path.read_bytes())
        for path in markers
        if path.read_bytes().strip()
    }
    assert not non_empty, (
        f"these package markers sit above every declared layer and are no longer empty: "
        f"{non_empty}. Whatever is in them is imported by every layer and constrained by none. If "
        f"it is foundational it belongs in src/runtime/; otherwise it belongs in the one "
        f"layer that needs it."
    )


@pytest.mark.parametrize("path", ALL_MODULES, ids=MODULE_IDS)
def test_imports_point_one_way(path: Path) -> None:
    layer = _layer_of(path)
    allowed = dict(LAYERS)[layer]
    own = layer.replace("/", ".")
    for module, lineno in _internal_imports(path):
        assert not module.startswith("<relative"), (
            f"{path.relative_to(ROOT)}:{lineno} uses a relative import. This repo uses absolute "
            f"imports throughout, so that a notebook's imports read the same whether it is run by "
            f"the orchestrator, by pytest, or pasted into a Fabric notebook cell."
        )
        if module == own or module.startswith(own + "."):
            continue  # within its own layer, which every layer is allowed to do
        assert any(module == a or module.startswith(a + ".") for a in allowed), (
            f"{path.relative_to(ROOT)}:{lineno} imports `{module}`, which is not in layer "
            f"`{layer}`'s allowed set {allowed or '()'}. This is the one-way dependency graph in "
            f"docs/architecture.md §1. If the new edge is right, the document is what needs "
            f"changing — and §1 names the property that edge would break."
        )


def test_runtime_shim_depends_on_nothing_in_the_project() -> None:
    """Stated apart from the parametrised test because it is the keystone, not a rule among them.

    `docs/architecture.md` §1 and `docs/design-decisions.md` #8 both rest on this single fact, and a
    failure here should read as "the shim is no longer portable", not as "a lint rule fired".
    """
    offenders = {
        p.relative_to(ROOT).as_posix(): [m for m, _ in _internal_imports(p)]
        for p in sorted((ROOT / "src/runtime").rglob("*.py"))
        if _internal_imports(p)
    }
    assert not offenders, (
        f"src/runtime/ now imports from the project: {offenders}. The shim exists to make the "
        f"transformation code portable to Fabric; once it depends on that code, 'this is "
        f"unmodified Fabric notebook code' stops being a claim anyone can check."
    )


def test_no_notebook_imports_another_notebook() -> None:
    """One Fabric Notebook item, one entry point, no `mssparkutils.notebook.run`."""
    offenders: dict[str, list[str]] = {}
    for p in sorted((ROOT / "src/notebooks").rglob("*.py")):
        cross = [
            m
            for m, _ in _internal_imports(p)
            if m.startswith("src.notebooks") and Path(m.split(".")[-1]).stem != p.stem
        ]
        if cross:
            offenders[p.relative_to(ROOT).as_posix()] = cross
    assert not offenders, (
        f"a notebook imports another notebook: {offenders}. On Fabric that is "
        f"mssparkutils.notebook.run — a second Spark session and a string-serialised return value. "
        f"Shared code belongs in src/lib/."
    )


def test_tools_share_no_code_with_what_they_verify() -> None:
    """The independence that makes the linter's verdict worth anything.

    `docs/gold-execution.md` splits the gold claim in two: the harness in `src/lib/gold.py` proves
    the logic, `tools/fabric_tsql_lint.py` proves the dialect. Those are two verdicts only while
    they are two readers. If the linter imported the harness, a construct the harness mishandled
    could be waved through by a linter asking the harness about it.
    """
    offenders = {
        p.relative_to(ROOT).as_posix(): [m for m, _ in _internal_imports(p) if m.startswith("src")]
        for p in sorted((ROOT / "tools").rglob("*.py"))
        if any(m.startswith("src") for m, _ in _internal_imports(p))
    }
    assert not offenders, (
        f"tools/ now imports from src/: {offenders}. The verification tools are independent "
        f"of the files they check, and that independence is the whole of their evidential value."
    )


def test_each_notebook_exposes_the_entry_point_the_orchestrator_calls() -> None:
    """The interface a Fabric Notebook activity can offer: a name and a parameter list.

    Asserted per notebook rather than as a pattern, because the two orchestrated notebooks have
    specific signatures that `orchestration/run.py` calls by keyword — and a renamed parameter is a
    pipeline that fails at run time on a tenant, where it is expensive to find out.

    Note what this does *not* say: that a notebook has only one public function. Each has several,
    because every step is a plain function a unit test can call — that is the point of rule 2 in
    this module's docstring. What is pinned is the one function the orchestrator names, its exact
    parameter list, and the fact that `main` takes none: parameters reach a notebook through
    `src.runtime.params`, which reads `sys.argv` and the environment. A `main(argv)` would put the
    parameter surface in argv's shape, and a Fabric Notebook activity has no argv to offer.
    """
    expected = {
        "nb_00_generate_landing_data": ("main", set()),
        "nb_01_bronze_ingest": (
            "ingest",
            {"entity", "until_date", "batch_id", "run_id", "force_reload"},
        ),
        "nb_02_silver_transform": (
            "transform",
            {"entity", "run_id", "until_date", "force_reload"},
        ),
        "nb_99_seed_metadata": ("main", set()),
        # Not called by orchestration/run.py, and deliberately: maintenance is driven by the
        # calendar rather than by ingest, so on Fabric it is a Notebook activity in its own
        # scheduled pipeline rather than a stage of pl_master. Pinned here anyway, because the
        # parameter list is still an activity's parameter list and still has to survive a rename.
        "nb_03_table_maintenance": (
            "maintain",
            {"layers", "tables", "actions", "vacuum_retain_hours", "small_file_mib", "dry_run",
             "run_id"},
        ),
    }
    for stem, (func, params) in expected.items():
        path = ROOT / "src/notebooks" / f"{stem}.py"
        tree = ast.parse(path.read_text(), filename=str(path))
        fns = {
            n.name: n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")
        }
        assert func in fns, (
            f"{stem} has no `{func}` — orchestration/run.py calls it by that name, and on Fabric "
            f"it is what the Notebook activity would invoke. Public functions found: "
            f"{sorted(fns)}"
        )
        assert "main" in fns, (
            f"{stem} has no `main` — it is what makes the file runnable as a notebook and as "
            f"`python -m`, and `make run` and the Makefile targets go through it."
        )
        main_args = fns["main"].args
        assert not (*main_args.posonlyargs, *main_args.args, *main_args.kwonlyargs), (
            f"{stem}.main now takes parameters. Parameters belong to `src.runtime.params`, which "
            f"reads sys.argv and the environment, so that the same file works as a notebook cell, "
            f"as a module and as a Fabric Notebook activity without three parameter surfaces."
        )
        args = fns[func].args
        names = {a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
        assert names == params, (
            f"{stem}.{func} takes {sorted(names)}, not {sorted(params)}. Each of these is a Fabric "
            f"Notebook activity parameter and is listed in docs/architecture.md §1, so a "
            f"rename is a three-place change, not one."
        )


def test_the_orchestrator_passes_parameters_the_notebooks_actually_have() -> None:
    """Read from both sides, so a rename cannot be made consistent by editing only the test.

    The test above is a hand-written spec: it says what the signatures should be. This one derives
    both sides from the code — the keywords `orchestration/run.py` passes at its call sites, and the
    parameters the notebooks declare — and so it stays true through a deliberate signature change
    while still catching a rename on one side only. On Fabric these two sides are a pipeline
    activity and a notebook, in separate items, and nothing checks them against each other until
    the activity runs.
    """
    run_py = ROOT / "orchestration/run.py"
    calls = [
        (node.func.attr, {kw.arg for kw in node.keywords}, node.lineno)
        for node in ast.walk(ast.parse(run_py.read_text(), filename=str(run_py)))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"ingest", "transform"}
    ]
    assert {name for name, _, _ in calls} == {"ingest", "transform"}, (
        f"expected orchestration/run.py to call both notebook entry points; found "
        f"{sorted({name for name, _, _ in calls})}"
    )
    declared = {
        node.name: {
            a.arg
            for a in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
        }
        for stem in ("nb_01_bronze_ingest", "nb_02_silver_transform")
        for node in ast.parse((ROOT / "src/notebooks" / f"{stem}.py").read_text()).body
        if isinstance(node, ast.FunctionDef) and node.name in {"ingest", "transform"}
    }
    for name, passed, lineno in calls:
        assert passed <= declared[name], (
            f"orchestration/run.py:{lineno} passes {sorted(passed - declared[name])} to `{name}`, "
            f"which does not declare it. On Fabric this is a Notebook activity parameter that the "
            f"notebook ignores, and the run succeeds with the default."
        )


def test_the_shim_offers_no_sql_escape_hatch() -> None:
    """The shim's surface is narrow on purpose, and the narrowness is a claim in README §2.

    `docs/gold-execution.md` keeps two claims apart: that the logic is right (execution) and that the
    T-SQL would run on Fabric (static analysis). A `sql_exec()` on the shim would collapse them, by
    putting gold's T-SQL behind the same interface as bronze's DataFrame code and inviting a notebook
    to run SQL that no linter ever sees. The T-SQL is executed by `src/lib/gold.py` instead, which is
    above the shim, is a harness rather than a second implementation, and is read independently by
    `tools/fabric_tsql_lint.py`.

    `spark.sql(...)` inside a notebook is not what this forbids and is not reachable from here; what
    is forbidden is the shim growing a substrate-switching SQL entry point, because that is the one
    that would arrive with local and Fabric branches and no linter on either.
    """
    path = ROOT / "src/runtime/context.py"
    tree = ast.parse(path.read_text(), filename=str(path))
    public = {
        n.name
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and not n.name.startswith("_")
    }
    forbidden = {n for n in public if n in {"sql", "sql_exec", "execute", "exec", "query", "run_sql"}}
    assert not forbidden, (
        f"src/runtime/context.py now exposes {sorted(forbidden)}. README §2 states there is no "
        f"sql_exec on the shim and says why: gold's T-SQL is executed by src/lib/gold.py and read "
        f"by tools/fabric_tsql_lint.py, and a SQL entry point on the shim would bypass both. If "
        f"this is deliberate, README §2 and docs/gold-execution.md are what need changing first."
    )

    # The invariant is about the parameter, not the name. A hatch is a shim function that runs SQL
    # its *caller* wrote — that is what arrives with local and Fabric branches no linter reads. A
    # closed statement the shim composes itself from a resolved table name and a number is not one:
    # `vacuum_dry_run` exists because `VACUUM ... DRY RUN` has no Python API, and there is nothing in
    # it for a linter to have missed. Naming alone cannot separate those two, so this checks the
    # signature. Written this way after the name-based rule above flagged a `sql_table_ref` that was
    # a genuine violation for a different reason — it handed a notebook a substrate-specific string —
    # and would equally have passed a rule that only looked for `exec`.
    hatches = {
        n.name: sorted(a.arg for a in (*n.args.posonlyargs, *n.args.args, *n.args.kwonlyargs))
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and not n.name.startswith("_")
        and {a.arg for a in (*n.args.posonlyargs, *n.args.args, *n.args.kwonlyargs)}
        & {"sql", "statement", "stmt", "query", "ddl", "dml"}
    }
    assert not hatches, (
        f"these shim functions take SQL from their caller: {hatches}. Whatever they are called, "
        f"that is the escape hatch this test is about: the statement is written above the shim, "
        f"branches per substrate inside it, and no linter reads either side. The shim may compose a "
        f"closed statement of its own (see vacuum_dry_run); it may not execute one it was handed."
    )


def test_table_ref_is_internal_to_the_shim() -> None:
    """Nothing above the shim is handed a path or a table name, so nothing above it can pin one.

    `table_ref` is the single function that resolves `./_onelake/silver/dim_account` against
    `lh_silver.dim_account`. The four table functions call it; a caller that called it directly would
    hold a substrate-specific string, and the next edit to that caller would be the one that hard-
    codes a location. This is asserted rather than left to review because the failure mode is a
    string that looks harmless locally and is wrong on Fabric.
    """
    offenders: dict[str, list[int]] = {}
    for path in ALL_MODULES:
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("src/runtime/"):
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        hits = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == "table_ref")
                or (isinstance(node.func, ast.Attribute) and node.func.attr == "table_ref")
            )
        ]
        if hits:
            offenders[rel] = hits
    assert not offenders, (
        f"these call table_ref from outside the shim: {offenders}. The caller now holds a "
        f"substrate-specific path or table name, which is the thing src/runtime/context.py exists "
        f"to keep out of the transformation code. Use read_table / write_table / table_exists / "
        f"delta_table, or add the operation to the shim."
    )
