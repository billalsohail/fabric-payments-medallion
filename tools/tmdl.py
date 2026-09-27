"""A small TMDL reader, for tests rather than for round-tripping.

TMDL is the text format Fabric's git integration stores a semantic model in, and
`semantic-model/` is authored in it directly so the model is reviewable as code. Nothing in this
repo can *execute* that model — there is no tenant and no Analysis Services instance — so the
question this module exists to answer is the next best one: **is the model internally consistent,
and consistent with the warehouse it reads?**

That is a parsing problem, so this is a parser. It is deliberately not a complete TMDL
implementation:

  * It reads the subset `semantic-model/` actually uses, and raises on a line shape it does not
    recognise rather than skipping it. A silent skip in a parser that backs a test is worse than no
    test, because the test keeps passing while checking less.
  * It does not attempt to validate TMDL itself. A file this module parses cleanly may still be
    rejected by Fabric on import — see the UNVALIDATED note in `semantic-model/README.md`. What it
    proves is that the model's *references* resolve: relationships to declared foreign keys,
    columns to warehouse columns, measures to columns and to other measures.
  * It does not evaluate DAX. `measure_column_refs` finds `Table[column]` references
    lexically, which is enough to catch a renamed or misspelled column and not enough to catch a
    logic error. The logic is argued for in the comments beside each measure and checked against
    SQL in `tests/test_gold_recon.py`.

The format is indentation-significant with tabs, which is the one thing worth knowing before
reading `_parse`.
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

MODEL_DIR = Path(__file__).resolve().parent.parent / "semantic-model" / "definition"

# `key: value`, where the key is a bare identifier. Checked before the `=` form because a property
# value may itself contain `=` (an annotation's JSON, a format string).
_PROP = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$")

# `kind name = value` / `kind name` / `kind`. The name may be single-quoted and contain spaces,
# which is how every measure is written.
_DECL = re.compile(
    r"""^
    (?P<kind>[A-Za-z_][A-Za-z0-9_]*)
    (?:\s+(?P<name>'[^']*'|[^\s=]+))?
    (?:\s*=\s*(?P<value>.*))?
    $""",
    re.X,
)

_REF = re.compile(r"^ref\s+(table|expression|role|culture)\s+('[^']*'|\S+)$")

_MULTILINE_FENCE = "```"


@dataclass
class Block:
    """One TMDL statement, plus whatever was indented under it."""

    kind: str
    name: str | None = None
    value: str | None = None
    line: int = 0
    doc: str = ""
    props: dict[str, str | None] = field(default_factory=dict)
    children: list["Block"] = field(default_factory=list)

    def of_kind(self, kind: str) -> list["Block"]:
        return [c for c in self.children if c.kind == kind]

    def has_flag(self, name: str) -> bool:
        """True for a bare valueless keyword nested under this block — `isHidden`, `isKey`,
        `discourageImplicitMeasures`. TMDL spells a boolean-true property as the word alone, so
        these arrive as childless declarations rather than in `props`; `isActive: false` is spelled
        with a value and so *is* a property. That asymmetry is TMDL's, not this parser's, and it is
        the reason both accessors exist."""
        return any(
            c.kind == name and c.value is None and not c.children and not c.props
            for c in self.children
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Block({self.kind} {self.name!r} line={self.line})"


def _indent(raw: str) -> int:
    n = 0
    for ch in raw:
        if ch == "\t":
            n += 1
        else:
            break
    return n


def parse(path: Path) -> list[Block]:
    """Parse one .tmdl file into its top-level blocks."""
    return _parse(path.read_text().splitlines(), str(path))


def _parse(lines: list[str], where: str) -> list[Block]:
    roots: list[Block] = []
    # (indent, owning block or None, that block's children list). The sentinel is indent -1 so the
    # first real line at indent 0 finds `roots` as its sibling list and no owner. The owner is
    # carried because a property belongs to the block it is nested *under*, which is not the same
    # as the previous sibling — `lineageTag` directly under `table` has no previous sibling at all.
    stack: list[tuple[int, Block | None, list[Block]]] = [(-1, None, roots)]
    pending_doc: list[str] = []
    i = 0

    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        lineno = i + 1
        i += 1

        if not stripped:
            # A blank line ends a doc comment. Without this, a `///` paragraph separated from the
            # thing it describes by a blank line would be attributed to it anyway.
            pending_doc = []
            continue

        if stripped.startswith("///"):
            pending_doc.append(stripped[3:].strip())
            continue

        indent = _indent(raw)

        # A declaration whose value is a fenced multi-line expression: `measure 'X' = ```` then the
        # expression, then a closing fence. Consume through the fence before doing anything else,
        # or the expression's own indentation will be read as structure.
        multiline: str | None = None
        if stripped.endswith(_MULTILINE_FENCE) and "=" in stripped:
            body: list[str] = []
            while i < len(lines):
                if lines[i].strip() == _MULTILINE_FENCE:
                    i += 1
                    break
                body.append(lines[i])
                i += 1
            else:
                raise ValueError(f"{where}:{lineno}: unterminated ``` expression")
            # `dedent`, not `strip()` per line. The fenced body is indented to sit under its
            # declaration, and inside that base indent the DAX carries its own structure —
            # `VAR`/`RETURN` at one level, the `IF` arguments at another. Stripping each line
            # discards exactly that, and `tools/extract_dax.py` has to reproduce the expression
            # verbatim, so the parser must not be the thing that flattens it. Removing the common
            # prefix drops the base indent and keeps everything relative to it. A line that is all
            # whitespace is ignored when computing the prefix, which is what makes a blank line
            # inside an expression harmless.
            multiline = textwrap.dedent("\n".join(body)).strip("\n")
            stripped = stripped[: stripped.rindex("=") + 1].strip()

        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()
        _, owner, siblings = stack[-1]

        prop = _PROP.match(stripped) if multiline is None else None
        if prop:
            if owner is None:
                raise ValueError(f"{where}:{lineno}: property {stripped!r} with nothing to own it")
            key, val = prop.group(1), prop.group(2).strip()
            if key in owner.props:
                raise ValueError(f"{where}:{lineno}: {owner.kind} {owner.name!r} repeats {key!r}")
            owner.props[key] = val or None
            pending_doc = []
            continue

        # `ref table dim_date` / `ref expression DatabaseQuery` — three tokens, so it does not fit
        # the general kind/name shape. Recorded as kind='ref' with the referenced object's kind in
        # `value`, which is what makes `model.tmdl`'s table list queryable.
        if ref := _REF.match(stripped):
            block = Block(kind="ref", name=ref.group(2), value=ref.group(1), line=lineno,
                          doc="\n".join(pending_doc))
            pending_doc = []
            siblings.append(block)
            stack.append((indent, block, block.children))
            continue

        decl = _DECL.match(stripped.rstrip("=").strip())
        if not decl:
            raise ValueError(f"{where}:{lineno}: unrecognised TMDL line {stripped!r}")

        name = decl.group("name")
        if name and name.startswith("'"):
            name = name[1:-1]
        block = Block(
            kind=decl.group("kind"),
            name=name,
            value=multiline if multiline is not None else (decl.group("value") or None),
            line=lineno,
            doc="\n".join(pending_doc),
        )
        pending_doc = []
        siblings.append(block)
        stack.append((indent, block, block.children))

    return roots


# -------------------------------------------------------------------------------------
# Model-level accessors
# -------------------------------------------------------------------------------------
@dataclass
class Model:
    tables: dict[str, Block]
    relationships: list[Block]
    model: Block
    expressions: list[Block]

    @property
    def measures(self) -> dict[str, tuple[str, Block]]:
        """Every measure in the model, keyed by name. Measure names are model-wide in DAX, not
        table-scoped, so a duplicate across two tables is a real collision — hence one flat dict,
        built so that a collision is detectable rather than silently overwritten."""
        out: dict[str, tuple[str, Block]] = {}
        for tname, table in self.tables.items():
            for m in table.of_kind("measure"):
                assert m.name is not None
                if m.name in out:
                    raise ValueError(
                        f"measure {m.name!r} defined on both {out[m.name][0]} and {tname}"
                    )
                out[m.name] = (tname, m)
        return out

    def columns(self, table: str) -> dict[str, Block]:
        return {c.name: c for c in self.tables[table].of_kind("column") if c.name}


def load(model_dir: Path = MODEL_DIR) -> Model:
    tables: dict[str, Block] = {}
    for path in sorted((model_dir / "tables").glob("*.tmdl")):
        blocks = [b for b in parse(path) if b.kind == "table"]
        if len(blocks) != 1:
            raise ValueError(f"{path}: expected exactly one `table` block, found {len(blocks)}")
        table = blocks[0]
        assert table.name is not None
        if table.name != path.stem:
            raise ValueError(f"{path}: declares table {table.name!r}; file name says {path.stem!r}")
        tables[table.name] = table

    rels = [b for b in parse(model_dir / "relationships.tmdl") if b.kind == "relationship"]
    model_blocks = [b for b in parse(model_dir / "model.tmdl") if b.kind == "model"]
    if len(model_blocks) != 1:
        raise ValueError("model.tmdl: expected exactly one `model` block")
    exprs = [b for b in parse(model_dir / "expressions.tmdl") if b.kind == "expression"]
    return Model(tables=tables, relationships=rels, model=model_blocks[0], expressions=exprs)


# `Table[column]` and `'Table name'[column]`.
_COL_REF = re.compile(r"(?:'([^']+)'|\b([A-Za-z_][A-Za-z0-9_]*))\[([^\]]+)\]")
# `[Measure name]` not preceded by a table reference.
_MEASURE_REF = re.compile(r"(?<![\]\w'])\[([^\]]+)\]")


def column_refs(expr: str) -> set[tuple[str, str]]:
    return {((m.group(1) or m.group(2)), m.group(3)) for m in _COL_REF.finditer(expr)}


def measure_refs(expr: str) -> set[str]:
    return {m.group(1) for m in _MEASURE_REF.finditer(expr)}
